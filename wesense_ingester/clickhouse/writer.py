"""
Buffered ClickHouse writer.

Thread-safe batch writer with configurable flush interval, batch size,
and flexible column list (set by adapter at init, not hardcoded).
Rows are returned to the buffer on failure for retry.
"""

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)

try:
    import clickhouse_connect
    _CH_AVAILABLE = True
except ImportError:
    _CH_AVAILABLE = False


@dataclass
class ClickHouseConfig:
    """ClickHouse connection configuration."""

    host: str = "localhost"
    port: int = 8123
    user: str = "default"
    password: str = ""
    database: str = "wesense"
    table: str = "sensor_readings"

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """Create config from environment variables with sensible defaults."""
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "wesense"),
            table=os.getenv("CLICKHOUSE_TABLE", "sensor_readings"),
        )


class BufferedClickHouseWriter:
    """
    Thread-safe buffered writer for ClickHouse.

    Rows are accumulated in an in-memory buffer and flushed either when
    the batch size is reached or on a periodic timer. On flush failure,
    rows are returned to the buffer for retry.

    The column list is set at init time — each adapter provides its own
    columns matching its ClickHouse table schema.
    """

    def __init__(
        self,
        config: Optional[ClickHouseConfig] = None,
        columns: Optional[Sequence[str]] = None,
        batch_size: int = 100,
        flush_interval: float = 10.0,
    ):
        """
        Args:
            config: ClickHouse connection config. Defaults to ClickHouseConfig.from_env().
            columns: Column names for inserts. Must match the order of row tuples.
            batch_size: Flush when buffer reaches this size.
            flush_interval: Seconds between periodic flushes.
        """
        if not _CH_AVAILABLE:
            raise ImportError(
                "clickhouse-connect not available. "
                "Install with: pip install clickhouse-connect>=0.8.0"
            )

        self.config = config or ClickHouseConfig.from_env()
        self.columns = list(columns) if columns else []
        self.batch_size = int(os.getenv("CLICKHOUSE_BATCH_SIZE", str(batch_size)))
        self.flush_interval = float(os.getenv("CLICKHOUSE_FLUSH_INTERVAL", str(flush_interval)))

        self._buffer: list[tuple] = []
        self._lock = threading.Lock()
        self._client: Any = None
        self._flush_timer: Optional[threading.Timer] = None
        self._total_written = 0
        self._total_failed = 0

        self._connect()
        self._schedule_flush()

    def _connect(self) -> None:
        """Connect to ClickHouse."""
        try:
            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.user,
                password=self.config.password,
                database=self.config.database,
            )
            logger.info(
                "Connected to ClickHouse at %s:%d/%s",
                self.config.host, self.config.port, self.config.database,
            )
        except Exception as e:
            logger.error("Failed to connect to ClickHouse: %s", e)
            self._client = None

    def _schedule_flush(self) -> None:
        """Schedule the next periodic flush."""
        if self._flush_timer is not None:
            self._flush_timer.cancel()
        self._flush_timer = threading.Timer(self.flush_interval, self._periodic_flush)
        self._flush_timer.daemon = True
        self._flush_timer.start()

    def _periodic_flush(self) -> None:
        """Timer callback: flush then reschedule."""
        self.flush()
        self._schedule_flush()

    def add(self, row: tuple) -> None:
        """Add a row to the buffer. Flushes if batch_size is reached."""
        with self._lock:
            self._buffer.append(row)
            size = len(self._buffer)

        if size >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """Flush the buffer to ClickHouse."""
        with self._lock:
            if not self._buffer or not self._client:
                return
            rows = self._buffer
            self._buffer = []

        table = f"{self.config.database}.{self.config.table}"
        try:
            self._client.insert(table, rows, column_names=self.columns)
            self._total_written += len(rows)
            logger.info("Flushed %d rows to ClickHouse", len(rows))
        except Exception as e:
            logger.error("ClickHouse flush failed (%d rows): %s", len(rows), e)
            self._total_failed += len(rows)
            # Return rows to buffer for retry
            with self._lock:
                self._buffer = rows + self._buffer

    def get_stats(self) -> dict[str, int]:
        """Return write statistics."""
        with self._lock:
            return {
                "buffer_size": len(self._buffer),
                "total_written": self._total_written,
                "total_failed": self._total_failed,
            }

    def close(self) -> None:
        """Flush remaining rows and stop the periodic timer."""
        if self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None
        self.flush()
