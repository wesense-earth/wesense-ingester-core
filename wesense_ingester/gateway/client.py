"""
HTTP client for the WeSense storage gateway.

Buffers reading dicts and POSTs them in batches to the gateway's
POST /readings endpoint. Mirrors BufferedClickHouseWriter's interface
(add/flush/close/get_stats) so ingesters can swap backends transparently.

Uses only stdlib urllib — no requests dependency.
"""

import json
import logging
import threading
import urllib.error
import urllib.request
from typing import Optional

from wesense_ingester.gateway.config import GatewayConfig

logger = logging.getLogger(__name__)


class GatewayClient:
    """
    Buffered HTTP client for the WeSense storage gateway.

    Thread-safe batch writer that POSTs reading dicts to the gateway.
    Mirrors BufferedClickHouseWriter's interface for drop-in replacement.
    """

    def __init__(self, config: Optional[GatewayConfig] = None):
        self.config = config or GatewayConfig.from_env()
        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._flush_timer: Optional[threading.Timer] = None
        self._total_sent = 0
        self._total_failed = 0

        self._schedule_flush()
        logger.info(
            "GatewayClient initialised (url=%s, batch_size=%d, flush_interval=%.1fs)",
            self.config.url, self.config.batch_size, self.config.flush_interval,
        )

    def _schedule_flush(self) -> None:
        """Schedule the next periodic flush."""
        if self._flush_timer is not None:
            self._flush_timer.cancel()
        self._flush_timer = threading.Timer(self.config.flush_interval, self._periodic_flush)
        self._flush_timer.daemon = True
        self._flush_timer.start()

    def _periodic_flush(self) -> None:
        """Timer callback: flush then reschedule."""
        self.flush()
        self._schedule_flush()

    def add(self, reading: dict) -> None:
        """Add a reading dict to the buffer. Flushes if batch_size is reached."""
        with self._lock:
            self._buffer.append(reading)
            size = len(self._buffer)

        if size >= self.config.batch_size:
            self.flush()

    def flush(self) -> None:
        """POST buffered readings to the gateway."""
        with self._lock:
            if not self._buffer:
                return
            batch = self._buffer
            self._buffer = []

        url = self.config.url.rstrip("/") + "/readings"
        payload = json.dumps({"readings": batch}).encode()

        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                body = json.loads(resp.read().decode())
                accepted = body.get("accepted", 0)
                duplicates = body.get("duplicates", 0)
                errors = body.get("errors", 0)
                self._total_sent += accepted + duplicates
                self._total_failed += errors
                logger.info(
                    "Gateway flush: %d readings (accepted=%d, duplicates=%d, errors=%d)",
                    len(batch), accepted, duplicates, errors,
                )
        except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
            logger.error("Gateway flush failed (%d readings): %s", len(batch), e)
            self._total_failed += len(batch)
            # Return readings to buffer for retry, respecting max_buffer_size
            with self._lock:
                self._buffer = batch + self._buffer
                overflow = len(self._buffer) - self.config.max_buffer_size
                if overflow > 0:
                    self._buffer = self._buffer[overflow:]
                    logger.warning(
                        "Buffer overflow — dropped %d oldest readings (max=%d)",
                        overflow, self.config.max_buffer_size,
                    )

    def get_stats(self) -> dict[str, int]:
        """Return client statistics."""
        with self._lock:
            return {
                "buffer_size": len(self._buffer),
                "total_sent": self._total_sent,
                "total_failed": self._total_failed,
                # Alias for compatibility with BufferedClickHouseWriter stats consumers
                "total_written": self._total_sent,
            }

    def close(self) -> None:
        """Cancel timer and flush remaining readings."""
        if self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None
        self.flush()
        logger.info(
            "GatewayClient closed (total_sent=%d, total_failed=%d)",
            self._total_sent, self._total_failed,
        )
