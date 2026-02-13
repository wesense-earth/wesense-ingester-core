"""
Zenoh queryable for serving ClickHouse data via distributed queries.

Registers as a Zenoh Queryable and responds to incoming queries by
reading from a local ClickHouse instance. Query handlers run on
background threads to avoid blocking Zenoh callbacks.
"""

import json
import logging
import threading
from typing import Any, Optional

from wesense_ingester.zenoh.config import ZenohConfig

logger = logging.getLogger(__name__)

try:
    import zenoh
    _ZENOH_AVAILABLE = True
except ImportError:
    _ZENOH_AVAILABLE = False

try:
    import clickhouse_connect
    _CH_AVAILABLE = True
except ImportError:
    _CH_AVAILABLE = False

_QUERY_SQL = {
    "summary": (
        "SELECT geo_country, geo_subdivision, reading_type, "
        "avg(value) AS avg_value, count() AS reading_count "
        "FROM {table} "
        "WHERE timestamp > now() - INTERVAL 1 HOUR "
        "GROUP BY geo_country, geo_subdivision, reading_type"
    ),
    "latest": (
        "SELECT * FROM {table} "
        "WHERE timestamp > now() - INTERVAL 1 HOUR "
        "ORDER BY timestamp DESC "
        "LIMIT 1 BY device_id, reading_type"
    ),
    "history": (
        "SELECT * FROM {table} "
        "WHERE timestamp > now() - INTERVAL {{hours}} HOUR "
        "ORDER BY timestamp DESC"
    ),
    "devices": (
        "SELECT device_id, max(timestamp) AS last_seen, count() AS reading_count "
        "FROM {table} "
        "WHERE timestamp > now() - INTERVAL 24 HOUR "
        "GROUP BY device_id"
    ),
}


class ZenohQueryable:
    """Serves distributed queries from ClickHouse data."""

    def __init__(
        self,
        config: Optional[ZenohConfig] = None,
        clickhouse_config: Optional[Any] = None,
    ):
        """
        Args:
            config: Zenoh session config. Defaults to ZenohConfig.from_env().
            clickhouse_config: Optional ClickHouseConfig for query execution.
        """
        self.config = config or ZenohConfig.from_env()
        self._ch_config = clickhouse_config
        self._session: Any = None
        self._ch_client: Any = None
        self._queryables: list[Any] = []
        self._connected = False

    def connect(self) -> None:
        """Open Zenoh session and ClickHouse read-only client."""
        if not self.config.enabled:
            logger.info("Zenoh queryable disabled")
            return

        if not _ZENOH_AVAILABLE:
            logger.warning(
                "eclipse-zenoh not available. "
                "Install with: pip install eclipse-zenoh>=1.0.0"
            )
            return

        try:
            cfg = zenoh.Config.from_json5(self.config.to_zenoh_json())
            self._session = zenoh.open(cfg)
            self._connected = True
            logger.info("Zenoh queryable connected (mode=%s)", self.config.mode)
        except Exception as e:
            logger.error("Zenoh queryable connection failed: %s", e)
            return

        if self._ch_config and _CH_AVAILABLE:
            try:
                self._ch_client = clickhouse_connect.get_client(
                    host=self._ch_config.host,
                    port=self._ch_config.port,
                    username=self._ch_config.user,
                    password=self._ch_config.password,
                    database=self._ch_config.database,
                )
                logger.info("Queryable ClickHouse client connected")
            except Exception as e:
                logger.error("Queryable ClickHouse connection failed: %s", e)

    def register(self, key_expr: str) -> None:
        """Register as Queryable for the given key expression."""
        if not self._connected or not self._session:
            logger.warning("Cannot register queryable — not connected")
            return

        qable = self._session.declare_queryable(key_expr, self._on_query)
        self._queryables.append(qable)
        logger.info("Registered queryable on %s", key_expr)

    def _on_query(self, query: Any) -> None:
        """Zenoh query callback — dispatch to background thread."""
        thread = threading.Thread(target=self._handle_query, args=(query,), daemon=True)
        thread.start()

    def _handle_query(self, query: Any) -> None:
        """Handle query on background thread."""
        try:
            payload_str = bytes(query.payload).decode() if query.payload else ""
        except Exception:
            payload_str = ""

        # Parse query type and parameters
        query_type = payload_str.split("?")[0].strip()
        params = {}
        if "?" in payload_str:
            param_str = payload_str.split("?", 1)[1]
            for part in param_str.split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k.strip()] = v.strip()

        if not self._ch_client:
            self._reply(query, {"results": [], "error": "no clickhouse connection"})
            return

        table = f"{self._ch_config.database}.{self._ch_config.table}"

        try:
            if query_type == "summary":
                result = self._query_summary(table)
            elif query_type == "latest":
                result = self._query_latest(table)
            elif query_type == "history":
                hours = min(int(params.get("hours", "1")), 24)
                result = self._query_history(table, hours)
            elif query_type == "devices":
                result = self._query_devices(table)
            else:
                result = {"error": f"unknown query type: {query_type}"}

            self._reply(query, result)
        except Exception as e:
            logger.error("Query handler error: %s", e)
            self._reply(query, {"error": str(e)})

    def _query_summary(self, table: str) -> dict:
        sql = _QUERY_SQL["summary"].format(table=table)
        result = self._ch_client.query(sql)
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return {"results": rows}

    def _query_latest(self, table: str) -> dict:
        sql = _QUERY_SQL["latest"].format(table=table)
        result = self._ch_client.query(sql)
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return {"results": rows}

    def _query_history(self, table: str, hours: int) -> dict:
        sql = _QUERY_SQL["history"].format(table=table).format(hours=hours)
        result = self._ch_client.query(sql)
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return {"results": rows}

    def _query_devices(self, table: str) -> dict:
        sql = _QUERY_SQL["devices"].format(table=table)
        result = self._ch_client.query(sql)
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return {"results": rows}

    def _reply(self, query: Any, data: dict) -> None:
        """Send JSON reply to a Zenoh query."""
        try:
            payload = json.dumps(data, default=str).encode()
            query.reply(query.key_expr, payload)
        except Exception as e:
            logger.error("Failed to reply to query: %s", e)

    def is_connected(self) -> bool:
        """Return whether the queryable has an active Zenoh session."""
        return self._connected

    def close(self) -> None:
        """Undeclare all queryables and close the session."""
        for qable in self._queryables:
            try:
                qable.undeclare()
            except Exception:
                pass
        self._queryables.clear()

        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        self._connected = False
