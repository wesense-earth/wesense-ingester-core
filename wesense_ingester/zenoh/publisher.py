"""
Zenoh publisher for signed sensor readings.

Publishes readings to the Zenoh network with optional Ed25519 signing.
Follows the WeSensePublisher pattern: connect/publish/close lifecycle,
try-except error handling, boolean returns.
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


class ZenohPublisher:
    """Publishes SignedReading messages to the Zenoh network."""

    def __init__(
        self,
        config: Optional[ZenohConfig] = None,
        signer: Optional[Any] = None,
    ):
        """
        Args:
            config: Zenoh session config. Defaults to ZenohConfig.from_env().
            signer: Optional ReadingSigner (typed as Any to avoid hard import).
        """
        self.config = config or ZenohConfig.from_env()
        self._signer = signer
        self._session: Any = None
        self._publishers: dict[str, Any] = {}
        self._pub_lock = threading.Lock()
        self._connected = False

    def connect(self) -> None:
        """Open Zenoh session on a daemon thread. Returns immediately."""
        if not self.config.enabled:
            logger.info("Zenoh publishing disabled")
            return

        if not _ZENOH_AVAILABLE:
            logger.warning(
                "eclipse-zenoh not available. "
                "Install with: pip install eclipse-zenoh>=1.0.0"
            )
            return

        thread = threading.Thread(target=self._connect_worker, daemon=True)
        thread.start()

    def _connect_worker(self) -> None:
        """Background thread: open Zenoh session."""
        try:
            cfg = zenoh.Config.from_json5(self.config.to_zenoh_json())
            self._session = zenoh.open(cfg)
            self._connected = True
            logger.info("Zenoh publisher connected (mode=%s)", self.config.mode)
        except Exception as e:
            logger.error("Zenoh connection failed: %s", e)

    def publish_reading(self, reading: dict[str, Any]) -> bool:
        """
        Serialize reading to JSON, optionally sign, publish to Zenoh.

        Returns True if publish succeeded, False otherwise.
        """
        if not self._connected or not self._session:
            return False

        country = (reading.get("geo_country") or "unknown").lower()
        subdivision = (reading.get("geo_subdivision") or "unknown").lower()
        device_id = reading.get("device_id") or "unknown"

        key_expr = self.config.build_key_expr(country, subdivision, device_id)

        try:
            payload_bytes = json.dumps(reading, sort_keys=True, default=str).encode()

            if self._signer:
                signed = self._signer.sign(payload_bytes)
                data = signed.SerializeToString()
            else:
                data = payload_bytes

            pub = self._get_or_create_publisher(key_expr)
            pub.put(data)
            logger.debug("Published to %s", key_expr)
            return True
        except Exception as e:
            logger.error("Failed to publish to %s: %s", key_expr, e)
            return False

    def _get_or_create_publisher(self, key_expr: str) -> Any:
        """Get cached publisher or declare a new one."""
        with self._pub_lock:
            if key_expr not in self._publishers:
                self._publishers[key_expr] = self._session.declare_publisher(key_expr)
            return self._publishers[key_expr]

    def is_connected(self) -> bool:
        """Return whether the publisher has an active Zenoh session."""
        return self._connected

    def close(self) -> None:
        """Undeclare all cached publishers and close the session."""
        with self._pub_lock:
            for pub in self._publishers.values():
                try:
                    pub.undeclare()
                except Exception:
                    pass
            self._publishers.clear()

        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        self._connected = False
