"""
Zenoh subscriber for signed sensor readings.

Subscribes to key expressions, verifies Ed25519 signatures against
a trust store, and delivers verified readings via callback.
"""

import json
import logging
import threading
from typing import Any, Callable, Optional

from wesense_ingester.zenoh.config import ZenohConfig

logger = logging.getLogger(__name__)

try:
    import zenoh
    _ZENOH_AVAILABLE = True
except ImportError:
    _ZENOH_AVAILABLE = False

try:
    from wesense_ingester.signing.signer import ReadingSigner
    _SIGNING_AVAILABLE = True
except ImportError:
    _SIGNING_AVAILABLE = False


class ZenohSubscriber:
    """Subscribes to Zenoh key expressions, verifies signatures, delivers readings."""

    def __init__(
        self,
        config: Optional[ZenohConfig] = None,
        trust_store: Optional[Any] = None,
        on_reading: Optional[Callable] = None,
    ):
        """
        Args:
            config: Zenoh session config. Defaults to ZenohConfig.from_env().
            trust_store: Optional TrustStore for signature verification.
            on_reading: Callback: (reading_dict: dict, signed_reading: Optional[SignedReading]).
        """
        self.config = config or ZenohConfig.from_env()
        self._trust_store = trust_store
        self._on_reading = on_reading
        self._session: Any = None
        self._subscribers: list[Any] = []
        self._connected = False

        self._stats_lock = threading.Lock()
        self._received = 0
        self._verified = 0
        self._rejected = 0
        self._unsigned = 0

    def connect(self) -> None:
        """Open Zenoh session (blocking — subscriber is the primary work loop)."""
        if not self.config.enabled:
            logger.info("Zenoh subscriber disabled")
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
            logger.info("Zenoh subscriber connected (mode=%s)", self.config.mode)
        except Exception as e:
            logger.error("Zenoh subscriber connection failed: %s", e)

    def subscribe(self, key_expr: str) -> None:
        """Subscribe to a key expression (e.g., 'wesense/v2/live/nz/**')."""
        if not self._connected or not self._session:
            logger.warning("Cannot subscribe — not connected")
            return

        sub = self._session.declare_subscriber(key_expr, self._on_sample)
        self._subscribers.append(sub)
        logger.info("Subscribed to %s", key_expr)

    def _on_sample(self, sample: Any) -> None:
        """Zenoh callback: deserialize, verify, deliver. Must not block."""
        with self._stats_lock:
            self._received += 1

        data = bytes(sample.payload)

        # Try SignedReading protobuf first
        try:
            if not _SIGNING_AVAILABLE:
                raise ImportError("signing not available")
            signed_reading = ReadingSigner.deserialize(data)

            if signed_reading.signature and signed_reading.ingester_id:
                # Valid protobuf envelope — verify signature
                if self._trust_store:
                    if not self._trust_store.is_trusted(signed_reading.ingester_id):
                        with self._stats_lock:
                            self._rejected += 1
                        logger.debug(
                            "Rejected untrusted ingester: %s",
                            signed_reading.ingester_id,
                        )
                        return

                    public_key = self._trust_store.get_public_key(
                        signed_reading.ingester_id, signed_reading.key_version,
                    )
                    if public_key is None:
                        with self._stats_lock:
                            self._rejected += 1
                        logger.debug(
                            "Rejected unknown key version %d for %s",
                            signed_reading.key_version, signed_reading.ingester_id,
                        )
                        return

                    if not ReadingSigner.verify(signed_reading, public_key):
                        with self._stats_lock:
                            self._rejected += 1
                        logger.debug("Rejected invalid signature from %s", signed_reading.ingester_id)
                        return

                    with self._stats_lock:
                        self._verified += 1
                else:
                    with self._stats_lock:
                        self._verified += 1

                reading_dict = json.loads(signed_reading.payload)

                if self._on_reading:
                    self._on_reading(reading_dict, signed_reading)
                return
        except Exception:
            pass

        # Fallback: try raw JSON (unsigned message)
        try:
            reading_dict = json.loads(data)
            with self._stats_lock:
                self._unsigned += 1

            if self._on_reading:
                self._on_reading(reading_dict, None)
        except Exception as e:
            logger.debug("Failed to parse sample: %s", e)

    @property
    def stats(self) -> dict[str, int]:
        """Thread-safe stats counters."""
        with self._stats_lock:
            return {
                "received": self._received,
                "verified": self._verified,
                "rejected": self._rejected,
                "unsigned": self._unsigned,
            }

    def is_connected(self) -> bool:
        """Return whether the subscriber has an active Zenoh session."""
        return self._connected

    def close(self) -> None:
        """Undeclare all subscribers and close the session."""
        for sub in self._subscribers:
            try:
                sub.undeclare()
            except Exception:
                pass
        self._subscribers.clear()

        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        self._connected = False
