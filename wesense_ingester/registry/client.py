"""
HTTP client for the wesense-orbitdb service.

Handles node registration and periodic trust list synchronisation.
Uses only stdlib urllib — no requests dependency.
"""

import base64
import json
import logging
import threading
import urllib.error
import urllib.request
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wesense_ingester.registry.config import RegistryConfig
    from wesense_ingester.signing.trust import TrustStore

logger = logging.getLogger(__name__)

# HTTP timeouts (seconds)
_CONNECT_TIMEOUT = 5
_READ_TIMEOUT = 10


def _http_request(url: str, method: str = "GET", data: dict | None = None) -> dict | None:
    """Send an HTTP request and return parsed JSON, or None on failure."""
    body = None
    headers = {}
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=body, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=_READ_TIMEOUT) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        logger.warning("OrbitDB HTTP %s %s → %d: %s", method, url, e.code, e.reason)
        return None
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        logger.warning("OrbitDB request failed (%s %s): %s", method, url, e)
        return None


class RegistryClient:
    """HTTP client for wesense-orbitdb. Handles node registration and periodic trust sync."""

    def __init__(self, config: "RegistryConfig", trust_store: "TrustStore"):
        self._config = config
        self._trust_store = trust_store
        self._sync_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def register_node(
        self,
        ingester_id: str,
        public_key_bytes: bytes,
        key_version: int,
        **metadata,
    ) -> None:
        """
        Register this ingester's public key in OrbitDB (nodes + trust databases).

        Non-blocking: runs in a short-lived daemon thread so startup isn't delayed.
        If OrbitDB is down, the registration is silently skipped.
        """
        pub_b64 = base64.b64encode(public_key_bytes).decode()

        def _register():
            base = self._config.url.rstrip("/")

            # PUT /nodes/{ingester_id}
            node_data = {
                "public_key": pub_b64,
                "key_version": key_version,
                **metadata,
            }
            result = _http_request(f"{base}/nodes/{ingester_id}", method="PUT", data=node_data)
            if result:
                logger.info("Registered node %s in OrbitDB", ingester_id)
            else:
                logger.warning("Failed to register node %s — OrbitDB may be unavailable", ingester_id)

            # PUT /trust/{ingester_id}
            trust_data = {
                "public_key": pub_b64,
                "key_version": key_version,
                "status": "active",
            }
            result = _http_request(f"{base}/trust/{ingester_id}", method="PUT", data=trust_data)
            if result:
                logger.info("Published trust entry for %s in OrbitDB", ingester_id)

        t = threading.Thread(target=_register, daemon=True, name="orbitdb-register")
        t.start()

    def start_trust_sync(self) -> None:
        """Start background daemon thread that periodically fetches GET /trust
        and updates the local TrustStore."""
        if self._sync_thread is not None:
            return

        def _sync_loop():
            while not self._stop_event.is_set():
                self.sync_trust_once()
                self._stop_event.wait(timeout=self._config.sync_interval)

        self._sync_thread = threading.Thread(
            target=_sync_loop, daemon=True, name="orbitdb-trust-sync"
        )
        self._sync_thread.start()
        logger.info(
            "Trust sync started (interval=%ds, url=%s)",
            self._config.sync_interval,
            self._config.url,
        )

    def sync_trust_once(self) -> None:
        """Single trust sync: GET /trust -> trust_store.update_from_dict()."""
        base = self._config.url.rstrip("/")
        data = _http_request(f"{base}/trust")
        if data and "keys" in data:
            self._trust_store.update_from_dict(data)
            logger.debug("Trust sync complete — %d ingesters", len(data["keys"]))
        else:
            logger.warning("Trust sync failed — OrbitDB may be unavailable")

    def close(self) -> None:
        """Signal sync thread to stop."""
        self._stop_event.set()
        if self._sync_thread is not None:
            self._sync_thread.join(timeout=5)
            self._sync_thread = None
