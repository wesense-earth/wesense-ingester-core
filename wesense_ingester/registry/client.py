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


class OrbitDBError(Exception):
    """Raised when OrbitDB is unreachable or returns an error."""


def _http_request(url: str, method: str = "GET", data: dict | None = None) -> dict:
    """Send an HTTP request and return parsed JSON. Raises OrbitDBError on failure."""
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
        raise OrbitDBError(f"HTTP {e.code} from {method} {url}: {e.reason}") from e
    except (urllib.error.URLError, OSError) as e:
        raise OrbitDBError(f"OrbitDB unreachable ({method} {url}): {e}") from e
    except json.JSONDecodeError as e:
        raise OrbitDBError(f"Invalid JSON from {method} {url}: {e}") from e


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
        Raises OrbitDBError if OrbitDB is unreachable.
        """
        pub_b64 = base64.b64encode(public_key_bytes).decode()
        base = self._config.url.rstrip("/")

        # PUT /nodes/{ingester_id}
        node_data = {
            "public_key": pub_b64,
            "key_version": key_version,
            **metadata,
        }
        _http_request(f"{base}/nodes/{ingester_id}", method="PUT", data=node_data)
        logger.info("Registered node %s in OrbitDB", ingester_id)

        # PUT /trust/{ingester_id}
        trust_data = {
            "public_key": pub_b64,
            "key_version": key_version,
            "status": "active",
        }
        _http_request(f"{base}/trust/{ingester_id}", method="PUT", data=trust_data)
        logger.info("Published trust entry for %s in OrbitDB", ingester_id)

    def start_trust_sync(self) -> None:
        """Start background daemon thread that periodically fetches GET /trust
        and updates the local TrustStore."""
        if self._sync_thread is not None:
            return

        def _sync_loop():
            while not self._stop_event.is_set():
                try:
                    self.sync_trust_once()
                except OrbitDBError as e:
                    logger.warning("Trust sync failed (will retry): %s", e)
                except Exception:
                    logger.exception("Trust sync unexpected error (will retry)")
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
        """Single trust sync: GET /trust -> trust_store.update_from_dict().
        Raises OrbitDBError if OrbitDB is unreachable."""
        base = self._config.url.rstrip("/")
        data = _http_request(f"{base}/trust")
        if "keys" not in data:
            raise OrbitDBError("OrbitDB /trust response missing 'keys' field")
        self._trust_store.update_from_dict(data)
        logger.debug("Trust sync complete — %d ingesters", len(data["keys"]))

    def discover_zenoh_peers(self, exclude_ids: set[str] | None = None) -> list[str]:
        """Fetch GET /nodes and return one zenoh_endpoint per remote node.

        Prefers LAN endpoints (zenoh_endpoint_lan) over WAN endpoints
        (zenoh_endpoint) to avoid NAT hairpin issues when peers are on
        the same local network.

        Args:
            exclude_ids: Ingester IDs to skip (e.g., local ingesters).

        Returns:
            Deduplicated list of zenoh endpoint strings
            (e.g., ["tcp/192.168.1.50:7447"]).
        """
        exclude = exclude_ids or set()
        base = self._config.url.rstrip("/")
        data = _http_request(f"{base}/nodes")
        endpoints = []
        seen = set()
        for node in data.get("nodes", []):
            nid = node.get("_id") or node.get("ingester_id", "")
            if nid in exclude:
                continue
            # Prefer LAN endpoint, fall back to WAN
            ep = node.get("zenoh_endpoint_lan") or node.get("zenoh_endpoint", "")
            if ep and ep not in seen:
                endpoints.append(ep)
                seen.add(ep)
        return endpoints

    def close(self) -> None:
        """Signal sync thread to stop."""
        self._stop_event.set()
        if self._sync_thread is not None:
            self._sync_thread.join(timeout=5)
            self._sync_thread = None
