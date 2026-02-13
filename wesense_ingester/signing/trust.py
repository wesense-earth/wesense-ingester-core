"""
Trust store for ingester public keys.

Maintains a JSON file mapping ingester IDs to their public keys,
with key versioning and revocation support. Thread-safe for
concurrent access from multiple ingester threads.
"""

import base64
import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

logger = logging.getLogger(__name__)


class TrustStore:
    """
    Thread-safe trust store for ingester public keys.

    Keys are stored on disk as JSON, keyed by ingester_id and key_version.
    Supports adding, revoking, querying, bulk update, and snapshot export.
    """

    def __init__(self, trust_file: str = "data/trust_list.json"):
        self._trust_file = trust_file
        self._lock = threading.Lock()
        self._keys: dict[str, dict[str, dict[str, Any]]] = {}
        self.load()

    def load(self) -> None:
        """Load trust list from disk. Missing file = empty trust store."""
        with self._lock:
            if not os.path.exists(self._trust_file):
                self._keys = {}
                return

            try:
                with open(self._trust_file, "r") as f:
                    data = json.load(f)
                self._keys = data.get("keys", {})
                logger.info(
                    "Loaded trust store with %d ingesters from %s",
                    len(self._keys), self._trust_file,
                )
            except Exception as e:
                logger.warning("Failed to load trust store from %s: %s", self._trust_file, e)
                self._keys = {}

    def save(self) -> None:
        """Atomically save trust list to disk."""
        with self._lock:
            self._save_unlocked()

    def _save_unlocked(self) -> None:
        """Save without acquiring lock (caller must hold lock)."""
        try:
            dir_path = os.path.dirname(self._trust_file)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)

            payload = {"keys": self._keys}

            fd, tmp_path = tempfile.mkstemp(
                dir=dir_path or ".",
                suffix=".tmp",
            )
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(payload, f, indent=2)
                os.replace(tmp_path, self._trust_file)
            except BaseException:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as e:
            logger.warning("Failed to save trust store to %s: %s", self._trust_file, e)

    def is_trusted(self, ingester_id: str) -> bool:
        """Check if an ingester has any active (non-revoked) keys."""
        with self._lock:
            versions = self._keys.get(ingester_id, {})
            return any(
                entry.get("status") == "active"
                for entry in versions.values()
            )

    def get_public_key(
        self, ingester_id: str, key_version: int
    ) -> Optional[Ed25519PublicKey]:
        """Get the public key for a specific ingester ID and version."""
        with self._lock:
            versions = self._keys.get(ingester_id, {})
            entry = versions.get(str(key_version))
            if entry is None or entry.get("status") != "active":
                return None

            key_bytes = base64.b64decode(entry["public_key"])
            return Ed25519PublicKey.from_public_bytes(key_bytes)

    def add_trusted(
        self,
        ingester_id: str,
        public_key_bytes: bytes,
        key_version: int,
        **metadata: Any,
    ) -> None:
        """Add a trusted public key for an ingester."""
        with self._lock:
            if ingester_id not in self._keys:
                self._keys[ingester_id] = {}

            self._keys[ingester_id][str(key_version)] = {
                "public_key": base64.b64encode(public_key_bytes).decode(),
                "status": "active",
                "added": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata,
            }
            self._save_unlocked()

        logger.info(
            "Added trusted key for %s version %d", ingester_id, key_version,
        )

    def revoke(self, ingester_id: str, key_version: int, reason: str = "") -> None:
        """Revoke a specific key version for an ingester."""
        with self._lock:
            versions = self._keys.get(ingester_id, {})
            entry = versions.get(str(key_version))
            if entry is None:
                return

            entry["status"] = "revoked"
            entry["revoked_at"] = datetime.now(timezone.utc).isoformat()
            entry["revoke_reason"] = reason
            self._save_unlocked()

        logger.info(
            "Revoked key for %s version %d: %s", ingester_id, key_version, reason,
        )

    def update_from_dict(self, trust_data: dict[str, Any]) -> None:
        """
        Bulk update from a dictionary (e.g. synced from OrbitDB).

        Expected format: {"keys": {"wsi_xxx": {"1": {...}, ...}, ...}}
        """
        incoming_keys = trust_data.get("keys", {})
        with self._lock:
            for ingester_id, versions in incoming_keys.items():
                if ingester_id not in self._keys:
                    self._keys[ingester_id] = {}
                for ver, entry in versions.items():
                    self._keys[ingester_id][ver] = entry
            self._save_unlocked()

        logger.info("Bulk-updated trust store with %d ingesters", len(incoming_keys))

    def export_snapshot(self, ingester_ids: list[str]) -> dict[str, Any]:
        """Export a subset of the trust store for the given ingester IDs."""
        with self._lock:
            subset = {}
            for iid in ingester_ids:
                if iid in self._keys:
                    subset[iid] = self._keys[iid]
            return {"keys": subset}
