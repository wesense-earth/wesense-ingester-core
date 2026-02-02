"""
Generic JSON key-value disk cache.

Used by adapters for their specific needs:
- Meshtastic: position cache, pending telemetry cache
- WeSense LoRa: metadata/location cache

Provides atomic writes (temp file + os.replace), TTL expiration,
configurable save frequency, and thread safety.
"""

import json
import logging
import os
import tempfile
import threading
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


class JSONDiskCache:
    """
    Thread-safe JSON key-value store on disk.

    Data is kept in memory and periodically persisted to disk. Writes are
    atomic (write to temp file, then os.replace) to avoid corruption.
    Entries are expired on load based on a configurable TTL.
    """

    def __init__(
        self,
        cache_file: str,
        ttl: float = 7 * 24 * 3600,
        save_interval: int = 10,
    ):
        """
        Args:
            cache_file: Path to the JSON file on disk.
            ttl: Time-to-live in seconds for entries (default 7 days).
                 Set to 0 to disable expiration.
            save_interval: Save to disk every N updates (default every 10th).
        """
        self.cache_file = cache_file
        self.ttl = ttl
        self.save_interval = save_interval
        self._data: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._update_count = 0

        self._load()

    def _load(self) -> None:
        """Load cache from disk, expiring old entries."""
        if not os.path.exists(self.cache_file):
            return

        try:
            with open(self.cache_file, "r") as f:
                raw = json.load(f)
        except Exception as e:
            logger.warning("Failed to load cache from %s: %s", self.cache_file, e)
            return

        saved_at = raw.get("saved_at", 0)
        data = raw.get("data", {})
        now = time.time()

        if self.ttl > 0:
            expired = 0
            filtered: dict[str, Any] = {}
            for key, entry in data.items():
                entry_time = entry.get("_cached_at", saved_at) if isinstance(entry, dict) else saved_at
                if (now - entry_time) <= self.ttl:
                    filtered[key] = entry
                else:
                    expired += 1
            self._data = filtered
            if expired > 0:
                logger.info(
                    "Loaded %d entries from %s (expired %d)",
                    len(self._data), self.cache_file, expired,
                )
            else:
                logger.info("Loaded %d entries from %s", len(self._data), self.cache_file)
        else:
            self._data = data
            logger.info("Loaded %d entries from %s", len(self._data), self.cache_file)

    def _save(self) -> None:
        """Atomically save cache to disk."""
        try:
            cache_dir = os.path.dirname(self.cache_file)
            if cache_dir:
                os.makedirs(cache_dir, exist_ok=True)

            payload = {
                "data": self._data,
                "saved_at": int(time.time()),
            }

            fd, tmp_path = tempfile.mkstemp(
                dir=cache_dir or ".",
                suffix=".tmp",
            )
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(payload, f)
                os.replace(tmp_path, self.cache_file)
            except BaseException:
                # Clean up temp file on failure
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as e:
            logger.warning("Failed to save cache to %s: %s", self.cache_file, e)

    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache, or None if not present / expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None

            # Check per-entry TTL
            if self.ttl > 0 and isinstance(entry, dict):
                cached_at = entry.get("_cached_at", 0)
                if cached_at and (time.time() - cached_at) > self.ttl:
                    del self._data[key]
                    return None

            return entry

    def set(self, key: str, value: Any) -> None:
        """
        Set a value in the cache. Automatically adds _cached_at timestamp
        if value is a dict. Periodically saves to disk based on save_interval.
        """
        with self._lock:
            if isinstance(value, dict):
                value = {**value, "_cached_at": time.time()}
            self._data[key] = value
            self._update_count += 1
            if self._update_count >= self.save_interval:
                self._save()
                self._update_count = 0

    def delete(self, key: str) -> bool:
        """Delete a key from the cache. Returns True if key existed."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def keys(self) -> list[str]:
        """Return all keys in the cache."""
        with self._lock:
            return list(self._data.keys())

    def items(self) -> list[tuple[str, Any]]:
        """Return all (key, value) pairs in the cache."""
        with self._lock:
            return list(self._data.items())

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def flush(self) -> None:
        """Force save to disk immediately."""
        with self._lock:
            self._save()
            self._update_count = 0
