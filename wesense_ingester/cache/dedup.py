"""
Reading deduplication cache.

Catches mesh network flooding where the same reading arrives multiple times
via different gateways. Thread-safe with background cleanup.
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)


class DeduplicationCache:
    """
    In-memory deduplication cache for sensor readings.

    Key: (device_id, reading_type, timestamp) tuple.
    Entries expire after a configurable TTL. Background cleanup runs
    at a configurable interval to remove expired entries.

    Thread-safe via lock.
    """

    def __init__(
        self,
        ttl: float = 3600.0,
        cleanup_interval: float = 300.0,
    ):
        """
        Args:
            ttl: Time-to-live in seconds for cache entries (default 1 hour).
            cleanup_interval: Seconds between background cleanup sweeps (default 5 min).
        """
        self.ttl = ttl
        self.cleanup_interval = cleanup_interval
        self._cache: dict[tuple, float] = {}
        self._lock = threading.Lock()
        self._last_cleanup = time.monotonic()
        self._duplicates_blocked = 0
        self._unique_processed = 0

    def is_duplicate(
        self,
        device_id: str,
        reading_type: str,
        timestamp: int,
    ) -> bool:
        """
        Check if this reading has already been processed.

        Returns True if duplicate (should skip), False if new (should process).
        Automatically triggers periodic cleanup.
        """
        key = (device_id, reading_type, timestamp)
        now = time.monotonic()

        with self._lock:
            # Periodic cleanup
            if now - self._last_cleanup > self.cleanup_interval:
                self._cleanup(now)
                self._last_cleanup = now

            if key in self._cache:
                self._duplicates_blocked += 1
                return True

            self._cache[key] = now
            self._unique_processed += 1
            return False

    def _cleanup(self, now: float) -> None:
        """Remove entries older than TTL. Must be called with lock held."""
        cutoff = now - self.ttl
        old_size = len(self._cache)
        self._cache = {k: v for k, v in self._cache.items() if v > cutoff}
        removed = old_size - len(self._cache)
        if removed > 0:
            logger.debug(
                "Dedup cleanup: %d -> %d entries (removed %d)",
                old_size, len(self._cache), removed,
            )

    def get_stats(self) -> dict:
        """Return deduplication statistics."""
        with self._lock:
            return {
                "cache_size": len(self._cache),
                "duplicates_blocked": self._duplicates_blocked,
                "unique_processed": self._unique_processed,
            }
