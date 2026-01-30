"""Tests for deduplication cache."""

import time
from unittest.mock import patch

from wesense_ingester.cache.dedup import DeduplicationCache


def test_first_reading_is_not_duplicate():
    cache = DeduplicationCache()
    assert cache.is_duplicate("node-1", "temperature", 1000) is False


def test_same_reading_is_duplicate():
    cache = DeduplicationCache()
    cache.is_duplicate("node-1", "temperature", 1000)
    assert cache.is_duplicate("node-1", "temperature", 1000) is True


def test_different_device_not_duplicate():
    cache = DeduplicationCache()
    cache.is_duplicate("node-1", "temperature", 1000)
    assert cache.is_duplicate("node-2", "temperature", 1000) is False


def test_different_type_not_duplicate():
    cache = DeduplicationCache()
    cache.is_duplicate("node-1", "temperature", 1000)
    assert cache.is_duplicate("node-1", "humidity", 1000) is False


def test_different_timestamp_not_duplicate():
    cache = DeduplicationCache()
    cache.is_duplicate("node-1", "temperature", 1000)
    assert cache.is_duplicate("node-1", "temperature", 1001) is False


def test_stats():
    cache = DeduplicationCache()
    cache.is_duplicate("node-1", "temperature", 1000)
    cache.is_duplicate("node-1", "temperature", 1000)
    cache.is_duplicate("node-2", "temperature", 1000)

    stats = cache.get_stats()
    assert stats["unique_processed"] == 2
    assert stats["duplicates_blocked"] == 1
    assert stats["cache_size"] == 2


def test_ttl_expiration():
    """Entries older than TTL are cleaned up."""
    cache = DeduplicationCache(ttl=0.1, cleanup_interval=0.0)

    cache.is_duplicate("node-1", "temperature", 1000)
    assert cache.is_duplicate("node-1", "temperature", 1000) is True

    # Wait for TTL to expire
    time.sleep(0.15)

    # After TTL, the entry should be cleaned and the reading should be new again
    assert cache.is_duplicate("node-1", "temperature", 1000) is False
