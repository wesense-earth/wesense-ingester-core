"""Tests for JSON disk cache."""

import json
import os
import tempfile
import time

from wesense_ingester.cache.disk_cache import JSONDiskCache


def test_set_and_get():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path, save_interval=1)

        cache.set("key1", {"value": 42})
        result = cache.get("key1")
        assert result["value"] == 42


def test_get_missing_key_returns_none():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path)

        assert cache.get("nonexistent") is None


def test_delete():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path, save_interval=1)

        cache.set("key1", {"value": 42})
        assert cache.delete("key1") is True
        assert cache.get("key1") is None
        assert cache.delete("key1") is False


def test_persistence_to_disk():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")

        # Write data
        cache1 = JSONDiskCache(path, save_interval=1)
        cache1.set("key1", {"value": 42})

        # Load from disk in new instance
        cache2 = JSONDiskCache(path, save_interval=1)
        result = cache2.get("key1")
        assert result is not None
        assert result["value"] == 42


def test_atomic_write():
    """Cache file should exist after flush and be valid JSON."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path, save_interval=1)

        cache.set("key1", {"value": 42})
        cache.flush()

        assert os.path.exists(path)
        with open(path) as f:
            data = json.load(f)
        assert "data" in data
        assert "saved_at" in data


def test_ttl_expiration():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path, ttl=0.1, save_interval=1)

        cache.set("key1", {"value": 42})
        assert cache.get("key1") is not None

        time.sleep(0.15)
        assert cache.get("key1") is None


def test_len():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path)

        assert len(cache) == 0
        cache.set("a", {"v": 1})
        cache.set("b", {"v": 2})
        assert len(cache) == 2


def test_keys_and_items():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path)

        cache.set("a", {"v": 1})
        cache.set("b", {"v": 2})
        assert sorted(cache.keys()) == ["a", "b"]
        assert len(cache.items()) == 2


def test_save_interval():
    """Cache should only save to disk every N updates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path, save_interval=3)

        cache.set("a", {"v": 1})
        assert not os.path.exists(path)

        cache.set("b", {"v": 2})
        assert not os.path.exists(path)

        cache.set("c", {"v": 3})
        assert os.path.exists(path)


def test_non_dict_values():
    """Non-dict values should work but won't get _cached_at."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cache.json")
        cache = JSONDiskCache(path, save_interval=1)

        cache.set("key1", "simple_string")
        assert cache.get("key1") == "simple_string"
