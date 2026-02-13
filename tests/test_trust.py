"""Tests for the trust store."""

import os
import tempfile
import threading

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from wesense_ingester.signing.trust import TrustStore


def _gen_key_bytes():
    """Generate a random Ed25519 public key and return raw bytes."""
    pk = Ed25519PrivateKey.generate().public_key()
    return pk.public_bytes(Encoding.Raw, PublicFormat.Raw)


def test_empty_trust_store():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        store = TrustStore(trust_file=path)

        assert store.is_trusted("wsi_00000000") is False


def test_add_and_query_trusted():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        store = TrustStore(trust_file=path)

        key_bytes = _gen_key_bytes()
        store.add_trusted("wsi_aabbccdd", key_bytes, key_version=1)

        assert store.is_trusted("wsi_aabbccdd") is True


def test_untrusted_id_returns_false():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        store = TrustStore(trust_file=path)

        key_bytes = _gen_key_bytes()
        store.add_trusted("wsi_aabbccdd", key_bytes, key_version=1)

        assert store.is_trusted("wsi_unknown") is False


def test_get_public_key():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        store = TrustStore(trust_file=path)

        key_bytes = _gen_key_bytes()
        store.add_trusted("wsi_aabbccdd", key_bytes, key_version=1)

        retrieved = store.get_public_key("wsi_aabbccdd", key_version=1)
        assert retrieved is not None
        retrieved_bytes = retrieved.public_bytes(Encoding.Raw, PublicFormat.Raw)
        assert retrieved_bytes == key_bytes


def test_revoke_key():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        store = TrustStore(trust_file=path)

        key_bytes = _gen_key_bytes()
        store.add_trusted("wsi_aabbccdd", key_bytes, key_version=1)
        assert store.is_trusted("wsi_aabbccdd") is True

        store.revoke("wsi_aabbccdd", key_version=1, reason="compromised")
        assert store.is_trusted("wsi_aabbccdd") is False
        assert store.get_public_key("wsi_aabbccdd", key_version=1) is None


def test_persistence():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        key_bytes = _gen_key_bytes()

        # Add and save
        store1 = TrustStore(trust_file=path)
        store1.add_trusted("wsi_aabbccdd", key_bytes, key_version=1)

        # Reload from disk
        store2 = TrustStore(trust_file=path)
        assert store2.is_trusted("wsi_aabbccdd") is True

        retrieved = store2.get_public_key("wsi_aabbccdd", key_version=1)
        assert retrieved is not None


def test_missing_file_loads_empty():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "nonexistent", "trust.json")
        store = TrustStore(trust_file=path)

        assert store.is_trusted("wsi_anything") is False


def test_update_from_dict():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        store = TrustStore(trust_file=path)

        import base64
        key1 = _gen_key_bytes()
        key2 = _gen_key_bytes()

        trust_data = {
            "keys": {
                "wsi_11111111": {
                    "1": {
                        "public_key": base64.b64encode(key1).decode(),
                        "status": "active",
                        "added": "2024-01-01T00:00:00+00:00",
                        "metadata": {},
                    }
                },
                "wsi_22222222": {
                    "1": {
                        "public_key": base64.b64encode(key2).decode(),
                        "status": "active",
                        "added": "2024-01-01T00:00:00+00:00",
                        "metadata": {},
                    }
                },
            }
        }

        store.update_from_dict(trust_data)

        assert store.is_trusted("wsi_11111111") is True
        assert store.is_trusted("wsi_22222222") is True


def test_export_snapshot():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        store = TrustStore(trust_file=path)

        key1 = _gen_key_bytes()
        key2 = _gen_key_bytes()
        key3 = _gen_key_bytes()
        store.add_trusted("wsi_aaa", key1, key_version=1)
        store.add_trusted("wsi_bbb", key2, key_version=1)
        store.add_trusted("wsi_ccc", key3, key_version=1)

        snapshot = store.export_snapshot(["wsi_aaa", "wsi_ccc"])

        assert "wsi_aaa" in snapshot["keys"]
        assert "wsi_ccc" in snapshot["keys"]
        assert "wsi_bbb" not in snapshot["keys"]


def test_thread_safety():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "trust.json")
        store = TrustStore(trust_file=path)

        errors = []

        def add_keys(start):
            try:
                for i in range(20):
                    key_bytes = _gen_key_bytes()
                    store.add_trusted(f"wsi_{start + i:08x}", key_bytes, key_version=1)
            except Exception as e:
                errors.append(e)

        def query_keys(start):
            try:
                for i in range(20):
                    store.is_trusted(f"wsi_{start + i:08x}")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=add_keys, args=(0,)),
            threading.Thread(target=add_keys, args=(100,)),
            threading.Thread(target=query_keys, args=(0,)),
            threading.Thread(target=query_keys, args=(100,)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
