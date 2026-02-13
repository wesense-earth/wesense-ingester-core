"""Tests for Ed25519 key management."""

import json
import os
import re
import tempfile

from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig


def _make_manager(tmpdir):
    """Create a key manager using a temporary directory."""
    config = KeyConfig(key_dir=tmpdir, key_file="test_key.pem")
    km = IngesterKeyManager(config=config)
    km.load_or_generate()
    return km


def test_generate_new_keypair():
    with tempfile.TemporaryDirectory() as tmpdir:
        km = _make_manager(tmpdir)
        assert km.private_key is not None
        assert km.public_key is not None


def test_load_existing_keypair():
    with tempfile.TemporaryDirectory() as tmpdir:
        km1 = _make_manager(tmpdir)
        pub1 = km1.public_key_bytes

        # Reload from same files
        config = KeyConfig(key_dir=tmpdir, key_file="test_key.pem")
        km2 = IngesterKeyManager(config=config)
        km2.load_or_generate()

        assert km2.public_key_bytes == pub1


def test_ingester_id_format():
    with tempfile.TemporaryDirectory() as tmpdir:
        km = _make_manager(tmpdir)
        assert re.match(r"^wsi_[0-9a-f]{8}$", km.ingester_id)


def test_ingester_id_deterministic():
    with tempfile.TemporaryDirectory() as tmpdir:
        km1 = _make_manager(tmpdir)
        id1 = km1.ingester_id

        # Reload
        config = KeyConfig(key_dir=tmpdir, key_file="test_key.pem")
        km2 = IngesterKeyManager(config=config)
        km2.load_or_generate()

        assert km2.ingester_id == id1


def test_key_version_default():
    with tempfile.TemporaryDirectory() as tmpdir:
        km = _make_manager(tmpdir)
        assert km.key_version == 1


def test_key_dir_created():
    with tempfile.TemporaryDirectory() as tmpdir:
        nested = os.path.join(tmpdir, "sub", "dir")
        config = KeyConfig(key_dir=nested, key_file="test_key.pem")
        km = IngesterKeyManager(config=config)
        km.load_or_generate()

        assert os.path.isdir(nested)


def test_pem_file_written():
    with tempfile.TemporaryDirectory() as tmpdir:
        km = _make_manager(tmpdir)
        pem_path = os.path.join(tmpdir, "test_key.pem")
        assert os.path.exists(pem_path)

        with open(pem_path, "rb") as f:
            content = f.read()
        assert b"BEGIN PRIVATE KEY" in content


def test_sidecar_file_written():
    with tempfile.TemporaryDirectory() as tmpdir:
        km = _make_manager(tmpdir)
        sidecar_path = os.path.join(tmpdir, "test_key.json")
        assert os.path.exists(sidecar_path)

        with open(sidecar_path, "r") as f:
            sidecar = json.load(f)
        assert sidecar["key_version"] == 1
        assert "created" in sidecar
