"""Tests for reading signer and verifier."""

import tempfile

from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig
from wesense_ingester.signing.signer import ReadingSigner


def _make_signer(tmpdir, key_file="test_key.pem"):
    """Create a signer with a fresh keypair."""
    config = KeyConfig(key_dir=tmpdir, key_file=key_file)
    km = IngesterKeyManager(config=config)
    km.load_or_generate()
    return ReadingSigner(km), km


def test_sign_produces_valid_envelope():
    with tempfile.TemporaryDirectory() as tmpdir:
        signer, km = _make_signer(tmpdir)
        envelope = signer.sign(b"test payload")

        assert envelope.payload == b"test payload"
        assert len(envelope.signature) == 64  # Ed25519 signature is 64 bytes
        assert envelope.ingester_id == km.ingester_id
        assert envelope.key_version == km.key_version


def test_sign_verify_roundtrip():
    with tempfile.TemporaryDirectory() as tmpdir:
        signer, km = _make_signer(tmpdir)
        envelope = signer.sign(b"hello world")

        assert ReadingSigner.verify(envelope, km.public_key) is True


def test_verify_tampered_payload_fails():
    with tempfile.TemporaryDirectory() as tmpdir:
        signer, km = _make_signer(tmpdir)
        envelope = signer.sign(b"original data")

        # Tamper with payload
        envelope.payload = b"tampered data"
        assert ReadingSigner.verify(envelope, km.public_key) is False


def test_verify_tampered_signature_fails():
    with tempfile.TemporaryDirectory() as tmpdir:
        signer, km = _make_signer(tmpdir)
        envelope = signer.sign(b"test data")

        # Tamper with signature
        bad_sig = bytearray(envelope.signature)
        bad_sig[0] ^= 0xFF
        envelope.signature = bytes(bad_sig)
        assert ReadingSigner.verify(envelope, km.public_key) is False


def test_verify_wrong_key_fails():
    with tempfile.TemporaryDirectory() as tmpdir:
        signer1, km1 = _make_signer(tmpdir, key_file="key1.pem")
        _, km2 = _make_signer(tmpdir, key_file="key2.pem")

        envelope = signer1.sign(b"signed by key1")
        assert ReadingSigner.verify(envelope, km2.public_key) is False


def test_deserialize_roundtrip():
    with tempfile.TemporaryDirectory() as tmpdir:
        signer, _ = _make_signer(tmpdir)
        envelope = signer.sign(b"roundtrip test")

        serialized = envelope.SerializeToString()
        restored = ReadingSigner.deserialize(serialized)

        assert restored.payload == b"roundtrip test"
        assert restored.signature == envelope.signature
        assert restored.ingester_id == envelope.ingester_id
        assert restored.key_version == envelope.key_version


def test_ingester_id_in_envelope():
    with tempfile.TemporaryDirectory() as tmpdir:
        signer, km = _make_signer(tmpdir)
        envelope = signer.sign(b"payload")

        assert envelope.ingester_id == km.ingester_id
        assert envelope.ingester_id.startswith("wsi_")


def test_key_version_in_envelope():
    with tempfile.TemporaryDirectory() as tmpdir:
        signer, km = _make_signer(tmpdir)
        envelope = signer.sign(b"payload")

        assert envelope.key_version == 1
        assert envelope.key_version == km.key_version
