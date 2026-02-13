"""Tests for ZenohSubscriber — zenoh and signing mocked."""

import importlib
import json
import sys
from unittest.mock import MagicMock, patch, call

from wesense_ingester.zenoh.config import ZenohConfig


def _make_subscriber(trust_store=None, on_reading=None, config=None):
    """Create a ZenohSubscriber with mocked zenoh module."""
    mock_zenoh = MagicMock()
    mock_session = MagicMock()
    mock_zenoh.open.return_value = mock_session
    mock_zenoh.Config.from_json5.return_value = MagicMock()

    with patch.dict(sys.modules, {"zenoh": mock_zenoh}):
        import wesense_ingester.zenoh.subscriber as sub_mod
        importlib.reload(sub_mod)

        cfg = config or ZenohConfig(
            mode="client",
            routers=["tcp/localhost:7447"],
            enabled=True,
        )
        subscriber = sub_mod.ZenohSubscriber(
            config=cfg,
            trust_store=trust_store,
            on_reading=on_reading,
        )
        subscriber._session = mock_session
        subscriber._connected = True

        return subscriber, mock_session, mock_zenoh


def _make_signed_sample(reading_dict, ingester_id="wsi_a1b2c3d4", key_version=1):
    """Create a mock sample carrying a SignedReading protobuf."""
    payload_bytes = json.dumps(reading_dict, sort_keys=True).encode()

    mock_signed = MagicMock()
    mock_signed.signature = b"valid-sig"
    mock_signed.ingester_id = ingester_id
    mock_signed.key_version = key_version
    mock_signed.payload = payload_bytes

    sample = MagicMock()
    sample.payload = mock_signed.SerializeToString() if hasattr(mock_signed, 'SerializeToString') else b"proto-bytes"

    return sample, mock_signed, payload_bytes


def test_subscribe_declares_subscriber():
    subscriber, mock_session, _ = _make_subscriber()
    mock_sub = MagicMock()
    mock_session.declare_subscriber.return_value = mock_sub

    subscriber.subscribe("wesense/v2/live/nz/**")

    mock_session.declare_subscriber.assert_called_once()
    args = mock_session.declare_subscriber.call_args
    assert args[0][0] == "wesense/v2/live/nz/**"


def test_on_sample_verifies_signature():
    """Happy path: signed reading from trusted ingester is delivered."""
    callback = MagicMock()
    mock_trust = MagicMock()
    mock_trust.is_trusted.return_value = True

    mock_pubkey = MagicMock()
    mock_trust.get_public_key.return_value = mock_pubkey

    subscriber, _, _ = _make_subscriber(trust_store=mock_trust, on_reading=callback)

    reading = {"device_id": "sensor-001", "value": 22.5}
    payload_bytes = json.dumps(reading, sort_keys=True).encode()

    mock_signed = MagicMock()
    mock_signed.signature = b"valid-sig"
    mock_signed.ingester_id = "wsi_a1b2c3d4"
    mock_signed.key_version = 1
    mock_signed.payload = payload_bytes

    # Patch ReadingSigner in the subscriber module
    with patch("wesense_ingester.zenoh.subscriber.ReadingSigner") as MockSigner:
        MockSigner.deserialize.return_value = mock_signed
        MockSigner.verify.return_value = True

        sample = MagicMock()
        sample.payload = b"serialized-proto"

        subscriber._on_sample(sample)

    callback.assert_called_once()
    delivered_reading, delivered_signed = callback.call_args[0]
    assert delivered_reading["device_id"] == "sensor-001"
    assert delivered_signed is mock_signed
    assert subscriber.stats["verified"] == 1


def test_on_sample_rejects_untrusted_ingester():
    callback = MagicMock()
    mock_trust = MagicMock()
    mock_trust.is_trusted.return_value = False

    subscriber, _, _ = _make_subscriber(trust_store=mock_trust, on_reading=callback)

    reading = {"device_id": "sensor-001"}
    payload_bytes = json.dumps(reading, sort_keys=True).encode()

    mock_signed = MagicMock()
    mock_signed.signature = b"sig"
    mock_signed.ingester_id = "wsi_untrusted"
    mock_signed.key_version = 1
    mock_signed.payload = payload_bytes

    with patch("wesense_ingester.zenoh.subscriber.ReadingSigner") as MockSigner:
        MockSigner.deserialize.return_value = mock_signed

        sample = MagicMock()
        sample.payload = b"proto-bytes"
        subscriber._on_sample(sample)

    callback.assert_not_called()
    assert subscriber.stats["rejected"] == 1


def test_on_sample_rejects_invalid_signature():
    callback = MagicMock()
    mock_trust = MagicMock()
    mock_trust.is_trusted.return_value = True
    mock_trust.get_public_key.return_value = MagicMock()

    subscriber, _, _ = _make_subscriber(trust_store=mock_trust, on_reading=callback)

    reading = {"device_id": "sensor-001"}
    payload_bytes = json.dumps(reading, sort_keys=True).encode()

    mock_signed = MagicMock()
    mock_signed.signature = b"bad-sig"
    mock_signed.ingester_id = "wsi_a1b2c3d4"
    mock_signed.key_version = 1
    mock_signed.payload = payload_bytes

    with patch("wesense_ingester.zenoh.subscriber.ReadingSigner") as MockSigner:
        MockSigner.deserialize.return_value = mock_signed
        MockSigner.verify.return_value = False

        sample = MagicMock()
        sample.payload = b"proto-bytes"
        subscriber._on_sample(sample)

    callback.assert_not_called()
    assert subscriber.stats["rejected"] == 1


def test_on_sample_delivers_verified_reading():
    """Callback receives (dict, signed_reading) tuple."""
    callback = MagicMock()
    subscriber, _, _ = _make_subscriber(trust_store=None, on_reading=callback)

    reading = {"device_id": "sensor-001", "value": 42}
    payload_bytes = json.dumps(reading, sort_keys=True).encode()

    mock_signed = MagicMock()
    mock_signed.signature = b"sig"
    mock_signed.ingester_id = "wsi_test1234"
    mock_signed.key_version = 1
    mock_signed.payload = payload_bytes

    with patch("wesense_ingester.zenoh.subscriber.ReadingSigner") as MockSigner:
        MockSigner.deserialize.return_value = mock_signed

        sample = MagicMock()
        sample.payload = b"proto-bytes"
        subscriber._on_sample(sample)

    callback.assert_called_once()
    delivered_reading, delivered_signed = callback.call_args[0]
    assert delivered_reading == reading
    assert delivered_signed is mock_signed


def test_on_sample_handles_unsigned_json():
    """Raw JSON fallback when message is not valid protobuf."""
    callback = MagicMock()
    subscriber, _, _ = _make_subscriber(trust_store=None, on_reading=callback)

    reading = {"device_id": "sensor-001", "value": 99}
    raw_json = json.dumps(reading).encode()

    with patch("wesense_ingester.zenoh.subscriber.ReadingSigner") as MockSigner:
        MockSigner.deserialize.side_effect = Exception("not protobuf")

        sample = MagicMock()
        sample.payload = raw_json
        subscriber._on_sample(sample)

    callback.assert_called_once()
    delivered_reading, delivered_signed = callback.call_args[0]
    assert delivered_reading["device_id"] == "sensor-001"
    assert delivered_signed is None
    assert subscriber.stats["unsigned"] == 1


def test_stats_tracking():
    callback = MagicMock()
    mock_trust = MagicMock()
    mock_trust.is_trusted.return_value = True
    mock_trust.get_public_key.return_value = MagicMock()

    subscriber, _, _ = _make_subscriber(trust_store=mock_trust, on_reading=callback)

    reading = {"device_id": "sensor-001"}
    payload_bytes = json.dumps(reading, sort_keys=True).encode()

    # One verified
    mock_signed = MagicMock()
    mock_signed.signature = b"sig"
    mock_signed.ingester_id = "wsi_trusted"
    mock_signed.key_version = 1
    mock_signed.payload = payload_bytes

    with patch("wesense_ingester.zenoh.subscriber.ReadingSigner") as MockSigner:
        MockSigner.deserialize.return_value = mock_signed
        MockSigner.verify.return_value = True

        sample = MagicMock()
        sample.payload = b"proto"
        subscriber._on_sample(sample)

    # One rejected (bad sig)
    with patch("wesense_ingester.zenoh.subscriber.ReadingSigner") as MockSigner:
        MockSigner.deserialize.return_value = mock_signed
        MockSigner.verify.return_value = False

        sample = MagicMock()
        sample.payload = b"proto"
        subscriber._on_sample(sample)

    # One unsigned (raw JSON)
    with patch("wesense_ingester.zenoh.subscriber.ReadingSigner") as MockSigner:
        MockSigner.deserialize.side_effect = Exception("nope")

        sample = MagicMock()
        sample.payload = json.dumps(reading).encode()
        subscriber._on_sample(sample)

    stats = subscriber.stats
    assert stats["received"] == 3
    assert stats["verified"] == 1
    assert stats["rejected"] == 1
    assert stats["unsigned"] == 1


def test_subscribe_when_not_connected():
    subscriber, mock_session, _ = _make_subscriber()
    subscriber._connected = False

    subscriber.subscribe("wesense/v2/live/**")

    mock_session.declare_subscriber.assert_not_called()


def test_close_undeclares_subscribers():
    subscriber, mock_session, _ = _make_subscriber()

    mock_sub1 = MagicMock()
    mock_sub2 = MagicMock()
    subscriber._subscribers = [mock_sub1, mock_sub2]

    subscriber.close()

    mock_sub1.undeclare.assert_called_once()
    mock_sub2.undeclare.assert_called_once()
    mock_session.close.assert_called_once()
    assert subscriber._connected is False
    assert subscriber._subscribers == []


def test_connect_disabled():
    mock_zenoh = MagicMock()
    with patch.dict(sys.modules, {"zenoh": mock_zenoh}):
        import wesense_ingester.zenoh.subscriber as sub_mod
        importlib.reload(sub_mod)

        config = ZenohConfig(enabled=False)
        subscriber = sub_mod.ZenohSubscriber(config=config)
        subscriber.connect()

        mock_zenoh.open.assert_not_called()
        assert subscriber.is_connected() is False
