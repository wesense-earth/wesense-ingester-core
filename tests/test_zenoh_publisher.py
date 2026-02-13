"""Tests for ZenohPublisher — zenoh mocked via sys.modules patch."""

import importlib
import json
import sys
from unittest.mock import MagicMock, patch

from wesense_ingester.zenoh.config import ZenohConfig


def _make_publisher(config=None, signer=None):
    """Create a ZenohPublisher with mocked zenoh module."""
    mock_zenoh = MagicMock()
    mock_session = MagicMock()
    mock_zenoh.open.return_value = mock_session
    mock_zenoh.Config.from_json5.return_value = MagicMock()

    with patch.dict(sys.modules, {"zenoh": mock_zenoh}):
        import wesense_ingester.zenoh.publisher as pub_mod
        importlib.reload(pub_mod)

        cfg = config or ZenohConfig(
            mode="client",
            routers=["tcp/localhost:7447"],
            enabled=True,
        )
        publisher = pub_mod.ZenohPublisher(config=cfg, signer=signer)
        # Simulate successful connection
        publisher._session = mock_session
        publisher._connected = True

        return publisher, mock_session, mock_zenoh


def _sample_reading(**overrides):
    reading = {
        "device_id": "sensor-001",
        "data_source": "WESENSE",
        "geo_country": "nz",
        "geo_subdivision": "auk",
        "reading_type": "temperature",
        "value": 22.5,
    }
    reading.update(overrides)
    return reading


def test_publish_constructs_correct_key_expr():
    publisher, mock_session, _ = _make_publisher()
    mock_pub = MagicMock()
    mock_session.declare_publisher.return_value = mock_pub

    publisher.publish_reading(_sample_reading())

    mock_session.declare_publisher.assert_called_once_with("wesense/v2/live/nz/auk/sensor-001")
    mock_pub.put.assert_called_once()


def test_publish_with_signer_produces_signed_envelope():
    mock_signer = MagicMock()
    mock_signed = MagicMock()
    mock_signed.SerializeToString.return_value = b"signed-data"
    mock_signer.sign.return_value = mock_signed

    publisher, mock_session, _ = _make_publisher(signer=mock_signer)
    mock_pub = MagicMock()
    mock_session.declare_publisher.return_value = mock_pub

    publisher.publish_reading(_sample_reading())

    mock_signer.sign.assert_called_once()
    mock_pub.put.assert_called_once_with(b"signed-data")


def test_publish_without_signer_sends_raw_json():
    publisher, mock_session, _ = _make_publisher(signer=None)
    mock_pub = MagicMock()
    mock_session.declare_publisher.return_value = mock_pub

    reading = _sample_reading()
    publisher.publish_reading(reading)

    data = mock_pub.put.call_args[0][0]
    parsed = json.loads(data)
    assert parsed["device_id"] == "sensor-001"
    assert parsed["value"] == 22.5


def test_publish_sorts_json_keys():
    publisher, mock_session, _ = _make_publisher(signer=None)
    mock_pub = MagicMock()
    mock_session.declare_publisher.return_value = mock_pub

    publisher.publish_reading(_sample_reading())

    data = mock_pub.put.call_args[0][0]
    keys = list(json.loads(data).keys())
    assert keys == sorted(keys)


def test_publish_returns_false_when_not_connected():
    publisher, _, _ = _make_publisher()
    publisher._connected = False

    result = publisher.publish_reading(_sample_reading())
    assert result is False


def test_publish_returns_true_on_success():
    publisher, mock_session, _ = _make_publisher()
    mock_pub = MagicMock()
    mock_session.declare_publisher.return_value = mock_pub

    result = publisher.publish_reading(_sample_reading())
    assert result is True


def test_publish_returns_false_on_exception():
    publisher, mock_session, _ = _make_publisher()
    mock_pub = MagicMock()
    mock_pub.put.side_effect = RuntimeError("network error")
    mock_session.declare_publisher.return_value = mock_pub

    result = publisher.publish_reading(_sample_reading())
    assert result is False


def test_missing_reading_fields_default_to_unknown():
    publisher, mock_session, _ = _make_publisher()
    mock_pub = MagicMock()
    mock_session.declare_publisher.return_value = mock_pub

    publisher.publish_reading({"device_id": "sensor-001"})

    mock_session.declare_publisher.assert_called_once_with("wesense/v2/live/unknown/unknown/sensor-001")


def test_connect_disabled():
    mock_zenoh = MagicMock()
    with patch.dict(sys.modules, {"zenoh": mock_zenoh}):
        import wesense_ingester.zenoh.publisher as pub_mod
        importlib.reload(pub_mod)

        config = ZenohConfig(enabled=False)
        publisher = pub_mod.ZenohPublisher(config=config)
        publisher.connect()

        mock_zenoh.open.assert_not_called()
        assert publisher.is_connected() is False


def test_close_undeclares_publishers_and_session():
    publisher, mock_session, _ = _make_publisher()

    mock_pub1 = MagicMock()
    mock_pub2 = MagicMock()
    publisher._publishers = {"key1": mock_pub1, "key2": mock_pub2}

    publisher.close()

    mock_pub1.undeclare.assert_called_once()
    mock_pub2.undeclare.assert_called_once()
    mock_session.close.assert_called_once()
    assert publisher._connected is False
    assert publisher._publishers == {}


def test_lazy_publisher_declaration():
    publisher, mock_session, _ = _make_publisher()
    mock_pub = MagicMock()
    mock_session.declare_publisher.return_value = mock_pub

    publisher.publish_reading(_sample_reading())

    assert mock_session.declare_publisher.call_count == 1
    assert "wesense/v2/live/nz/auk/sensor-001" in publisher._publishers


def test_publisher_caching():
    publisher, mock_session, _ = _make_publisher()
    mock_pub = MagicMock()
    mock_session.declare_publisher.return_value = mock_pub

    reading = _sample_reading()
    publisher.publish_reading(reading)
    publisher.publish_reading(reading)

    # Only one declare_publisher call — cached on second publish
    assert mock_session.declare_publisher.call_count == 1
    assert mock_pub.put.call_count == 2
