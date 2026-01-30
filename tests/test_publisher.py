"""Tests for MQTT publisher."""

import json
from unittest.mock import MagicMock, patch, call


def _make_publisher(prefix="wesense/decoded"):
    """Create a publisher with mocked paho-mqtt."""
    mock_mqtt = MagicMock()
    mock_client_instance = MagicMock()
    mock_mqtt.Client.return_value = mock_client_instance

    # Need to provide CallbackAPIVersion
    mock_mqtt.CallbackAPIVersion.VERSION2 = 2

    with patch.dict("sys.modules", {"paho.mqtt.client": mock_mqtt, "paho.mqtt": MagicMock(), "paho": MagicMock()}):
        import importlib
        import wesense_ingester.mqtt.publisher as pub_mod
        importlib.reload(pub_mod)

        from wesense_ingester.mqtt.publisher import WeSensePublisher, MQTTPublisherConfig

        config = MQTTPublisherConfig(
            broker="localhost",
            port=1883,
            client_id="test-publisher",
            topic_prefix=prefix,
        )
        publisher = WeSensePublisher(config=config)
        publisher._client = mock_client_instance
        publisher._connected = True

        return publisher, mock_client_instance


def test_topic_construction():
    publisher, mock_client = _make_publisher()

    reading = {
        "device_id": "sensor-001",
        "data_source": "WESENSE",
        "geo_country": "nz",
        "geo_subdivision": "auk",
        "reading_type": "temperature",
        "value": 22.5,
    }

    publisher.publish_reading(reading)

    mock_client.publish.assert_called_once()
    topic = mock_client.publish.call_args[0][0]
    assert topic == "wesense/decoded/wesense/nz/auk/sensor-001"


def test_topic_with_meshtastic_source():
    publisher, mock_client = _make_publisher()

    reading = {
        "device_id": "!e4cc140c",
        "data_source": "MESHTASTIC_PUBLIC",
        "geo_country": "nz",
        "geo_subdivision": "auk",
    }

    publisher.publish_reading(reading)

    topic = mock_client.publish.call_args[0][0]
    assert topic == "wesense/decoded/meshtastic_public/nz/auk/!e4cc140c"


def test_missing_fields_default_to_unknown():
    publisher, mock_client = _make_publisher()

    reading = {
        "device_id": "sensor-001",
    }

    publisher.publish_reading(reading)

    topic = mock_client.publish.call_args[0][0]
    assert topic == "wesense/decoded/unknown/unknown/unknown/sensor-001"


def test_payload_is_valid_json():
    publisher, mock_client = _make_publisher()

    reading = {
        "device_id": "sensor-001",
        "data_source": "WESENSE",
        "geo_country": "nz",
        "geo_subdivision": "auk",
        "value": 22.5,
    }

    publisher.publish_reading(reading)

    payload = mock_client.publish.call_args[0][1]
    parsed = json.loads(payload)
    assert parsed["value"] == 22.5
    assert parsed["device_id"] == "sensor-001"


def test_not_connected_returns_false():
    publisher, mock_client = _make_publisher()
    publisher._connected = False

    result = publisher.publish_reading({"device_id": "sensor-001"})
    assert result is False
    mock_client.publish.assert_not_called()


def test_connected_returns_true():
    publisher, mock_client = _make_publisher()

    result = publisher.publish_reading({
        "device_id": "sensor-001",
        "data_source": "WESENSE",
        "geo_country": "nz",
        "geo_subdivision": "auk",
    })
    assert result is True
