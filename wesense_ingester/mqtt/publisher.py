"""
MQTT publisher for WeSense decoded readings.

Publishes readings to the WeSense MQTT hub with hierarchical topic
construction: {prefix}/{data_source}/{country}/{subdivision}/{device_id}
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:
    import paho.mqtt.client as mqtt
    _MQTT_AVAILABLE = True
except ImportError:
    _MQTT_AVAILABLE = False


@dataclass
class MQTTPublisherConfig:
    """MQTT publisher connection configuration."""

    broker: str = "localhost"
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    client_id: str = "wesense-publisher"
    topic_prefix: str = "wesense/decoded"

    @classmethod
    def from_env(cls, client_id: str = "wesense-publisher") -> "MQTTPublisherConfig":
        """Create config from environment variables."""
        return cls(
            broker=os.getenv("MQTT_BROKER", "localhost"),
            port=int(os.getenv("MQTT_PORT", "1883")),
            username=os.getenv("MQTT_USERNAME"),
            password=os.getenv("MQTT_PASSWORD"),
            client_id=client_id,
            topic_prefix=os.getenv("MQTT_TOPIC_PREFIX", "wesense/decoded"),
        )


class WeSensePublisher:
    """
    MQTT publisher for decoded WeSense readings.

    Constructs topic from reading fields:
        {prefix}/{data_source}/{country}/{subdivision}/{device_id}

    Uses paho-mqtt v2 callback API.
    """

    def __init__(self, config: Optional[MQTTPublisherConfig] = None):
        """
        Args:
            config: MQTT connection config. Defaults to MQTTPublisherConfig.from_env().
        """
        if not _MQTT_AVAILABLE:
            raise ImportError(
                "paho-mqtt not available. "
                "Install with: pip install paho-mqtt>=2.0.0"
            )

        self.config = config or MQTTPublisherConfig.from_env()
        self._client: Optional[mqtt.Client] = None
        self._connected = False

    def connect(self) -> None:
        """Connect to the MQTT broker."""
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.config.client_id,
        )

        if self.config.username:
            self._client.username_pw_set(self.config.username, self.config.password)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

        try:
            self._client.connect(self.config.broker, self.config.port, keepalive=60)
            self._client.loop_start()
            logger.info(
                "MQTT publisher connecting to %s:%d",
                self.config.broker, self.config.port,
            )
        except Exception as e:
            logger.error("MQTT connection failed: %s", e)
            self._client = None

    def _on_connect(self, client: Any, userdata: Any, flags: Any, rc: Any, properties: Any = None) -> None:
        """paho-mqtt v2 on_connect callback."""
        self._connected = True
        logger.info("MQTT publisher connected to %s:%d", self.config.broker, self.config.port)

    def _on_disconnect(self, client: Any, userdata: Any, flags: Any, rc: Any, properties: Any = None) -> None:
        """paho-mqtt v2 on_disconnect callback."""
        self._connected = False
        logger.warning("MQTT publisher disconnected (rc=%s)", rc)

    def publish_reading(self, reading: dict[str, Any]) -> bool:
        """
        Publish a reading to the MQTT hub.

        Constructs topic from reading fields:
            {prefix}/{data_source}/{country}/{subdivision}/{device_id}

        Args:
            reading: Reading dict with at least device_id, data_source,
                     geo_country, geo_subdivision.

        Returns:
            True if publish was attempted, False if not connected.
        """
        if not self._client or not self._connected:
            return False

        data_source = (reading.get("data_source") or "unknown").lower()
        country = (reading.get("geo_country") or "unknown").lower()
        subdivision = (reading.get("geo_subdivision") or "unknown").lower()
        device_id = reading.get("device_id", "unknown")

        topic = f"{self.config.topic_prefix}/{data_source}/{country}/{subdivision}/{device_id}"

        try:
            payload = json.dumps(reading, default=str)
            self._client.publish(topic, payload)
            logger.debug("Published to %s", topic)
            return True
        except Exception as e:
            logger.error("Failed to publish to %s: %s", topic, e)
            return False

    def is_connected(self) -> bool:
        """Return whether the publisher is connected to the broker."""
        return self._connected

    def close(self) -> None:
        """Disconnect from the MQTT broker."""
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._connected = False
