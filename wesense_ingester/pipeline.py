"""
ReadingPipeline — unified processing pipeline for all WeSense ingesters.

Enforces the Dual-Path Identity Invariant: every reading is built into a
canonical form, signed once, and the identical signed payload is sent to
both MQTT (for P2P distribution) and the storage broker (for archival).

Adapters call pipeline.process(reading_dict) and never touch signing,
MQTT publishing, or the gateway client directly.
"""

import json
import logging
import os
import socket

from wesense_ingester.cache.dedup import DeduplicationCache
from wesense_ingester.gateway.client import GatewayClient
from wesense_ingester.gateway.config import GatewayConfig
from wesense_ingester.geocoding.geocoder import ReverseGeocoder
from wesense_ingester.mqtt.publisher import MQTTPublisherConfig, WeSensePublisher
from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig
from wesense_ingester.signing.signer import ReadingSigner

logger = logging.getLogger(__name__)

# Fields that form the canonical reading — signed and archived.
# Order matters for documentation, not for signing (sort_keys=True handles that).
CANONICAL_FIELDS = [
    "device_id",
    "timestamp",
    "reading_type",
    "value",
    "unit",
    "latitude",
    "longitude",
    "altitude",
    "data_source",
    "data_source_name",
    "sensor_transport",
    "geo_country",
    "geo_subdivision",
    "board_model",
    "sensor_model",
    "calibration_status",
    "deployment_type",
    "deployment_type_source",
    "node_name",
    "node_info",
    "node_info_url",
    "location_source",
    "data_license",
]


def build_canonical(reading: dict) -> dict:
    """
    Build the canonical reading from adapter input.

    Enforces types, fills defaults, strips non-canonical fields.
    The output is what gets signed and archived — identical on every path.
    """
    lat = reading.get("latitude")
    lon = reading.get("longitude")
    alt = reading.get("altitude")

    return {
        "device_id": str(reading["device_id"]),
        "timestamp": int(reading["timestamp"]),
        "reading_type": str(reading["reading_type"]),
        "value": float(reading["value"]),
        "unit": str(reading.get("unit", "")),
        "latitude": float(lat) if lat is not None else None,
        "longitude": float(lon) if lon is not None else None,
        "altitude": float(alt) if alt is not None else None,
        "data_source": str(reading.get("data_source", "")),
        "data_source_name": str(reading.get("data_source_name", "")),
        "sensor_transport": str(reading.get("sensor_transport", "")),
        "geo_country": str(reading.get("geo_country", "")),
        "geo_subdivision": str(reading.get("geo_subdivision", "")),
        "board_model": str(reading.get("board_model", "")),
        "sensor_model": str(reading.get("sensor_model", "")),
        "calibration_status": str(reading.get("calibration_status", "")),
        "deployment_type": str(reading.get("deployment_type", "")),
        "deployment_type_source": str(reading.get("deployment_type_source", "")),
        "node_name": str(reading.get("node_name", "")),
        "node_info": str(reading.get("node_info", "")),
        "node_info_url": str(reading.get("node_info_url", "")),
        "location_source": str(reading.get("location_source", "")),
        "data_license": str(reading.get("data_license", "CC-BY-4.0")),
    }


def canonical_to_json(canonical: dict) -> bytes:
    """Serialize a canonical reading to deterministic JSON bytes for signing."""
    return json.dumps(canonical, sort_keys=True, default=str).encode()


class ReadingPipeline:
    """
    Unified reading pipeline: dedup → geocode → sign → MQTT + gateway.

    Enforces the Dual-Path Identity Invariant by building one canonical
    reading and sending the identical signed payload to both paths.
    """

    def __init__(
        self,
        name: str = "ingester",
        gateway_config: GatewayConfig | None = None,
        mqtt_config: MQTTPublisherConfig | None = None,
        key_config: KeyConfig | None = None,
        enable_dedup: bool = True,
        enable_geocoder: bool = True,
        node_id: str | None = None,
    ):
        self._name = name
        self._node_id = node_id or os.getenv("INGESTION_NODE_ID", socket.gethostname())

        # Deduplication
        self._dedup = DeduplicationCache() if enable_dedup else None

        # Geocoding
        self._geocoder = ReverseGeocoder() if enable_geocoder else None

        # Ed25519 signing
        _key_config = key_config or KeyConfig.from_env()
        self._key_manager = IngesterKeyManager(config=_key_config)
        self._key_manager.load_or_generate()
        self._signer = ReadingSigner(self._key_manager)
        logger.info(
            "Pipeline '%s' — ingester ID: %s (key version %d)",
            name, self._key_manager.ingester_id, self._key_manager.key_version,
        )

        # Storage broker (gateway)
        self._gateway = None
        try:
            _gw_config = gateway_config or GatewayConfig.from_env()
            self._gateway = GatewayClient(config=_gw_config)
        except Exception as e:
            logger.warning("No storage broker: %s (MQTT-only mode)", e)

        # MQTT publisher
        self._publisher = None
        try:
            _mqtt_config = mqtt_config or MQTTPublisherConfig.from_env(
                client_id=f"{name}_publisher"
            )
            self._publisher = WeSensePublisher(config=_mqtt_config)
            self._publisher.connect()
        except Exception as e:
            logger.warning("No MQTT publisher: %s", e)

    @property
    def ingester_id(self) -> str:
        """The pipeline's Ed25519 ingester identity (wsi_xxxxxxxx)."""
        return self._key_manager.ingester_id

    @property
    def key_manager(self) -> IngesterKeyManager:
        """Access the key manager (for OrbitDB registration etc.)."""
        return self._key_manager

    @property
    def dedup(self) -> DeduplicationCache | None:
        """Access the dedup cache (for stats etc.)."""
        return self._dedup

    @property
    def geocoder(self) -> ReverseGeocoder | None:
        """Access the geocoder (for adapters that geocode before calling process)."""
        return self._geocoder

    def process(self, reading: dict) -> bool:
        """
        Process a single reading through the full pipeline.

        The reading dict must contain at minimum: device_id, timestamp,
        reading_type, value, and latitude/longitude. All other fields
        default to empty strings or CC-BY-4.0 for data_license.

        Returns True if the reading was processed, False if it was a
        duplicate or failed validation.
        """
        device_id = reading.get("device_id", "")
        reading_type = reading.get("reading_type", "")
        timestamp = reading.get("timestamp", 0)

        # 1. Dedup
        if self._dedup and self._dedup.is_duplicate(device_id, reading_type, timestamp):
            return False

        # 2. Geocode if not already done
        if self._geocoder and not reading.get("geo_country"):
            lat = reading.get("latitude")
            lon = reading.get("longitude")
            if lat is not None and lon is not None:
                geo = self._geocoder.reverse_geocode(lat, lon)
                if geo:
                    reading["geo_country"] = geo["geo_country"]
                    reading["geo_subdivision"] = geo["geo_subdivision"]

        # 3. Build canonical reading (enforces types, fills defaults)
        canonical = build_canonical(reading)

        # 4. Sign the canonical reading
        canonical_bytes = canonical_to_json(canonical)
        signed = self._signer.sign(canonical_bytes)
        sig_hex = signed.signature.hex()

        # Signature metadata (travels alongside canonical, not part of it)
        sig_fields = {
            "signature": sig_hex,
            "ingester_id": self._key_manager.ingester_id,
            "key_version": self._key_manager.key_version,
        }

        # 5. MQTT publish — canonical + signature
        if self._publisher:
            self._publisher.publish_reading({**canonical, **sig_fields})

        # 6. Storage broker — canonical + signature + operational metadata
        #    Map sensor_transport → transport_type for ClickHouse column name
        if self._gateway:
            gateway_dict = {**canonical, **sig_fields}
            gateway_dict["transport_type"] = gateway_dict.pop("sensor_transport", "")
            gateway_dict["network_source"] = reading.get("network_source", "")
            gateway_dict["ingestion_node_id"] = self._node_id
            self._gateway.add(gateway_dict)

        return True

    def get_stats(self) -> dict:
        """Aggregate stats from pipeline components."""
        stats = {}
        if self._dedup:
            stats["dedup"] = self._dedup.get_stats()
        if self._gateway:
            stats["gateway"] = self._gateway.get_stats()
        if self._geocoder:
            stats["geocoder"] = self._geocoder.cache_info()
        return stats

    def close(self) -> None:
        """Shut down all pipeline components."""
        if self._gateway:
            self._gateway.close()
        if self._publisher:
            self._publisher.close()
        logger.info("Pipeline '%s' shut down.", self._name)
