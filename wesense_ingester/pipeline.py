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
from wesense_ingester.reading_types import get_display_name
from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig
from wesense_ingester.signing.signer import ReadingSigner

logger = logging.getLogger(__name__)

# =============================================================================
# Canonical reading versioning
#
# Each version of the canonical schema is FROZEN. Never modify CANONICAL_FIELDS_V1
# or build_canonical_v1() — they must produce byte-identical output forever so
# that signatures created in 2026 can still be verified in 2225.
#
# To add or change canonical fields:
#   1. Define CANONICAL_FIELDS_V2 and build_canonical_v2()
#   2. Bump CURRENT_CANONICAL_VERSION to 2
#   3. Add v2 to the CANONICAL_BUILDERS dispatcher
#   4. Readings signed under v1 continue to verify against build_canonical_v1
#
# The signing_payload_version field on every reading tells verifiers which
# builder to use. This is the forward-compatibility guarantee.
# =============================================================================

CURRENT_CANONICAL_VERSION = 1

# v1 — frozen 2026-04-14. DO NOT MODIFY.
CANONICAL_FIELDS_V1 = [
    "device_id",
    "timestamp",
    "reading_type",
    "reading_type_name",
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

# Backward-compat alias — current version's fields (what new code uses today)
CANONICAL_FIELDS = CANONICAL_FIELDS_V1


def build_canonical_v1(reading: dict) -> dict:
    """
    Build the v1 canonical reading. FROZEN — do not modify.

    Version 1 was frozen on 2026-04-14 with 24 fields. Any change to
    canonical field names, types, or defaults requires a new version.
    """
    lat = reading.get("latitude")
    lon = reading.get("longitude")
    alt = reading.get("altitude")

    reading_type = str(reading["reading_type"])
    reading_type_name = str(
        reading.get("reading_type_name") or get_display_name(reading_type)
    )

    return {
        "device_id": str(reading["device_id"]),
        "timestamp": int(reading["timestamp"]),
        "reading_type": reading_type,
        "reading_type_name": reading_type_name,
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


# Dispatcher: version number → builder function
# When adding v2, add the entry here too.
CANONICAL_BUILDERS = {
    1: build_canonical_v1,
}


def build_canonical(reading: dict, version: int | None = None) -> dict:
    """
    Build the canonical reading using the requested version's builder.

    If version is None, uses CURRENT_CANONICAL_VERSION (for signing new readings).
    Verifiers pass the version from the reading to reproduce the exact payload
    that was originally signed.

    Raises ValueError for unknown versions.
    """
    v = version if version is not None else CURRENT_CANONICAL_VERSION
    builder = CANONICAL_BUILDERS.get(v)
    if builder is None:
        raise ValueError(
            f"Unknown canonical version {v}. "
            f"This code only knows versions {sorted(CANONICAL_BUILDERS.keys())}. "
            f"Upgrade the station to support newer signatures."
        )
    return builder(reading)


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
        enable_orbitdb_registry: bool = True,
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

        # OrbitDB registry — node registration + trust sync.
        # The pipeline exposes self.trust_store for verifying inbound signatures
        # (used by the live transport bridge; ingesters typically don't need
        # to verify inbound, but having a consistent setup is harmless).
        self._registry_client = None
        self._trust_store = None
        if enable_orbitdb_registry:
            try:
                # Lazy imports — these pull in heavier dependencies that some
                # consumers (e.g. the storage broker when used standalone) don't need.
                from wesense_ingester.registry import RegistryClient, RegistryConfig
                from wesense_ingester.signing.trust import TrustStore

                self._trust_store = TrustStore()
                _reg_config = RegistryConfig.from_env()
                if _reg_config.enabled:
                    self._registry_client = RegistryClient(
                        config=_reg_config,
                        trust_store=self._trust_store,
                    )
                    try:
                        self._registry_client.register_node(
                            ingester_id=self._key_manager.ingester_id,
                            public_key_bytes=self._key_manager.public_key_bytes,
                            key_version=self._key_manager.key_version,
                        )
                    except Exception as e:
                        logger.warning(
                            "Pipeline '%s': OrbitDB registration failed (%s), "
                            "will retry on next trust sync", name, e,
                        )
                    self._registry_client.start_trust_sync()
                    logger.info(
                        "Pipeline '%s': OrbitDB registry active (trust sync started)", name,
                    )
                else:
                    logger.info(
                        "Pipeline '%s': OrbitDB registry disabled (REGISTRY_ENABLED != true)",
                        name,
                    )
            except Exception as e:
                logger.warning(
                    "Pipeline '%s': OrbitDB registry setup failed: %s", name, e,
                )

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

    @property
    def trust_store(self):
        """Access the trust store (may be None if OrbitDB registry disabled)."""
        return self._trust_store

    @property
    def registry_client(self):
        """Access the OrbitDB registry client (may be None if disabled)."""
        return self._registry_client

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

        # Signature metadata (travels alongside canonical, not part of it).
        # signing_payload_version tells verifiers which builder produced the
        # signed bytes — essential for long-term signature verification.
        sig_fields = {
            "signature": sig_hex,
            "ingester_id": self._key_manager.ingester_id,
            "key_version": self._key_manager.key_version,
            "signing_payload_version": CURRENT_CANONICAL_VERSION,
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
        if self._registry_client:
            try:
                self._registry_client.stop_trust_sync()
            except Exception as e:
                logger.debug("Registry stop_trust_sync failed: %s", e)
        if self._gateway:
            self._gateway.close()
        if self._publisher:
            self._publisher.close()
        logger.info("Pipeline '%s' shut down.", self._name)
