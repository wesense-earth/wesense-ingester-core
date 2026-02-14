"""
wesense-ingester-core — Shared library for WeSense sensor data ingesters.

Convenience re-exports for the most commonly used classes and functions.
"""

from wesense_ingester.cache.dedup import DeduplicationCache
from wesense_ingester.cache.disk_cache import JSONDiskCache
from wesense_ingester.ids.reading_id import generate_reading_id
from wesense_ingester.logging.setup import setup_logging

__all__ = [
    "BufferedClickHouseWriter",
    "ClickHouseConfig",
    "DeduplicationCache",
    "IngesterKeyManager",
    "JSONDiskCache",
    "KeyConfig",
    "MQTTPublisherConfig",
    "ReadingSigner",
    "RegistryClient",
    "RegistryConfig",
    "ReverseGeocoder",
    "TrustStore",
    "WeSensePublisher",
    "ZenohConfig",
    "ZenohPublisher",
    "ZenohQueryable",
    "ZenohSubscriber",
    "generate_reading_id",
    "setup_logging",
]


def __getattr__(name: str):
    """Lazy imports for classes with heavy dependencies."""
    if name in ("BufferedClickHouseWriter", "ClickHouseConfig"):
        from wesense_ingester.clickhouse.writer import BufferedClickHouseWriter, ClickHouseConfig
        return {"BufferedClickHouseWriter": BufferedClickHouseWriter, "ClickHouseConfig": ClickHouseConfig}[name]

    if name in ("WeSensePublisher", "MQTTPublisherConfig"):
        from wesense_ingester.mqtt.publisher import WeSensePublisher, MQTTPublisherConfig
        return {"WeSensePublisher": WeSensePublisher, "MQTTPublisherConfig": MQTTPublisherConfig}[name]

    if name == "ReverseGeocoder":
        from wesense_ingester.geocoding.geocoder import ReverseGeocoder
        return ReverseGeocoder

    if name in ("IngesterKeyManager", "KeyConfig", "ReadingSigner", "TrustStore"):
        from wesense_ingester.signing import IngesterKeyManager, KeyConfig, ReadingSigner, TrustStore
        return {"IngesterKeyManager": IngesterKeyManager, "KeyConfig": KeyConfig,
                "ReadingSigner": ReadingSigner, "TrustStore": TrustStore}[name]

    if name in ("ZenohConfig", "ZenohPublisher", "ZenohSubscriber", "ZenohQueryable"):
        from wesense_ingester.zenoh import ZenohConfig, ZenohPublisher, ZenohSubscriber, ZenohQueryable
        return {"ZenohConfig": ZenohConfig, "ZenohPublisher": ZenohPublisher,
                "ZenohSubscriber": ZenohSubscriber, "ZenohQueryable": ZenohQueryable}[name]

    if name in ("RegistryConfig", "RegistryClient"):
        from wesense_ingester.registry import RegistryConfig, RegistryClient
        return {"RegistryConfig": RegistryConfig, "RegistryClient": RegistryClient}[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
