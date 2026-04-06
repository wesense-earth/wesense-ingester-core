"""Configuration for the OrbitDB registry client."""

import os
from dataclasses import dataclass


@dataclass
class RegistryConfig:
    """OrbitDB registry connection settings."""

    enabled: bool = True
    url: str = "http://wesense-orbitdb:5200"
    sync_interval: int = 3600  # seconds (1 hour)

    @classmethod
    def from_env(cls) -> "RegistryConfig":
        url = os.getenv("ORBITDB_URL", "http://wesense-orbitdb:5200")
        if os.getenv("TLS_ENABLED", "").lower() == "true":
            url = url.replace("http://", "https://")
        return cls(
            enabled=os.getenv("ORBITDB_ENABLED", "true").lower() == "true",
            url=url,
            sync_interval=int(os.getenv("ORBITDB_SYNC_INTERVAL", "3600")),
        )
