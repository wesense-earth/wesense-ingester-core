"""Configuration for the OrbitDB registry client."""

import os
from dataclasses import dataclass


@dataclass
class RegistryConfig:
    """OrbitDB registry connection settings."""

    enabled: bool = False
    url: str = "http://wesense-orbitdb:5200"
    sync_interval: int = 3600  # seconds (1 hour)

    @classmethod
    def from_env(cls) -> "RegistryConfig":
        return cls(
            enabled=os.getenv("ORBITDB_ENABLED", "false").lower() == "true",
            url=os.getenv("ORBITDB_URL", "http://wesense-orbitdb:5200"),
            sync_interval=int(os.getenv("ORBITDB_SYNC_INTERVAL", "3600")),
        )
