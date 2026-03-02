"""Configuration for the storage gateway client."""

import os
from dataclasses import dataclass


@dataclass
class GatewayConfig:
    """Storage gateway connection settings."""

    url: str = "http://localhost:8080"
    batch_size: int = 100
    flush_interval: float = 10.0
    max_buffer_size: int = 10000
    timeout: float = 10.0

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        return cls(
            url=os.getenv("GATEWAY_URL", "http://localhost:8080"),
            batch_size=int(os.getenv("GATEWAY_BATCH_SIZE", "100")),
            flush_interval=float(os.getenv("GATEWAY_FLUSH_INTERVAL", "10.0")),
            max_buffer_size=int(os.getenv("GATEWAY_MAX_BUFFER_SIZE", "10000")),
            timeout=float(os.getenv("GATEWAY_TIMEOUT", "10.0")),
        )
