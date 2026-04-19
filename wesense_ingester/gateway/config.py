"""Configuration for the storage gateway client."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class GatewayConfig:
    """Storage gateway connection settings."""

    url: str = "http://localhost:8080"
    batch_size: int = 100
    flush_interval: float = 10.0
    max_buffer_size: int = 10000
    timeout: float = 10.0
    tls_enabled: bool = False
    tls_ca_certfile: Optional[str] = None

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        tls_enabled = os.getenv("TLS_ENABLED", "").lower() == "true"
        url = os.getenv("GATEWAY_URL", "http://localhost:8080")
        if tls_enabled:
            url = url.replace("http://", "https://")
        return cls(
            url=url,
            batch_size=int(os.getenv("GATEWAY_BATCH_SIZE", "100")),
            flush_interval=float(os.getenv("GATEWAY_FLUSH_INTERVAL", "10.0")),
            max_buffer_size=int(os.getenv("GATEWAY_MAX_BUFFER_SIZE", "10000")),
            timeout=float(os.getenv("GATEWAY_TIMEOUT", "10.0")),
            tls_enabled=tls_enabled,
            tls_ca_certfile=os.getenv("TLS_CA_CERTFILE"),
        )
