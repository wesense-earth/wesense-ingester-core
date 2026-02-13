"""
Zenoh session configuration.

Pure Python — no zenoh import needed. Follows the MQTTPublisherConfig /
ClickHouseConfig pattern: @dataclass with from_env() class method.
"""

import json
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ZenohConfig:
    """Zenoh session configuration."""

    mode: str = "client"
    routers: list[str] = field(default_factory=list)
    listen: list[str] = field(default_factory=list)
    enabled: bool = True
    key_prefix: str = "wesense/v2/live"

    @classmethod
    def from_env(cls) -> "ZenohConfig":
        """Create config from environment variables."""
        routers_str = os.getenv("ZENOH_ROUTERS", "")
        listen_str = os.getenv("ZENOH_LISTEN", "")
        return cls(
            mode=os.getenv("ZENOH_MODE", "client"),
            routers=[r.strip() for r in routers_str.split(",") if r.strip()],
            listen=[l.strip() for l in listen_str.split(",") if l.strip()],
            enabled=os.getenv("ZENOH_ENABLED", "true").lower() in ("true", "1", "yes"),
            key_prefix=os.getenv("ZENOH_KEY_PREFIX", "wesense/v2/live"),
        )

    def to_zenoh_json(self) -> str:
        """Build JSON config string for zenoh.Config.from_json5()."""
        config: dict = {"mode": self.mode}

        if self.routers:
            config["connect"] = {"endpoints": self.routers}

        if self.listen:
            config["listen"] = {"endpoints": self.listen}

        if self.mode == "peer":
            config["scouting"] = {
                "multicast": {"enabled": True},
                "gossip": {"enabled": True},
            }

        return json.dumps(config)

    def build_key_expr(
        self,
        country: Optional[str] = None,
        subdivision: Optional[str] = None,
        device_id: Optional[str] = None,
    ) -> str:
        """Build key expression: {prefix}/{country}/{subdivision}/{device_id}.

        Mirrors the MQTT topic pattern. None fields default to "unknown".
        """
        return "/".join([
            self.key_prefix,
            (country or "unknown").lower(),
            (subdivision or "unknown").lower(),
            device_id or "unknown",
        ])
