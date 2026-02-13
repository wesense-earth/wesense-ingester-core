"""Tests for ZenohConfig — pure Python, no zenoh import needed."""

import json
import os
from unittest.mock import patch

from wesense_ingester.zenoh.config import ZenohConfig


def test_default_values():
    config = ZenohConfig()
    assert config.mode == "client"
    assert config.routers == []
    assert config.listen == []
    assert config.enabled is True
    assert config.key_prefix == "wesense/v2/live"


def test_from_env_reads_all_vars():
    env = {
        "ZENOH_MODE": "peer",
        "ZENOH_ROUTERS": "tcp/router1:7447",
        "ZENOH_LISTEN": "tcp/0.0.0.0:7447",
        "ZENOH_ENABLED": "false",
        "ZENOH_KEY_PREFIX": "custom/prefix",
    }
    with patch.dict(os.environ, env, clear=False):
        config = ZenohConfig.from_env()

    assert config.mode == "peer"
    assert config.routers == ["tcp/router1:7447"]
    assert config.listen == ["tcp/0.0.0.0:7447"]
    assert config.enabled is False
    assert config.key_prefix == "custom/prefix"


def test_from_env_parses_comma_separated_routers():
    env = {"ZENOH_ROUTERS": "tcp/a:7447,tcp/b:7447"}
    with patch.dict(os.environ, env, clear=False):
        config = ZenohConfig.from_env()

    assert config.routers == ["tcp/a:7447", "tcp/b:7447"]


def test_from_env_empty_routers():
    env = {"ZENOH_ROUTERS": ""}
    with patch.dict(os.environ, env, clear=False):
        config = ZenohConfig.from_env()

    assert config.routers == []


def test_from_env_enabled_parsing_true():
    for val in ("true", "1", "yes", "True", "YES"):
        env = {"ZENOH_ENABLED": val}
        with patch.dict(os.environ, env, clear=False):
            config = ZenohConfig.from_env()
        assert config.enabled is True, f"Expected True for {val!r}"


def test_from_env_enabled_parsing_false():
    for val in ("false", "0", "no", "False", "NO"):
        env = {"ZENOH_ENABLED": val}
        with patch.dict(os.environ, env, clear=False):
            config = ZenohConfig.from_env()
        assert config.enabled is False, f"Expected False for {val!r}"


def test_to_zenoh_json_client_mode():
    config = ZenohConfig(
        mode="client",
        routers=["tcp/router1:7447", "tcp/router2:7447"],
    )
    result = json.loads(config.to_zenoh_json())

    assert result["mode"] == "client"
    assert result["connect"]["endpoints"] == ["tcp/router1:7447", "tcp/router2:7447"]
    assert "scouting" not in result


def test_to_zenoh_json_peer_mode_enables_scouting():
    config = ZenohConfig(mode="peer")
    result = json.loads(config.to_zenoh_json())

    assert result["mode"] == "peer"
    assert result["scouting"]["multicast"]["enabled"] is True
    assert result["scouting"]["gossip"]["enabled"] is True


def test_to_zenoh_json_no_routers():
    config = ZenohConfig(mode="client", routers=[])
    result = json.loads(config.to_zenoh_json())

    assert "connect" not in result


def test_to_zenoh_json_with_listen():
    config = ZenohConfig(mode="router", listen=["tcp/0.0.0.0:7447"])
    result = json.loads(config.to_zenoh_json())

    assert result["listen"]["endpoints"] == ["tcp/0.0.0.0:7447"]


def test_build_key_expr():
    config = ZenohConfig(key_prefix="wesense/v2/live")
    key = config.build_key_expr("nz", "auk", "sensor-001")
    assert key == "wesense/v2/live/nz/auk/sensor-001"


def test_build_key_expr_defaults_to_unknown():
    config = ZenohConfig(key_prefix="wesense/v2/live")
    key = config.build_key_expr(None, None, None)
    assert key == "wesense/v2/live/unknown/unknown/unknown"


def test_build_key_expr_custom_prefix():
    config = ZenohConfig(key_prefix="test/data")
    key = config.build_key_expr("gb", "lon", "dev-42")
    assert key == "test/data/gb/lon/dev-42"


def test_build_key_expr_lowercases():
    config = ZenohConfig(key_prefix="wesense/v2/live")
    key = config.build_key_expr("NZ", "AUK", "sensor-001")
    assert key == "wesense/v2/live/nz/auk/sensor-001"
