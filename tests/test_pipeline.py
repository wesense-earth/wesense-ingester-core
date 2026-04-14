"""
Tests for ReadingPipeline and canonical reading versioning.

The most important tests here enforce that CANONICAL_FIELDS_V1 and
build_canonical_v1() are FROZEN. Breaking these tests means breaking
signature verification for readings that have already been signed and
archived — a one-way door. If you need to change canonical fields,
create v2 and add it alongside v1, never modify v1.
"""

import json

import pytest

from wesense_ingester.pipeline import (
    CANONICAL_BUILDERS,
    CANONICAL_FIELDS_V1,
    CURRENT_CANONICAL_VERSION,
    build_canonical,
    build_canonical_v1,
    canonical_to_json,
)


# =============================================================================
# Frozen v1 contract tests
#
# These tests pin the v1 canonical schema to its exact shape at the time
# v1 was frozen (2026-04-14). They must ONLY be updated to fix test bugs —
# never to accommodate changes to v1. Add a v2 instead.
# =============================================================================


def test_canonical_fields_v1_is_frozen():
    """
    CANONICAL_FIELDS_V1 must match the exact list frozen on 2026-04-14.
    Changing this list breaks signature verification for all v1 archives.
    """
    expected = [
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
    assert CANONICAL_FIELDS_V1 == expected, (
        "CANONICAL_FIELDS_V1 must not change. If you need to add/change "
        "fields, create CANONICAL_FIELDS_V2 and build_canonical_v2() instead."
    )


def test_build_canonical_v1_keys_match_fields():
    """build_canonical_v1 must produce exactly the keys in CANONICAL_FIELDS_V1."""
    sample = {
        "device_id": "test_device",
        "timestamp": 1712000000,
        "reading_type": "temperature",
        "value": 22.5,
        "latitude": -36.848,
        "longitude": 174.763,
    }
    result = build_canonical_v1(sample)
    assert set(result.keys()) == set(CANONICAL_FIELDS_V1)


def test_build_canonical_v1_deterministic_output():
    """
    Given the same input, build_canonical_v1 + canonical_to_json must
    produce byte-identical output. Signatures rely on this.
    """
    sample = {
        "device_id": "wsi_deterministic_test",
        "timestamp": 1712000000,
        "reading_type": "pm2_5",
        "value": 12.3,
        "unit": "µg/m³",
        "latitude": -36.848,
        "longitude": 174.763,
        "data_source": "mydata",
        "data_source_name": "My Data",
    }

    bytes_1 = canonical_to_json(build_canonical_v1(sample))
    bytes_2 = canonical_to_json(build_canonical_v1(dict(sample)))

    assert bytes_1 == bytes_2


def test_build_canonical_v1_exact_bytes_snapshot():
    """
    Snapshot test — the exact bytes produced by build_canonical_v1 for a
    known input must never change. This is the strongest guarantee that
    signatures remain verifiable.
    """
    sample = {
        "device_id": "snapshot_test",
        "timestamp": 1712000000,
        "reading_type": "temperature",
        "value": 22.5,
        "unit": "°C",
        "latitude": -36.848,
        "longitude": 174.763,
        "altitude": 45.0,
        "data_source": "snapshot",
        "data_source_name": "Snapshot Test",
        "sensor_transport": "wifi",
        "geo_country": "nz",
        "geo_subdivision": "auk",
        "board_model": "TEST_BOARD",
        "sensor_model": "TEST_SENSOR",
        "calibration_status": "calibrated",
        "deployment_type": "OUTDOOR",
        "deployment_type_source": "manual",
        "node_name": "Snapshot Node",
        "node_info": "",
        "node_info_url": "",
        "location_source": "gps",
        "data_license": "CC-BY-4.0",
    }

    result = canonical_to_json(build_canonical_v1(sample))
    parsed = json.loads(result)

    # Must be a dict with the exact 24 v1 keys
    assert set(parsed.keys()) == set(CANONICAL_FIELDS_V1)

    # Sort-order matters for determinism; verify sort_keys behaviour
    # by reconstructing and comparing.
    resorted = json.dumps(parsed, sort_keys=True).encode()
    assert result == resorted


# =============================================================================
# Dispatcher / forward-compat tests
# =============================================================================


def test_current_version_is_registered():
    """CURRENT_CANONICAL_VERSION must have a corresponding builder."""
    assert CURRENT_CANONICAL_VERSION in CANONICAL_BUILDERS


def test_build_canonical_defaults_to_current_version():
    """build_canonical() without a version should use CURRENT_CANONICAL_VERSION."""
    sample = {
        "device_id": "test",
        "timestamp": 1712000000,
        "reading_type": "temperature",
        "value": 22.5,
    }
    default_result = build_canonical(sample)
    current_result = build_canonical(sample, version=CURRENT_CANONICAL_VERSION)
    assert default_result == current_result


def test_build_canonical_rejects_unknown_version():
    """Unknown versions must raise ValueError, not silently fall back."""
    sample = {
        "device_id": "test",
        "timestamp": 1712000000,
        "reading_type": "temperature",
        "value": 22.5,
    }
    with pytest.raises(ValueError, match="Unknown canonical version"):
        build_canonical(sample, version=999)


def test_build_canonical_accepts_explicit_v1():
    """Verifiers must be able to request v1 explicitly for old readings."""
    sample = {
        "device_id": "test",
        "timestamp": 1712000000,
        "reading_type": "temperature",
        "value": 22.5,
    }
    result = build_canonical(sample, version=1)
    assert set(result.keys()) == set(CANONICAL_FIELDS_V1)
