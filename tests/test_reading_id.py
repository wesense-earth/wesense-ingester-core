"""Tests for content-based reading ID generation."""

from wesense_ingester.ids.reading_id import generate_reading_id


def test_deterministic():
    """Same inputs always produce the same ID."""
    id1 = generate_reading_id("sensor-001", 1704067200, "temperature", 22.5)
    id2 = generate_reading_id("sensor-001", 1704067200, "temperature", 22.5)
    assert id1 == id2


def test_different_device_different_id():
    id1 = generate_reading_id("sensor-001", 1704067200, "temperature", 22.5)
    id2 = generate_reading_id("sensor-002", 1704067200, "temperature", 22.5)
    assert id1 != id2


def test_different_timestamp_different_id():
    id1 = generate_reading_id("sensor-001", 1704067200, "temperature", 22.5)
    id2 = generate_reading_id("sensor-001", 1704067201, "temperature", 22.5)
    assert id1 != id2


def test_different_type_different_id():
    id1 = generate_reading_id("sensor-001", 1704067200, "temperature", 22.5)
    id2 = generate_reading_id("sensor-001", 1704067200, "humidity", 22.5)
    assert id1 != id2


def test_different_value_different_id():
    id1 = generate_reading_id("sensor-001", 1704067200, "temperature", 22.5)
    id2 = generate_reading_id("sensor-001", 1704067200, "temperature", 23.0)
    assert id1 != id2


def test_format_hex_32_chars():
    """ID is a 32-character hex string."""
    result = generate_reading_id("sensor-001", 1704067200, "temperature", 22.5)
    assert len(result) == 32
    assert all(c in "0123456789abcdef" for c in result)


def test_meshtastic_device_id():
    """Works with Meshtastic-style node IDs."""
    result = generate_reading_id("meshtastic_e4cc140c", 1704067200, "temperature", 22.5)
    assert len(result) == 32
