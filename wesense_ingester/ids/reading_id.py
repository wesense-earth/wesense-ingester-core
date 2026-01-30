"""
Content-based reading ID generation.

Produces a deterministic SHA-256 identifier for each sensor reading based on
its content (device, timestamp, type, value). The same physical measurement
always produces the same ID regardless of which node received it or when.

See: wesense-clickhouse-ipfs/ARCHITECTURE.md
"""

import hashlib


def generate_reading_id(
    device_id: str,
    sensor_timestamp: int,
    reading_type: str,
    value: float,
) -> str:
    """
    Generate a deterministic reading ID based on content.

    Args:
        device_id: Unique identifier for the sensor.
        sensor_timestamp: Unix timestamp from the SENSOR (not receive time).
        reading_type: Type of measurement (e.g. "temperature", "humidity").
        value: The measurement value.

    Returns:
        SHA-256 hex digest truncated to 32 characters.
    """
    content = f"{device_id}|{sensor_timestamp}|{reading_type}|{value}"
    return hashlib.sha256(content.encode()).hexdigest()[:32]
