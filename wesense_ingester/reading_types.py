"""
Standard WeSense reading type registry.

Maps canonical `reading_type` strings to human-readable display names and
expected units. Used by the pipeline to auto-fill `reading_type_name` when
an ingester doesn't set it explicitly.

Adding a new reading type: add an entry here, then deploy. Ingesters that
produce the new type automatically get the display name; Respiro and other
consumers can read it from the database without code changes.
"""

# reading_type → (reading_type_name, expected_unit)
READING_TYPES = {
    # Common environmental
    "temperature": ("Temperature", "°C"),
    "humidity": ("Humidity", "%"),
    "pressure": ("Pressure", "hPa"),
    "co2": ("CO₂", "ppm"),

    # Particulate matter (mass concentration)
    "pm1_0": ("PM1.0", "µg/m³"),
    "pm2_5": ("PM2.5", "µg/m³"),
    "pm10": ("PM10", "µg/m³"),

    # Particle counts (per 0.1L)
    "particles_0_3um": ("Particles (>0.3µm)", "count/0.1L"),
    "particles_0_5um": ("Particles (>0.5µm)", "count/0.1L"),
    "particles_1_0um": ("Particles (>1.0µm)", "count/0.1L"),
    "particles_2_5um": ("Particles (>2.5µm)", "count/0.1L"),
    "particles_5_0um": ("Particles (>5.0µm)", "count/0.1L"),
    "particles_10um": ("Particles (>10µm)", "count/0.1L"),

    # Air quality indices
    "voc_index": ("VOC Index", "index"),
    "nox_index": ("NOx Index", "index"),
    "voc_raw": ("VOC Raw", "Ω"),
    "nox_raw": ("NOx Raw", "Ω"),

    # Light & weather
    "light_level": ("Light Level", "lux"),
    "wind_speed": ("Wind Speed", "m/s"),
    "wind_direction": ("Wind Direction", "°"),
    "wind_gust": ("Wind Gust", "m/s"),
    "wind_gust_direction": ("Wind Gust Direction", "°"),
    "rainfall": ("Rainfall", "mm"),

    # Gases
    "no": ("NO", "µg/m³"),
    "no2": ("NO₂", "µg/m³"),
    "so2": ("SO₂", "µg/m³"),
    "o3": ("O₃", "µg/m³"),
    "co": ("CO", "mg/m³"),
}


def get_display_name(reading_type: str) -> str:
    """
    Get the human-readable display name for a reading_type.

    Returns an empty string if the reading_type is unknown — consumers
    can fall back to humanising the reading_type string themselves.
    """
    entry = READING_TYPES.get(reading_type)
    return entry[0] if entry else ""


def get_expected_unit(reading_type: str) -> str:
    """
    Get the expected unit for a reading_type.

    Returns an empty string if the reading_type is unknown.
    """
    entry = READING_TYPES.get(reading_type)
    return entry[1] if entry else ""
