"""
Reverse geocoder using GeoNames database (offline only).

Provides country/subdivision lookup from coordinates without any network
dependency. Uses an LRU cache on rounded coordinates (~100m precision)
to avoid redundant lookups.
"""

import functools
import logging
from typing import Any, Optional

from wesense_ingester.geocoding.iso3166 import get_country_code, get_subdivision_code

logger = logging.getLogger(__name__)

try:
    import reverse_geocoder as rg
    _RG_AVAILABLE = True
except ImportError:
    _RG_AVAILABLE = False


class ReverseGeocoder:
    """
    GeoNames-only reverse geocoder with LRU cache.

    Coordinates are rounded to 3 decimal places (~100m) for cache efficiency.
    """

    def __init__(self, cache_size: int = 4096):
        """
        Args:
            cache_size: Maximum number of cached coordinate lookups.

        Raises:
            ImportError: If reverse_geocoder library is not installed.
        """
        if not _RG_AVAILABLE:
            raise ImportError(
                "reverse_geocoder library not available. "
                "Install with: pip install reverse_geocoder==1.5.1"
            )

        self._lookup = functools.lru_cache(maxsize=cache_size)(self._raw_lookup)
        logger.info("ReverseGeocoder initialized (cache_size=%d)", cache_size)

    def reverse_geocode(
        self, latitude: float, longitude: float
    ) -> Optional[dict[str, Any]]:
        """
        Reverse geocode coordinates to location information.

        Args:
            latitude: Latitude in decimal degrees (-90 to 90).
            longitude: Longitude in decimal degrees (-180 to 180).

        Returns:
            Dict with keys: city, admin1, country, country_code,
            geo_country (ISO lowercase), geo_subdivision (ISO code).
            Returns None if coordinates are invalid or lookup fails.
        """
        if latitude is None or longitude is None:
            return None

        if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
            logger.warning("Invalid coordinates: %s, %s", latitude, longitude)
            return None

        # Round for cache efficiency (~100m precision)
        rounded_lat = round(latitude, 3)
        rounded_lon = round(longitude, 3)

        return self._lookup(rounded_lat, rounded_lon)

    @staticmethod
    def _raw_lookup(lat: float, lon: float) -> Optional[dict[str, Any]]:
        """Perform the actual GeoNames lookup (cached by lru_cache)."""
        try:
            results = rg.search([(lat, lon)], mode=1)
            if not results:
                logger.warning("No GeoNames result for %s, %s", lat, lon)
                return None

            result = results[0]
            cc_upper = result.get("cc", "")
            admin1 = result.get("admin1")

            country_code = cc_upper.lower() if cc_upper else "unknown"
            subdivision_code = get_subdivision_code(country_code, admin1) if admin1 else "unknown"

            return {
                "city": result.get("name"),
                "admin1": admin1,
                "country_code": cc_upper,
                "geo_country": country_code,
                "geo_subdivision": subdivision_code,
            }
        except Exception as e:
            logger.error("Geocoding error for %s, %s: %s", lat, lon, e)
            return None

    @staticmethod
    def format_subdivision_code(admin1: Optional[str]) -> str:
        """
        Format an admin1 name as a URL-safe subdivision code slug.

        Examples:
            "Auckland" -> "auckland"
            "New South Wales" -> "new-south-wales"
            "Hawke's Bay" -> "hawkes-bay"
        """
        if not admin1:
            return "unknown"
        return admin1.lower().replace(" ", "-").replace("'", "")

    def cache_info(self) -> dict[str, int]:
        """Return LRU cache hit/miss statistics."""
        info = self._lookup.cache_info()
        return {
            "hits": info.hits,
            "misses": info.misses,
            "size": info.currsize,
            "maxsize": info.maxsize,
        }
