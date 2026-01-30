"""Tests for reverse geocoder."""

from unittest.mock import MagicMock, patch

import pytest


def test_invalid_latitude():
    with patch.dict("sys.modules", {"reverse_geocoder": MagicMock()}):
        from wesense_ingester.geocoding.geocoder import ReverseGeocoder
        geocoder = ReverseGeocoder()

        assert geocoder.reverse_geocode(91.0, 174.0) is None
        assert geocoder.reverse_geocode(-91.0, 174.0) is None


def test_invalid_longitude():
    with patch.dict("sys.modules", {"reverse_geocoder": MagicMock()}):
        from wesense_ingester.geocoding.geocoder import ReverseGeocoder
        geocoder = ReverseGeocoder()

        assert geocoder.reverse_geocode(-36.0, 181.0) is None
        assert geocoder.reverse_geocode(-36.0, -181.0) is None


def test_none_coordinates():
    with patch.dict("sys.modules", {"reverse_geocoder": MagicMock()}):
        from wesense_ingester.geocoding.geocoder import ReverseGeocoder
        geocoder = ReverseGeocoder()

        assert geocoder.reverse_geocode(None, 174.0) is None
        assert geocoder.reverse_geocode(-36.0, None) is None


def test_format_subdivision_code():
    with patch.dict("sys.modules", {"reverse_geocoder": MagicMock()}):
        from wesense_ingester.geocoding.geocoder import ReverseGeocoder

        assert ReverseGeocoder.format_subdivision_code("Auckland") == "auckland"
        assert ReverseGeocoder.format_subdivision_code("New South Wales") == "new-south-wales"
        assert ReverseGeocoder.format_subdivision_code("Hawke's Bay") == "hawkes-bay"
        assert ReverseGeocoder.format_subdivision_code(None) == "unknown"
        assert ReverseGeocoder.format_subdivision_code("") == "unknown"


def test_successful_geocode():
    mock_rg = MagicMock()
    mock_rg.search.return_value = [
        {"name": "Auckland", "admin1": "Auckland", "cc": "NZ"}
    ]

    with patch.dict("sys.modules", {"reverse_geocoder": mock_rg}):
        # Need to reimport to pick up the mock
        import importlib
        import wesense_ingester.geocoding.geocoder as geocoder_mod
        importlib.reload(geocoder_mod)

        geocoder = geocoder_mod.ReverseGeocoder()
        result = geocoder.reverse_geocode(-36.848, 174.763)

        assert result is not None
        assert result["city"] == "Auckland"
        assert result["admin1"] == "Auckland"
        assert result["geo_country"] == "nz"
        assert result["geo_subdivision"] == "auk"


def test_cache_info():
    mock_rg = MagicMock()
    mock_rg.search.return_value = [
        {"name": "Auckland", "admin1": "Auckland", "cc": "NZ"}
    ]

    with patch.dict("sys.modules", {"reverse_geocoder": mock_rg}):
        import importlib
        import wesense_ingester.geocoding.geocoder as geocoder_mod
        importlib.reload(geocoder_mod)

        geocoder = geocoder_mod.ReverseGeocoder()
        geocoder.reverse_geocode(-36.848, 174.763)
        geocoder.reverse_geocode(-36.848, 174.763)  # Should hit cache

        info = geocoder.cache_info()
        assert info["hits"] == 1
        assert info["misses"] == 1
