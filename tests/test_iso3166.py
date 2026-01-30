"""Tests for ISO 3166 country/subdivision mapper."""

from wesense_ingester.geocoding.iso3166 import (
    get_country_code,
    get_iso_codes,
    get_subdivision_code,
    SUBDIVISION_NAME_TO_ISO,
)


# --- Country codes ---

def test_uk_uses_gb():
    """United Kingdom uses ISO standard 'gb', not 'uk'."""
    assert get_country_code("United Kingdom") == "gb"


def test_new_zealand():
    assert get_country_code("New Zealand") == "nz"


def test_unknown_country():
    assert get_country_code("Narnia") == "unknown"


def test_empty_country():
    assert get_country_code("") == "unknown"


def test_country_case_insensitive():
    assert get_country_code("new zealand") == "nz"
    assert get_country_code("NEW ZEALAND") == "nz"
    assert get_country_code("united kingdom") == "gb"


def test_czechia_and_czech_republic():
    """Both names map to the same code."""
    assert get_country_code("Czechia") == "cz"
    assert get_country_code("Czech Republic") == "cz"


# --- Subdivision codes ---

def test_waikato_uses_wko():
    """Waikato uses 'wko' (closer to ISO 3166-2:NZ-WKO), not 'wai'."""
    assert get_subdivision_code("nz", "Waikato") == "wko"


def test_waikato_region_variant():
    """'Waikato Region' variant also maps correctly."""
    assert get_subdivision_code("nz", "Waikato Region") == "wko"


def test_auckland():
    assert get_subdivision_code("nz", "Auckland") == "auk"


def test_auckland_region_variant():
    assert get_subdivision_code("nz", "Auckland Region") == "auk"


def test_all_us_states():
    """All 50 US states + DC should be present."""
    us_subdivisions = {
        (cc, name) for (cc, name) in SUBDIVISION_NAME_TO_ISO if cc == "us"
    }
    us_codes = {SUBDIVISION_NAME_TO_ISO[k] for k in us_subdivisions}
    # 50 states + DC = 51 unique codes
    assert len(us_codes) == 51


def test_subdivision_case_insensitive():
    assert get_subdivision_code("nz", "auckland") == "auk"
    assert get_subdivision_code("nz", "AUCKLAND") == "auk"
    assert get_subdivision_code("us", "california") == "ca"


def test_subdivision_unknown():
    assert get_subdivision_code("nz", "Atlantis") == "unknown"


def test_subdivision_empty():
    assert get_subdivision_code("nz", "") == "unknown"
    assert get_subdivision_code("", "Auckland") == "unknown"


def test_nz_manawatu_variants():
    """Both spelling variants of Manawatu-Wanganui work."""
    assert get_subdivision_code("nz", "Manawatu-Wanganui") == "mwt"
    assert get_subdivision_code("nz", "Manawatū-Whanganui") == "mwt"


def test_gb_subdivisions():
    assert get_subdivision_code("gb", "England") == "eng"
    assert get_subdivision_code("gb", "Scotland") == "sct"
    assert get_subdivision_code("gb", "Wales") == "wls"
    assert get_subdivision_code("gb", "Northern Ireland") == "nir"


# --- Combined lookup ---

def test_get_iso_codes():
    country, subdivision = get_iso_codes("New Zealand", "Auckland")
    assert country == "nz"
    assert subdivision == "auk"


def test_get_iso_codes_unknown():
    country, subdivision = get_iso_codes("Narnia", "FakeRegion")
    assert country == "unknown"
    assert subdivision == "unknown"
