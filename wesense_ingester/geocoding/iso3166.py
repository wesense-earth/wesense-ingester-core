"""
Authoritative ISO 3166 mapper for WeSense.

Merges the Meshtastic and WeSense LoRa iso3166_mapper modules into a single
authoritative source. All subdivision codes are WeSense's own invention
(GeoNames admin1 names mapped to short lowercase slugs), not an external
standard.

Divergence resolutions:
    UK code: "gb" (ISO standard, not "uk")
    NZ Waikato: "wko" (closer to ISO 3166-2:NZ-WKO, not "wai")
    US states: All 50 + DC (from Meshtastic version)
    NZ variants: "Auckland Region" etc. included (from WeSense version)
    Case matching: Case-insensitive fallback (from WeSense version)
"""

# ISO 3166-1 alpha-2 country codes (lowercase)
COUNTRY_NAME_TO_ISO: dict[str, str] = {
    "New Zealand": "nz",
    "Australia": "au",
    "United States": "us",
    "United Kingdom": "gb",
    "Canada": "ca",
    "Germany": "de",
    "France": "fr",
    "Japan": "jp",
    "China": "cn",
    "Brazil": "br",
    "Mexico": "mx",
    "South Africa": "za",
    "India": "in",
    "Russia": "ru",
    "Singapore": "sg",
    "Malaysia": "my",
    "Taiwan": "tw",
    "Poland": "pl",
    "Czech Republic": "cz",
    "Czechia": "cz",
    "Ukraine": "ua",
    "Argentina": "ar",
    "Belarus": "by",
    "Netherlands": "nl",
    "Spain": "es",
    "Italy": "it",
    "Sweden": "se",
    "Norway": "no",
    "Denmark": "dk",
    "Finland": "fi",
    "Switzerland": "ch",
    "Austria": "at",
    "Belgium": "be",
    "Ireland": "ie",
    "Portugal": "pt",
    "Greece": "gr",
    "Thailand": "th",
    "Indonesia": "id",
    "Philippines": "ph",
    "Vietnam": "vn",
    "South Korea": "kr",
    "Hong Kong": "hk",
    "United Arab Emirates": "ae",
    "Saudi Arabia": "sa",
    "Israel": "il",
    "Egypt": "eg",
    "Chile": "cl",
    "Colombia": "co",
    "Peru": "pe",
}

# Pre-built case-insensitive lookup for country names
_COUNTRY_NAME_LOWER: dict[str, str] = {k.lower(): v for k, v in COUNTRY_NAME_TO_ISO.items()}

# ISO 3166-2 subdivision codes (lowercase, without country prefix)
# Key: (country_code, state/region name)
# Value: subdivision code
SUBDIVISION_NAME_TO_ISO: dict[tuple[str, str], str] = {
    # =========================================================================
    # New Zealand — includes "Region"/"District" variant names from WeSense
    # Waikato uses "wko" (closer to ISO 3166-2:NZ-WKO)
    # =========================================================================
    ("nz", "Auckland"): "auk",
    ("nz", "Auckland Region"): "auk",
    ("nz", "Bay of Plenty"): "bop",
    ("nz", "Bay of Plenty Region"): "bop",
    ("nz", "Canterbury"): "can",
    ("nz", "Canterbury Region"): "can",
    ("nz", "Gisborne"): "gis",
    ("nz", "Gisborne District"): "gis",
    ("nz", "Hawke's Bay"): "hkb",
    ("nz", "Hawke's Bay Region"): "hkb",
    ("nz", "Manawatu-Wanganui"): "mwt",
    ("nz", "Manawatū-Whanganui"): "mwt",
    ("nz", "Marlborough"): "mbh",
    ("nz", "Marlborough Region"): "mbh",
    ("nz", "Nelson"): "nsn",
    ("nz", "Nelson Region"): "nsn",
    ("nz", "Northland"): "ntl",
    ("nz", "Northland Region"): "ntl",
    ("nz", "Otago"): "ota",
    ("nz", "Otago Region"): "ota",
    ("nz", "Southland"): "stl",
    ("nz", "Southland Region"): "stl",
    ("nz", "Taranaki"): "tki",
    ("nz", "Taranaki Region"): "tki",
    ("nz", "Tasman"): "tas",
    ("nz", "Tasman District"): "tas",
    ("nz", "Waikato"): "wko",
    ("nz", "Waikato Region"): "wko",
    ("nz", "Wellington"): "wgn",
    ("nz", "Wellington Region"): "wgn",
    ("nz", "West Coast"): "wtc",
    ("nz", "West Coast Region"): "wtc",

    # =========================================================================
    # Australia
    # =========================================================================
    ("au", "New South Wales"): "nsw",
    ("au", "Queensland"): "qld",
    ("au", "Victoria"): "vic",
    ("au", "Western Australia"): "wa",
    ("au", "South Australia"): "sa",
    ("au", "Tasmania"): "tas",
    ("au", "Northern Territory"): "nt",
    ("au", "Australian Capital Territory"): "act",

    # =========================================================================
    # United States — all 50 states + DC (from Meshtastic version)
    # =========================================================================
    ("us", "Alabama"): "al",
    ("us", "Alaska"): "ak",
    ("us", "Arizona"): "az",
    ("us", "Arkansas"): "ar",
    ("us", "California"): "ca",
    ("us", "Colorado"): "co",
    ("us", "Connecticut"): "ct",
    ("us", "Delaware"): "de",
    ("us", "Florida"): "fl",
    ("us", "Georgia"): "ga",
    ("us", "Hawaii"): "hi",
    ("us", "Idaho"): "id",
    ("us", "Illinois"): "il",
    ("us", "Indiana"): "in",
    ("us", "Iowa"): "ia",
    ("us", "Kansas"): "ks",
    ("us", "Kentucky"): "ky",
    ("us", "Louisiana"): "la",
    ("us", "Maine"): "me",
    ("us", "Maryland"): "md",
    ("us", "Massachusetts"): "ma",
    ("us", "Michigan"): "mi",
    ("us", "Minnesota"): "mn",
    ("us", "Mississippi"): "ms",
    ("us", "Missouri"): "mo",
    ("us", "Montana"): "mt",
    ("us", "Nebraska"): "ne",
    ("us", "Nevada"): "nv",
    ("us", "New Hampshire"): "nh",
    ("us", "New Jersey"): "nj",
    ("us", "New Mexico"): "nm",
    ("us", "New York"): "ny",
    ("us", "North Carolina"): "nc",
    ("us", "North Dakota"): "nd",
    ("us", "Ohio"): "oh",
    ("us", "Oklahoma"): "ok",
    ("us", "Oregon"): "or",
    ("us", "Pennsylvania"): "pa",
    ("us", "Rhode Island"): "ri",
    ("us", "South Carolina"): "sc",
    ("us", "South Dakota"): "sd",
    ("us", "Tennessee"): "tn",
    ("us", "Texas"): "tx",
    ("us", "Utah"): "ut",
    ("us", "Vermont"): "vt",
    ("us", "Virginia"): "va",
    ("us", "Washington"): "wa",
    ("us", "West Virginia"): "wv",
    ("us", "Wisconsin"): "wi",
    ("us", "Wyoming"): "wy",
    ("us", "District of Columbia"): "dc",

    # =========================================================================
    # United Kingdom — uses "gb" country code (ISO standard)
    # =========================================================================
    ("gb", "England"): "eng",
    ("gb", "Scotland"): "sct",
    ("gb", "Wales"): "wls",
    ("gb", "Northern Ireland"): "nir",

    # =========================================================================
    # Canada
    # =========================================================================
    ("ca", "Ontario"): "on",
    ("ca", "Quebec"): "qc",
    ("ca", "British Columbia"): "bc",
    ("ca", "Alberta"): "ab",
    ("ca", "Manitoba"): "mb",
    ("ca", "Saskatchewan"): "sk",
    ("ca", "Nova Scotia"): "ns",
    ("ca", "New Brunswick"): "nb",
    ("ca", "Newfoundland and Labrador"): "nl",
    ("ca", "Prince Edward Island"): "pe",
    ("ca", "Northwest Territories"): "nt",
    ("ca", "Yukon"): "yt",
    ("ca", "Nunavut"): "nu",

    # =========================================================================
    # Germany (common states)
    # =========================================================================
    ("de", "Bavaria"): "by",
    ("de", "Berlin"): "be",
    ("de", "Hamburg"): "hh",
    ("de", "Hesse"): "he",
    ("de", "North Rhine-Westphalia"): "nw",
    ("de", "Saxony"): "sn",
}

# Pre-built case-insensitive lookup for subdivision names
_SUBDIVISION_LOWER: dict[tuple[str, str], str] = {
    (cc, sn.lower()): code for (cc, sn), code in SUBDIVISION_NAME_TO_ISO.items()
}


def get_country_code(country_name: str) -> str:
    """
    Convert country name to ISO 3166-1 alpha-2 code (lowercase).

    Uses case-insensitive matching. Returns "unknown" if not found.
    """
    if not country_name:
        return "unknown"

    # Try exact match first
    code = COUNTRY_NAME_TO_ISO.get(country_name)
    if code is not None:
        return code

    # Case-insensitive fallback
    return _COUNTRY_NAME_LOWER.get(country_name.lower(), "unknown")


def get_subdivision_code(country_code: str, state_name: str) -> str:
    """
    Convert state/region name to subdivision code.

    Uses case-insensitive matching. Returns "unknown" if not found.
    """
    if not state_name or not country_code:
        return "unknown"

    cc = country_code.lower()

    # Try exact match first
    code = SUBDIVISION_NAME_TO_ISO.get((cc, state_name))
    if code is not None:
        return code

    # Case-insensitive fallback
    return _SUBDIVISION_LOWER.get((cc, state_name.lower()), "unknown")


def get_iso_codes(country_name: str, state_name: str) -> tuple[str, str]:
    """
    Get both country and subdivision ISO codes from names.

    Returns (country_code, subdivision_code).
    """
    country_code = get_country_code(country_name)
    subdivision_code = get_subdivision_code(country_code, state_name)
    return (country_code, subdivision_code)
