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
    ("us", "Washington, D.C."): "dc",

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
    # Germany
    # =========================================================================
    ("de", "Bavaria"): "by",
    ("de", "Berlin"): "be",
    ("de", "Hamburg"): "hh",
    ("de", "Hesse"): "he",
    ("de", "North Rhine-Westphalia"): "nw",
    ("de", "Saxony"): "sn",
    ("de", "Baden-Wuerttemberg"): "bw",
    ("de", "Lower Saxony"): "ni",
    ("de", "Rheinland-Pfalz"): "rp",
    ("de", "Saxony-Anhalt"): "st",
    ("de", "Schleswig-Holstein"): "sh",
    ("de", "Brandenburg"): "bb",
    ("de", "Thuringia"): "th",
    ("de", "Bremen"): "hb",
    ("de", "Mecklenburg-Vorpommern"): "mv",
    ("de", "Saarland"): "sl",

    # =========================================================================
    # Poland
    # =========================================================================
    ("pl", "Lesser Poland Voivodeship"): "ma",
    ("pl", "Swietokrzyskie"): "sk",
    ("pl", "Lower Silesian Voivodeship"): "ds",
    ("pl", "Greater Poland Voivodeship"): "wp",
    ("pl", "Pomeranian Voivodeship"): "pm",
    ("pl", "Silesian Voivodeship"): "sl",
    ("pl", "Masovian Voivodeship"): "mz",
    ("pl", "Lubusz"): "lb",
    ("pl", "Subcarpathian Voivodeship"): "pk",
    ("pl", "Lublin Voivodeship"): "lu",
    ("pl", "West Pomeranian Voivodeship"): "zp",
    ("pl", "Lodz Voivodeship"): "ld",
    ("pl", "Opole Voivodeship"): "op",
    ("pl", "Kuyavian-Pomeranian Voivodeship"): "kp",
    ("pl", "Podlaskie"): "pd",
    ("pl", "Warmian-Masurian Voivodeship"): "wn",

    # =========================================================================
    # Netherlands
    # =========================================================================
    ("nl", "Groningen"): "gr",
    ("nl", "South Holland"): "zh",
    ("nl", "North Brabant"): "nb",
    ("nl", "Utrecht"): "ut",
    ("nl", "Gelderland"): "ge",
    ("nl", "North Holland"): "nh",
    ("nl", "Overijssel"): "ov",
    ("nl", "Limburg"): "li",
    ("nl", "Friesland"): "fr",
    ("nl", "Flevoland"): "fl",
    ("nl", "Zeeland"): "ze",
    ("nl", "Drenthe"): "dr",

    # =========================================================================
    # Russia
    # =========================================================================
    ("ru", "Orenburg"): "ore",
    ("ru", "Chelyabinsk"): "che",
    ("ru", "Novosibirsk"): "nvs",
    ("ru", "Sverdlovsk"): "svr",
    ("ru", "Bashkortostan"): "ba",
    ("ru", "Smolensk"): "smo",
    ("ru", "St.-Petersburg"): "spb",
    ("ru", "Rjazan"): "rya",
    ("ru", "Nizjnij Novgorod"): "nnv",
    ("ru", "Leningrad"): "len",
    ("ru", "Moscow"): "mow",
    ("ru", "Moskovskaya"): "mos",
    ("ru", "Penza"): "pnz",
    ("ru", "Jaroslavl"): "yar",
    ("ru", "Krasnodarskiy"): "kda",
    ("ru", "Tatarstan"): "ta",
    ("ru", "Saratov"): "sar",
    ("ru", "Vladimir"): "vla",
    ("ru", "Rostov"): "ros",

    # =========================================================================
    # Switzerland
    # =========================================================================
    ("ch", "Geneva"): "ge",
    ("ch", "Vaud"): "vd",
    ("ch", "Zug"): "zg",
    ("ch", "Schwyz"): "sz",
    ("ch", "Bern"): "be",
    ("ch", "Zurich"): "zh",
    ("ch", "Fribourg"): "fr",
    ("ch", "Appenzell Ausserrhoden"): "ar",
    ("ch", "Neuchatel"): "ne",
    ("ch", "Lucerne"): "lu",
    ("ch", "Aargau"): "ag",
    ("ch", "Basel-Stadt"): "bs",
    ("ch", "Basel-Landschaft"): "bl",
    ("ch", "Graubuenden"): "gr",
    ("ch", "St. Gallen"): "sg",
    ("ch", "Thurgau"): "tg",
    ("ch", "Ticino"): "ti",
    ("ch", "Valais"): "vs",
    ("ch", "Solothurn"): "so",
    ("ch", "Schaffhausen"): "sh",
    ("ch", "Uri"): "ur",
    ("ch", "Obwalden"): "ow",
    ("ch", "Nidwalden"): "nw",
    ("ch", "Glarus"): "gl",
    ("ch", "Jura"): "ju",
    ("ch", "Appenzell Innerrhoden"): "ai",

    # =========================================================================
    # Czech Republic
    # =========================================================================
    ("cz", "South Moravian"): "jm",
    ("cz", "Olomoucky"): "ol",
    ("cz", "Praha"): "pr",
    ("cz", "Kralovehradecky"): "kr",
    ("cz", "Central Bohemia"): "st",
    ("cz", "Moravskoslezsky"): "mo",
    ("cz", "Zlinsky"): "zl",
    ("cz", "Vysocina"): "vy",
    ("cz", "Pardubicky"): "pa",
    ("cz", "Plzensky"): "pl",
    ("cz", "Jihocesky"): "jc",
    ("cz", "Liberecky"): "li",
    ("cz", "Karlovarsky"): "ka",
    ("cz", "Ustecky"): "us",

    # =========================================================================
    # France
    # =========================================================================
    ("fr", "Lorraine"): "lor",
    ("fr", "Rhone-Alpes"): "ara",
    ("fr", "Pays de la Loire"): "pdl",
    ("fr", "Aquitaine"): "naq",
    ("fr", "Languedoc-Roussillon"): "occ",
    ("fr", "Provence-Alpes-Cote d'Azur"): "pac",
    ("fr", "Ile-de-France"): "idf",
    ("fr", "Midi-Pyrenees"): "occ",
    ("fr", "Alsace"): "ges",
    ("fr", "Champagne-Ardenne"): "ges",
    ("fr", "Haute-Normandie"): "nor",
    ("fr", "Bourgogne"): "bfc",
    ("fr", "Bretagne"): "bre",
    ("fr", "Centre"): "cvl",
    ("fr", "Picardie"): "hdf",
    ("fr", "Nord-Pas-de-Calais"): "hdf",
    ("fr", "Basse-Normandie"): "nor",
    ("fr", "Poitou-Charentes"): "naq",
    ("fr", "Limousin"): "naq",
    ("fr", "Auvergne"): "ara",
    ("fr", "Franche-Comte"): "bfc",
    ("fr", "Corse"): "cor",

    # =========================================================================
    # Taiwan
    # =========================================================================
    ("tw", "Taiwan"): "twn",
    ("tw", "Taipei"): "tpe",
    ("tw", "Kaohsiung"): "khh",
    ("tw", "Taichung"): "txg",
    ("tw", "Tainan"): "tnn",

    # =========================================================================
    # Hungary
    # =========================================================================
    ("hu", "Budapest"): "bu",
    ("hu", "Pest"): "pe",
    ("hu", "Gyor-Moson-Sopron"): "gs",
    ("hu", "Fejer"): "fe",
    ("hu", "Veszprem"): "ve",
    ("hu", "Komarom-Esztergom"): "ke",
    ("hu", "Bacs-Kiskun"): "bk",
    ("hu", "Baranya"): "ba",
    ("hu", "Bekes"): "be",
    ("hu", "Borsod-Abauj-Zemplen"): "bz",
    ("hu", "Csongrad"): "cs",
    ("hu", "Hajdu-Bihar"): "hb",
    ("hu", "Heves"): "he",
    ("hu", "Jasz-Nagykun-Szolnok"): "jn",
    ("hu", "Nograd"): "no",
    ("hu", "Somogy"): "so",
    ("hu", "Szabolcs-Szatmar-Bereg"): "sz",
    ("hu", "Tolna"): "to",
    ("hu", "Vas"): "va",
    ("hu", "Zala"): "za",

    # =========================================================================
    # Belgium
    # =========================================================================
    ("be", "Flanders"): "vl",
    ("be", "Wallonia"): "wa",
    ("be", "Brussels"): "bru",

    # =========================================================================
    # Croatia
    # =========================================================================
    ("hr", "Brodsko-Posavska"): "bp",
    ("hr", "Zagrebacka"): "zg",
    ("hr", "Grad Zagreb"): "gz",
    ("hr", "Splitsko-Dalmatinska"): "sd",
    ("hr", "Primorsko-Goranska"): "pg",
    ("hr", "Istarska"): "is",
    ("hr", "Osjecko-Baranjska"): "ob",

    # =========================================================================
    # Austria
    # =========================================================================
    ("at", "Carinthia"): "ktn",
    ("at", "Lower Austria"): "noe",
    ("at", "Tyrol"): "tir",
    ("at", "Upper Austria"): "ooe",
    ("at", "Vienna"): "vie",
    ("at", "Salzburg"): "sbg",
    ("at", "Styria"): "stm",
    ("at", "Burgenland"): "bgl",
    ("at", "Vorarlberg"): "vbg",

    # =========================================================================
    # Brazil
    # =========================================================================
    ("br", "Rio de Janeiro"): "rj",
    ("br", "Rio Grande do Sul"): "rs",
    ("br", "Sao Paulo"): "sp",

    # =========================================================================
    # South Africa
    # =========================================================================
    ("za", "Western Cape"): "wc",
    ("za", "Eastern Cape"): "ec",
    ("za", "Gauteng"): "gt",
    ("za", "KwaZulu-Natal"): "kzn",

    # =========================================================================
    # Romania
    # =========================================================================
    ("ro", "Timis"): "tm",

    # =========================================================================
    # Argentina
    # =========================================================================
    ("ar", "Buenos Aires"): "ba",
    ("ar", "Buenos Aires F.D."): "cba",
    ("ar", "Cordoba"): "co",

    # =========================================================================
    # Mexico
    # =========================================================================
    ("mx", "Mexico City"): "cdmx",
    ("mx", "Chihuahua"): "chh",
    ("mx", "Baja California"): "bcn",
    ("mx", "Jalisco"): "jal",

    # =========================================================================
    # Other countries with data
    # =========================================================================
    ("dk", "Central Jutland"): "mid",
    ("dk", "South Denmark"): "syd",
    ("dk", "Capital Region"): "hov",
    ("dk", "North Denmark"): "nor",
    ("dk", "Zealand"): "sja",

    ("se", "Norrbotten"): "nrb",
    ("se", "Stockholm"): "sto",
    ("se", "Vastra Gotaland"): "vgo",
    ("se", "Skane"): "ska",

    ("no", "Finnmark Fylke"): "fin",
    ("no", "Hordaland"): "hor",
    ("no", "Nord-Trondelag"): "ntr",
    ("no", "Oslo"): "osl",

    ("fi", "Uusimaa"): "usi",
    ("fi", "Lapland"): "lap",

    ("es", "Catalonia"): "ct",
    ("es", "Andalusia"): "an",
    ("es", "Murcia"): "mc",
    ("es", "Extremadura"): "ex",
    ("es", "Basque Country"): "pv",
    ("es", "Madrid"): "md",

    ("it", "Lombardy"): "lom",
    ("it", "Piedmont"): "pie",
    ("it", "Tuscany"): "tos",
    ("it", "Sardinia"): "sar",

    ("sk", "Nitriansky"): "ni",

    ("si", "Ilirska Bistrica"): "ib",

    ("ua", "Crimea"): "crm",
    ("ua", "Mykolaiv"): "mk",

    ("pt", "Madeira"): "mad",

    ("jp", "Fukushima"): "fks",

    ("cn", "Zhejiang Sheng"): "zj",

    ("kz", "Almaty Qalasy"): "ala",

    ("li", "Schaan"): "sch",

    ("lt", "Vilnius County"): "vil",

    ("md", "Chisinau"): "cu",

    ("ge", "Tbilisi"): "tb",

    ("pr", "Trujillo Alto"): "ta",
    ("pr", "Cidra"): "cid",

    ("in", "Maharashtra"): "mh",
    ("in", "Central Java"): "jt",
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
