"""Configuration loader for the Polymarket Weather Bot."""
import os
import re
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")


class Config:
    """Runtime configuration loaded from a local, gitignored `.env` file."""

    # Values that should never be treated as real credentials. These allow the
    # public `.env.template` to be explicit without accidentally enabling live
    # trading if copied unchanged.
    _PLACEHOLDER_VALUES = {
        "",
        "replace_me",
        "replace_me_with_local_private_key",
        "replace_me_with_local_funder_address",
        "your_private_key_here",
        "your_funder_address_here",
    }

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}

    @classmethod
    def _looks_configured(cls, value: str) -> bool:
        return bool(value) and value.strip() not in cls._PLACEHOLDER_VALUES

    # Polymarket Auth
    PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "").strip()
    FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "").strip()
    SIGNER_ADDRESS = os.getenv("POLY_SIGNER_ADDRESS", "").strip()
    SIGNATURE_TYPE = int(os.getenv("POLY_SIGNATURE_TYPE", "1"))
    CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()
    GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
    CHAIN_ID = int(os.getenv("POLY_CHAIN_ID", "137"))

    # Bot settings. Live trading is opt-in twice: CLI flag + env flag.
    ENABLE_LIVE_TRADING = _env_bool("ENABLE_LIVE_TRADING", False)
    PAPER_TRADING = _env_bool("PAPER_TRADING", True)
    MAX_CAPITAL = float(os.getenv("MAX_CAPITAL", "25.00"))
    MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", "2.00"))
    EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.15"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()

    @classmethod
    def live_trading_errors(cls) -> list[str]:
        """Return reasons live trading should be refused."""
        errors: list[str] = []
        if not cls.ENABLE_LIVE_TRADING:
            errors.append("ENABLE_LIVE_TRADING must be set to true")
        if cls.PAPER_TRADING:
            errors.append("PAPER_TRADING must be set to false")
        if not cls._looks_configured(cls.PRIVATE_KEY):
            errors.append("POLY_PRIVATE_KEY is missing or still a placeholder")
        elif not re.fullmatch(r"0x[0-9a-fA-F]{64}", cls.PRIVATE_KEY):
            errors.append("POLY_PRIVATE_KEY must be 0x followed by 64 hex characters")
        if not cls._looks_configured(cls.FUNDER_ADDRESS):
            errors.append("POLY_FUNDER_ADDRESS is missing or still a placeholder")
        elif not re.fullmatch(r"0x[0-9a-fA-F]{40}", cls.FUNDER_ADDRESS):
            errors.append("POLY_FUNDER_ADDRESS must be 0x followed by 40 hex characters")
        return errors

    @classmethod
    def assert_live_trading_allowed(cls) -> None:
        """Fail closed before any real-money CLOB client is initialized."""
        errors = cls.live_trading_errors()
        if errors:
            joined = "; ".join(errors)
            raise RuntimeError(f"Live trading refused: {joined}")

    # Polymarket data API
    DATA_API_HOST = "https://data-api.polymarket.com"

    # City coordinates for weather markets
    # IMPORTANT: Uses AIRPORT STATION coordinates, NOT city centers!
    # Polymarket resolves via Weather Underground airport stations.
    # Using city center coords causes 1-3°F errors vs actual resolution.
    # Maps Polymarket city names → (lat, lon, timezone)
    CITY_COORDS = {
        # US cities (resolve in °F) — Wunderground airport stations
        "new york":     (40.7772, -73.8726, "America/New_York"),     # KLGA LaGuardia
        "nyc":          (40.7772, -73.8726, "America/New_York"),     # KLGA LaGuardia
        "chicago":      (41.7868, -87.7522, "America/Chicago"),      # KMDW Midway
        "seattle":      (47.4502, -122.3088, "America/Los_Angeles"), # KSEA Sea-Tac
        "atlanta":      (33.6407, -84.4277, "America/New_York"),     # KATL Hartsfield
        "dallas":       (32.8998, -97.0403, "America/Chicago"),      # KDFW DFW
        "miami":        (25.7959, -80.2870, "America/New_York"),     # KMIA Miami Intl
        "los angeles":  (33.9425, -118.4081, "America/Los_Angeles"), # KLAX LAX
        "austin":       (30.1975, -97.6664, "America/Chicago"),      # KAUS Bergstrom
        "houston":      (29.6454, -95.2789, "America/Chicago"),      # KHOU Hobby
        "denver":       (39.7174, -104.7506, "America/Denver"),      # KBKF Buckley
        "san francisco": (37.6213, -122.3790, "America/Los_Angeles"),# KSFO SFO
        # International cities (resolve in °C) — airport stations
        "london":       (51.5048, 0.0495, "Europe/London"),          # EGLC London City
        "paris":        (49.0097, 2.5479, "Europe/Paris"),           # LFPG CDG
        "tokyo":        (35.5494, 139.7798, "Asia/Tokyo"),           # RJTT Haneda
        "seoul":        (37.4602, 126.4407, "Asia/Seoul"),           # RKSI Incheon
        "shanghai":     (31.1443, 121.8083, "Asia/Shanghai"),        # ZSPD Pudong
        "toronto":      (43.6772, -79.6306, "America/Toronto"),      # CYYZ Pearson
        "singapore":    (1.3644, 103.9915, "Asia/Singapore"),        # WSSS Changi
        "hong kong":    (22.3080, 113.9185, "Asia/Hong_Kong"),       # VHHH HK Intl
        "taipei":       (25.0777, 121.2328, "Asia/Taipei"),          # RCTP Taoyuan
        "wellington":   (-41.3272, 174.8053, "Pacific/Auckland"),    # NZWN Wellington
        "buenos aires": (-34.8222, -58.5358, "America/Argentina/Buenos_Aires"), # SAEZ Ezeiza
        "sao paulo":    (-23.4356, -46.4731, "America/Sao_Paulo"),   # SBGR Guarulhos
        "ankara":       (40.1281, 32.9951, "Europe/Istanbul"),       # LTAC Esenboga
        "istanbul":     (40.9828, 28.8108, "Europe/Istanbul"),       # LTFM Istanbul
        "munich":       (48.3538, 11.7861, "Europe/Berlin"),         # EDDM Munich
        "tel aviv":     (32.0114, 34.8867, "Asia/Jerusalem"),        # LLBG Ben Gurion
        "milan":        (45.6306, 8.7231, "Europe/Rome"),            # LIMC Malpensa
        "madrid":       (40.4719, -3.5626, "Europe/Madrid"),         # LEMD Barajas
        "warsaw":       (52.1657, 20.9671, "Europe/Warsaw"),         # EPWA Chopin
        "beijing":      (40.0799, 116.6031, "Asia/Shanghai"),        # ZBAA Capital
        "wuhan":        (30.7838, 114.2081, "Asia/Shanghai"),        # ZHHH Tianhe
        "chengdu":      (30.5785, 103.9471, "Asia/Shanghai"),        # ZUUU Shuangliu
        "shenzhen":     (22.6393, 113.8107, "Asia/Shanghai"),        # ZGSZ Bao'an
        "chongqing":    (29.7192, 106.6417, "Asia/Chongqing"),       # ZUCK Jiangbei
        "lucknow":      (26.7606, 80.8893, "Asia/Kolkata"),          # VILK Amausi
        # Added 2026-04-03 — new Polymarket cities
        "mexico city":  (19.4363, -99.0721, "America/Mexico_City"),  # MMMX Benito Juárez
        "amsterdam":    (52.3086, 4.7639, "Europe/Amsterdam"),       # EHAM Schiphol
        "helsinki":      (60.3172, 24.9633, "Europe/Helsinki"),       # EFHK Helsinki-Vantaa
        "panama city":  (9.0714, -79.3835, "America/Panama"),        # MPTO Tocumen
        "kuala lumpur": (2.7456, 101.7099, "Asia/Kuala_Lumpur"),     # WMKK KLIA
        "jakarta":      (-6.1256, 106.6559, "Asia/Jakarta"),         # WIII Soekarno-Hatta
        # Less common
        "berlin":       (52.3667, 13.5033, "Europe/Berlin"),         # EDDB Brandenburg
        "sydney":       (-33.9461, 151.1772, "Australia/Sydney"),    # YSSY Kingsford
        "mumbai":       (19.0896, 72.8656, "Asia/Kolkata"),          # VABB Chhatrapati
        "moscow":       (55.9726, 37.4146, "Europe/Moscow"),         # UUEE Sheremetyevo
        "stockholm":    (59.6519, 17.9186, "Europe/Stockholm"),      # ESSA Arlanda
        # Added 2026-04-10 — forecast scanner found these cities on Polymarket
        "cape town":    (-33.9715, 18.6021, "Africa/Johannesburg"),  # FACT Cape Town Intl
        "jeddah":       (21.6805, 39.1747, "Asia/Riyadh"),           # OEJN King Abdulaziz
        "lagos":        (6.5774, 3.3215, "Africa/Lagos"),            # DNMM Murtala Muhammed
    }

    # ICAO airport codes for Aviation TAF lookups
    # These are the EXACT stations used for Polymarket weather resolution
    CITY_ICAO = {
        "new york": "KLGA",
        "nyc": "KLGA",
        "chicago": "KMDW",
        "seattle": "KSEA",
        "atlanta": "KATL",
        "dallas": "KDFW",
        "miami": "KMIA",
        "los angeles": "KLAX",
        "austin": "KAUS",
        "houston": "KHOU",
        "denver": "KBKF",
        "san francisco": "KSFO",
        "london": "EGLC",
        "paris": "LFPG",
        "tokyo": "RJTT",
        "seoul": "RKSI",
        "shanghai": "ZSPD",
        "toronto": "CYYZ",
        "singapore": "WSSS",
        "hong kong": "VHHH",
        "taipei": "RCTP",
        "wellington": "NZWN",
        "buenos aires": "SAEZ",
        "sao paulo": "SBGR",
        "ankara": "LTAC",
        "munich": "EDDM",
        "tel aviv": "LLBG",
        "milan": "LIMC",
        "madrid": "LEMD",
        "warsaw": "EPWA",
        "beijing": "ZBAA",
        "wuhan": "ZHHH",
        "chengdu": "ZUUU",
        "shenzhen": "ZGSZ",
        "chongqing": "ZUCK",
        "lucknow": "VILK",
        "berlin": "EDDB",
        "sydney": "YSSY",
        "mumbai": "VABB",
        "moscow": "UUEE",
        "stockholm": "ESSA",
        "istanbul": "LTFM",
        "mexico city": "MMMX",
        "amsterdam": "EHAM",
        "helsinki": "EFHK",
        "panama city": "MPTO",
        "kuala lumpur": "WMKK",
        "jakarta": "WIII",
    }

    @classmethod
    def get_city_icao(cls, city_name: str) -> Optional[str]:
        """Look up ICAO airport code for a city. Returns code or None."""
        key = city_name.lower().strip()
        key = cls.CITY_ALIASES.get(key, key)
        if key in cls.CITY_ICAO:
            return cls.CITY_ICAO[key]
        for known, code in cls.CITY_ICAO.items():
            if known in key or key in known:
                return code
        return None

    # Aliases for city names that appear differently on Polymarket
    CITY_ALIASES = {
        "new york city": "new york",
        "nyc": "new york",
        "são paulo": "sao paulo",
        "sã£o paulo": "sao paulo",
    }

    @classmethod
    def get_city_coords(cls, city_name: str):
        """Look up city coordinates. Returns (lat, lon, tz) or None."""
        key = city_name.lower().strip()
        # Check aliases first
        key = cls.CITY_ALIASES.get(key, key)
        # Direct match
        if key in cls.CITY_COORDS:
            return cls.CITY_COORDS[key]
        # Fuzzy: check if either is a substring of the other
        for known, coords in cls.CITY_COORDS.items():
            if known in key or key in known:
                return coords
        return None
