"""Configuration and constants."""
import os
import json
import logging

try:
    from dotenv import load_dotenv
    load_dotenv()  # Load from .env if available (local dev)
except ImportError:
    pass  # python-dotenv not installed, use direct env vars (GitHub Actions)

log = logging.getLogger(__name__)

# ═══ TELEGRAM ═══
TG_TOKEN = os.environ.get("TG_TOKEN", "YOUR_TOKEN")
TG_CHAT = os.environ.get("TG_CHAT", "YOUR_CHAT")
TG_MAX_BYTES = 4000  # Telegram hard limit is 4096

# ═══ SCRAPING ═══
MAX_PAIRS = int(os.environ.get("MAX_PAIRS", "400"))
MAX_BLOCKS_PER_SOURCE = 3
FILTER = ["CDI", "CDD", "CIVP", "STAGE"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,ar;q=0.8,en;q=0.7",
}

# ═══ CONTRACT MAPPING ═══
CONTRACT_MAP = {
    "cdi": "CDI",
    "permanent": "CDI",
    "indéterminé": "CDI",
    "temps plein": "CDI",
    "full time": "CDI",
    "cdd": "CDD",
    "déterminé": "CDD",
    "temporaire": "CDD",
    "intérim": "CDD",
    "civp": "CIVP",
    "insertion": "CIVP",
    "stage": "STAGE",
    "internship": "STAGE",
    "stagiaire": "STAGE",
    "pfe": "STAGE",
    "freelance": "FREELANCE",
    "consultant": "FREELANCE",
    "saison": "SAISON",
    "saisonnier": "SAISON",
}

ICONS = {
    "CDI": "🟢",
    "CDD": "🟡",
    "CIVP": "🔵",
    "STAGE": "🟣",
    "FREELANCE": "🟠",
    "SAISON": "⚪",
    "?": "⚫",
}

# ═══ KEYWORDS & CITIES ═══
def load_keywords() -> list[str]:
    """Load keywords from external JSON file."""
    try:
        with open("keywords.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            keywords = data.get("keywords", [])
            log.info(f"Loaded {len(keywords)} keywords")
            return keywords
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.warning(f"Could not load keywords.json: {e}. Using fallback.")
        return ["ingénieur", "développeur", "data scientist"]


KEYWORDS = load_keywords()

CITIES = [
    "Casablanca",
    "Rabat",
    "Marrakech",
    "Tanger",
    "Fès",
    "Meknès",
    "Agadir",
    "Oujda",
    "Kénitra",
    "Tétouan",
    "El Jadida",
    "Nador",
    "Mohammedia",
    "Safi",
    "Khouribga",
]

# ═══ JOB SOURCES ═══
SOURCES = [
    {
        "name": "ANAPEC",
        "base": "https://www.anapec.org",
        "path": "/search/result?mot_cle={}&ville={}",
    },
    {
        "name": "Rekrute",
        "base": "https://www.rekrute.com",
        "path": "/offres-emploi.html?q={}&l={}",
    },
    {
        "name": "Emploi.ma",
        "base": "https://www.emploi.ma",
        "path": "/recherche?q={}&l={}",
    },
    {"name": "Indeed", "base": "https://ma.indeed.com", "path": "/jobs?q={}&l={}&sort=date"},
    {
        "name": "MarocAnnonces",
        "base": "https://www.marocannonces.com",
        "path": "/emploi/offres-emploi/?keyword={}&city={}",
    },
    {
        "name": "Bayt",
        "base": "https://www.bayt.com/en/morocco",
        "path": "/jobs/{}-jobs/?location={}",
    },
    {
        "name": "Jobijoba",
        "base": "https://www.jobijoba.com/ma",
        "path": "/search?what={}&where={}",
    },
    {
        "name": "Glassdoor",
        "base": "https://www.glassdoor.com",
        "path": "/Job/morocco-{kw}-jobs-SRCH_IL.0,7_IN187_KO8,{end}.htm",
    },
    {
        "name": "MEmploi",
        "base": "https://www.memploi.ma",
        "path": "/recherche?q={}&l={}",
    },
    {
        "name": "Moovjobs",
        "base": "https://www.moovjobs.com",
        "path": "/emplois?q={}&l={}",
    },
    {
        "name": "Kemayo",
        "base": "https://www.kemayo.ma",
        "path": "/offres-emploi?q={}&l={}",
    },
    {
        "name": "OptionCarriere",
        "base": "https://www.optioncarriere.ma",
        "path": "/emploi?q={}&l={}",
    },
    {
        "name": "Talent.com",
        "base": "https://ma.talent.com",
        "path": "/search?l={}&q={}",
    },
    {
        "name": "Careerjet",
        "base": "https://www.careerjet.ma",
        "path": "/search?loc={}&keywords={}",
    },
    {
        "name": "Waadni",
        "base": "https://www.waadni.com",
        "path": "/recherche?q={}&l={}",
    },
]
