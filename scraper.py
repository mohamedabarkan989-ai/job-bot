"""Scraping logic and HTML parsing."""
import re
import time
import random
import logging
from urllib.parse import quote_plus, urlparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import requests

from config import HEADERS, SOURCES, CONTRACT_MAP

log = logging.getLogger(__name__)

# Cache RobotFileParser per domain (FIX-5)
_robots_cache: dict[str, RobotFileParser] = {}

# Per-source block counter (FIX-7)
SOURCE_BLOCKS: dict[str, int] = {}


def robots_ok(url: str) -> bool:
    """Check if URL is allowed by robots.txt."""
    p = urlparse(url)
    base = f"{p.scheme}://{p.netloc}"
    if base not in _robots_cache:
        rp = RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        try:
            rp.read()
        except Exception:
            pass
        _robots_cache[base] = rp
    return _robots_cache[base].can_fetch("*", url)


def fetch(url: str, source_name: str = "?") -> BeautifulSoup | None:
    """Fetch URL with retry/back-off. Returns BeautifulSoup or None."""
    if not robots_ok(url):
        log.info(f"robots.txt blocked: {url}")
        return None

    from config import MAX_BLOCKS_PER_SOURCE

    if SOURCE_BLOCKS.get(source_name, 0) >= MAX_BLOCKS_PER_SOURCE:
        return None

    for i in range(3):
        try:
            time.sleep(random.uniform(5, 10))
            r = requests.get(url, headers=HEADERS, timeout=20)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                log.warning(f"429 from {source_name}. Waiting {wait}s")
                time.sleep(wait)
                continue

            if r.status_code == 403:
                SOURCE_BLOCKS[source_name] = SOURCE_BLOCKS.get(source_name, 0) + 1
                log.warning(
                    f"403 [{source_name}] "
                    f"({SOURCE_BLOCKS[source_name]}/{MAX_BLOCKS_PER_SOURCE})"
                )
                return None

            if r.status_code == 404:
                return None

            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")

        except Exception as e:
            log.warning(f"Fetch retry {i+1}/3 [{source_name}]: {e}")
            time.sleep(10 * (i + 1))

    return None


def txt(el) -> str:
    """Extract text from element."""
    return el.get_text(strip=True) if el else ""


def find_text(card, patterns: list[str]) -> str:
    """Find text in element by class patterns (word boundary regex)."""
    for p in patterns:
        el = card.find(class_=re.compile(r"\b" + re.escape(p) + r"\b", re.I))
        if el:
            return txt(el)
    return ""


def norm_contract(raw: str) -> str:
    """Normalize contract type."""
    if not raw:
        return "?"
    low = raw.lower()
    for k, v in CONTRACT_MAP.items():
        if k in low:
            return v
    return "?"


def build_url(src: dict, kw: str, city: str) -> str | None:
    """Build URL for job source (FIX-3: Glassdoor uses unencoded length)."""
    kw_enc = quote_plus(kw)
    city_enc = quote_plus(city)

    if src["name"] == "Glassdoor":
        try:
            end = 8 + len(kw)
            path = src["path"].format(kw=kw_enc, end=end)
            return src["base"] + path
        except (IndexError, KeyError):
            return None

    try:
        return src["base"] + src["path"].format(kw_enc, city_enc)
    except (IndexError, KeyError):
        return None


def parse_cards(soup: BeautifulSoup, source: str, base_url: str) -> list[dict]:
    """
    Parse job cards from HTML. Rejects off-site links (FIX-2).
    Requires min title length >= 8 chars.
    """
    base_netloc = urlparse(base_url).netloc
    jobs = []

    for card in soup.find_all(["div", "article", "li", "tr", "section"]):
        el = card.find(["h2", "h3"]) or card.find("a")
        if not el:
            continue

        title = txt(el)
        if len(title) < 8:
            continue

        a = card.find("a")
        link = a["href"] if a and a.get("href") else ""
        if link and not link.startswith("http"):
            link = base_url.rstrip("/") + "/" + link.lstrip("/")
        if not link:
            continue

        # FIX-2a: drop off-site links
        link_netloc = urlparse(link).netloc
        if link_netloc and link_netloc != base_netloc:
            continue

        jobs.append(
            {
                "title": title,
                "company": find_text(
                    card, ["company", "entreprise", "corp", "employer"]
                )
                or "N/A",
                "location": find_text(
                    card, ["location", "ville", "lieu", "city", "region"]
                )
                or "?",
                "source": source,
                "url": link,
                "contract": norm_contract(
                    find_text(card, ["type", "contrat", "contract", "nature"])
                ),
                "salary": find_text(card, ["salary", "salaire", "remuneration"]),
                "sector": find_text(card, ["sector", "secteur", "category"]),
            }
        )

    return jobs


def scrape_keyword_city(kw: str, city: str) -> list[dict]:
    """Scrape all sources for keyword/city pair."""
    from config import MAX_BLOCKS_PER_SOURCE

    all_jobs = []
    for src in SOURCES:
        if SOURCE_BLOCKS.get(src["name"], 0) >= MAX_BLOCKS_PER_SOURCE:
            log.warning(f"[{src['name']}] skipped — block limit reached")
            continue
        try:
            url = build_url(src, kw, city)
            if not url:
                continue
            soup = fetch(url, source_name=src["name"])
            if soup:
                jobs = parse_cards(soup, src["name"], src["base"])
                all_jobs.extend(jobs)
                if jobs:
                    log.info(f"[{src['name']}] {kw}/{city}: {len(jobs)} found")
        except Exception as e:
            log.error(f"[{src['name']}] {e}")
    return all_jobs
