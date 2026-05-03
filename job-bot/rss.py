import asyncio
import hashlib
import logging
import random
from datetime import datetime, timezone

import aiohttp
import feedparser

from config import RSS_SOURCES, CONTRACT_MAP, KEYWORDS, CITIES

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MoroccoJobsBot/2.0)"}
MIN_SCORE = 1


def _norm_contract(text: str) -> str:
    if not text:
        return "?"
    low = text.lower()
    for k, v in CONTRACT_MAP.items():
        if k in low:
            return v
    return "?"


def _uid(url: str, title: str) -> str:
    return hashlib.md5(f"{url}{title}".encode()).hexdigest()


def _score(entry) -> int:
    title   = entry.get("title", "").lower()
    summary = entry.get("summary", "").lower()
    score   = 0
    for k in KEYWORDS:
        kl = k.lower()
        if kl in title:   score += 2
        elif kl in summary: score += 1
    for c in CITIES:
        if c.lower() in title or c.lower() in summary:
            score += 1
    return score


async def _fetch(session: aiohttp.ClientSession, url: str, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        await asyncio.sleep(random.uniform(1, 3))
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    return await r.read()
                if r.status == 429:
                    wait = int(r.headers.get("Retry-After", 60 * (attempt + 1)))
                    log.warning(f"[RSS] 429 {url} — waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if r.status in (403, 404):
                    log.warning(f"[RSS] {r.status} {url} — skip")
                    return None
        except Exception as ex:
            log.warning(f"[RSS] attempt {attempt+1}/{retries} {url}: {ex}")
            await asyncio.sleep(10 * (attempt + 1))
    return None


async def _fetch_source(session: aiohttp.ClientSession, url: str, seen: set) -> list[dict]:
    content = await _fetch(session, url)
    if not content:
        return []
    feed   = feedparser.parse(content)
    source = feed.feed.get("title", url.split("/")[2])
    jobs   = []
    for e in feed.entries:
        score = _score(e)
        if score < MIN_SCORE:
            continue
        link  = e.get("link", "")
        title = e.get("title", "").strip()
        if not link or len(title) < 5:
            continue
        uid = _uid(link, title)
        if uid in seen:
            continue
        seen.add(uid)
        summary = e.get("summary", "")
        jobs.append({
            "uid":        uid,
            "title":      title,
            "company":    e.get("author", "N/A"),
            "location":   next((c for c in CITIES if c.lower() in summary.lower()), "?"),
            "source":     source,
            "url":        link,
            "contract":   _norm_contract(summary),
            "salary":     "",
            "sector":     "",
            "sent":       False,
            "score":      score,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    log.info(f"[{source}] {len(jobs)} jobs (score>={MIN_SCORE})")
    return jobs


async def _fetch_all(extra_urls: list[str] = []) -> list[dict]:
    seen = set()
    urls = RSS_SOURCES + extra_urls
    async with aiohttp.ClientSession() as session:
        chunks = await asyncio.gather(*[_fetch_source(session, u, seen) for u in urls])
    jobs = [j for chunk in chunks for j in chunk]
    jobs.sort(key=lambda j: j["score"], reverse=True)
    return jobs


def fetch_rss(extra_urls: list[str] = []) -> list[dict]:
    return asyncio.run(_fetch_all(extra_urls))
