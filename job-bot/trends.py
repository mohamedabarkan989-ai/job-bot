import logging
from pytrends.request import TrendReq

log = logging.getLogger(__name__)

SEED_TERMS = ["emploi Maroc", "recrutement Maroc", "offre emploi", "stage Maroc"]


def trending_keywords(top_n: int = 10) -> list[str]:
    try:
        pt = TrendReq(hl="fr-MA", tz=0, timeout=(10, 25), retries=2, backoff_factor=0.5)
        pt.build_payload(SEED_TERMS[:4], geo="MA", timeframe="now 7-d")
        related = pt.related_queries()
        kws = []
        for term in SEED_TERMS:
            df = related.get(term, {}).get("top")
            if df is not None and not df.empty:
                kws.extend(df["query"].tolist())
        seen, unique = set(), []
        for k in kws:
            if k not in seen:
                seen.add(k)
                unique.append(k)
        result = unique[:top_n]
        log.info(f"[Trends] {len(result)} keywords: {result}")
        return result
    except Exception as ex:
        log.warning(f"[Trends] unavailable: {ex}")
        return []


def rss_urls_from_trends(keywords: list[str]) -> list[str]:
    """Build extra Indeed RSS URLs from trending keywords."""
    urls = []
    for kw in keywords:
        from urllib.parse import quote_plus
        urls.append(f"https://ma.indeed.com/rss?q={quote_plus(kw)}&l=Maroc&sort=date")
    return urls
