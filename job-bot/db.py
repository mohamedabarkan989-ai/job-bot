import time
import logging
from functools import wraps
from supabase import create_client
from config import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger(__name__)
_client = None


def db():
    global _client
    if not _client:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


def _retry(retries: int = 3, backoff: float = 2.0, default=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            global _client
            for attempt in range(retries):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    log.warning(f"[DB] {fn.__name__} attempt {attempt+1}/{retries}: {e}")
                    _client = None
                    if attempt < retries - 1:
                        time.sleep(backoff * (attempt + 1))
            log.error(f"[DB] {fn.__name__} failed after {retries} attempts")
            return default
        return wrapper
    return decorator


@_retry(default=set())
def existing_uids(uids: list[str]) -> set[str]:
    """Batch existence check — 1 query instead of N."""
    r = db().table("jobs").select("uid").in_("uid", uids).execute()
    return {row["uid"] for row in (r.data or [])}


@_retry(default=0)
def save_jobs(jobs: list[dict]) -> int:
    """Batch upsert — 1 query for all new jobs."""
    if not jobs:
        return 0
    db().table("jobs").upsert(jobs, on_conflict="uid", ignore_duplicates=True).execute()
    return len(jobs)


@_retry(default=[])
def unsent_jobs(limit: int = 50) -> list[dict]:
    r = (db().table("jobs")
         .select("*")
         .eq("sent", False)
         .in_("contract", ["CDI", "CDD", "CIVP", "STAGE"])
         .order("score", desc=True)        # best matches first
         .order("created_at", desc=True)   # then newest
         .limit(limit)
         .execute())
    return r.data or []


@_retry(default=None)
def mark_sent(uids: list[str]) -> None:
    if uids:
        db().table("jobs").update({"sent": True}).in_("uid", uids).execute()


@_retry(default={})
def stats() -> dict:
    # aggregate server-side — no full table scan in memory
    total_r    = db().table("jobs").select("uid", count="exact").execute()
    sent_r     = db().table("jobs").select("uid", count="exact").eq("sent", True).execute()
    contract_r = db().rpc("stats_by_contract").execute()
    source_r   = db().rpc("stats_by_source").execute()

    by_contract = {r["contract"]: r["count"] for r in (contract_r.data or [])}
    by_source   = {r["source"]:   r["count"] for r in (source_r.data   or [])}
    total = total_r.count or 0
    sent  = sent_r.count  or 0

    return {"total": total, "sent": sent, "by_contract": by_contract, "by_source": by_source}
