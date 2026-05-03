#!/usr/bin/env python3
"""Morocco Jobs Bot — RSS + Supabase + Telegram
Usage: python main.py [full|fetch|send|stats|trends]
"""
import sys
import logging
from config import MAX_SEND

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


def cmd_fetch(extra_urls: list[str] = []):
    from rss import fetch_rss
    from db import existing_uids, save_jobs
    jobs    = fetch_rss(extra_urls)
    uids    = [j["uid"] for j in jobs]
    seen    = existing_uids(uids)
    new     = [j for j in jobs if j["uid"] not in seen]
    saved   = save_jobs(new)
    log.info(f"Fetched {len(jobs)} | new {len(new)} | saved {saved}")


def cmd_send():
    from db import unsent_jobs, mark_sent
    from telegram import send_jobs
    jobs = unsent_jobs(MAX_SEND)
    sent = send_jobs(jobs)
    if sent:
        mark_sent(sent)
    log.info(f"Sent {len(sent)}/{len(jobs)}")


def cmd_stats():
    from db import stats
    from telegram import send_stats
    send_stats(stats())


def cmd_trends():
    from trends import trending_keywords, rss_urls_from_trends
    kws  = trending_keywords()
    urls = rss_urls_from_trends(kws)
    log.info(f"Trends: {len(kws)} kws → {len(urls)} extra URLs")
    cmd_fetch(extra_urls=urls)


def cmd_full():
    cmd_trends()
    cmd_send()
    cmd_stats()


COMMANDS = {"fetch": cmd_fetch, "send": cmd_send,
            "stats": cmd_stats, "trends": cmd_trends, "full": cmd_full}

if __name__ == "__main__":
    from telegram import validate
    if not validate():
        log.error("Invalid Telegram token. Aborting.")
        sys.exit(1)
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    fn   = COMMANDS.get(mode)
    if not fn:
        log.error(f"Unknown: {mode}. Use: {list(COMMANDS)}")
        sys.exit(1)
    log.info(f"=== [{mode}] ===")
    fn()
    log.info("Done.")
