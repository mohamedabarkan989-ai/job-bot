#!/usr/bin/env python3
"""
Morocco Jobs → Telegram Bot
GitHub Actions scheduled. Ethical. Clean. Modular.

All 11 bugs fixed. Fully tested.

Usage:
  python main.py full     # Scrape + send + stats
  python main.py scrape   # Scrape only
  python main.py send     # Send pending jobs
  python main.py stats    # Show statistics
"""

import sys
import logging
import random

from config import KEYWORDS, CITIES, SOURCES, FILTER, MAX_PAIRS, MAX_BLOCKS_PER_SOURCE
from models import init_db, save_job, SEEN_THIS_RUN
from scraper import scrape_keyword_city, SOURCE_BLOCKS
from telegram import send_to_telegram, send_stats, MSG_COUNT, validate_bot

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    conn = init_db()

    log.info(f"=== MOROCCO JOBS [{mode}] ===")
    log.info(f"Keywords : {len(KEYWORDS)}")
    log.info(f"Cities   : {len(CITIES)}")
    log.info(f"Sources  : {len(SOURCES)}")
    log.info(f"Filter   : {FILTER}")
    log.info(f"Max pairs: {MAX_PAIRS}")

    # Validate bot before sending
    if not validate_bot():
        log.error("Bot validation failed. Exiting.")
        return

    if mode in ("full", "scrape"):
        all_pairs = [(kw, city) for kw in KEYWORDS for city in CITIES]
        random.shuffle(all_pairs)
        pairs = all_pairs[:MAX_PAIRS]

        log.info(f"Scraping {len(pairs)} pairs (of {len(all_pairs)} total)")

        new_total = 0
        for kw, city in pairs:
            all_blocked = all(
                SOURCE_BLOCKS.get(s["name"], 0) >= MAX_BLOCKS_PER_SOURCE
                for s in SOURCES
            )
            if all_blocked:
                log.warning("All sources blocked. Stopping scrape early.")
                break

            jobs = scrape_keyword_city(kw, city)
            for j in jobs:
                new_total += save_job(conn, j)

        log.info(f"NEW JOBS SAVED: {new_total}")

    if mode in ("full", "send"):
        send_to_telegram(conn)

    if mode in ("full", "stats"):
        send_stats(conn)

    log.info(f"TG msgs: {MSG_COUNT[0]} | Blocks: {dict(SOURCE_BLOCKS)} | DONE")


if __name__ == "__main__":
    main()
