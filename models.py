"""Database schema and data models."""
import sqlite3
import hashlib
import logging
from datetime import datetime

log = logging.getLogger(__name__)


def init_db() -> sqlite3.Connection:
    """Initialize SQLite database."""
    conn = sqlite3.connect("jobs.db")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            uid      TEXT PRIMARY KEY,
            title    TEXT,
            company  TEXT,
            location TEXT,
            source   TEXT,
            url      TEXT,
            contract TEXT,
            salary   TEXT,
            sector   TEXT,
            sent     INTEGER DEFAULT 0,
            ts       TEXT
        )
    """
    )
    conn.commit()
    return conn


SEEN_THIS_RUN: set[str] = set()


def save_job(conn: sqlite3.Connection, job: dict) -> int:
    """
    Save job to database. Returns 1 if new, 0 if duplicate.
    Only marks rows whose tg_send() returned True.
    """
    uid = hashlib.md5(f"{job['url']}{job['title']}".encode()).hexdigest()

    if uid in SEEN_THIS_RUN:
        return 0
    SEEN_THIS_RUN.add(uid)

    try:
        conn.execute(
            "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,0,?)",
            (
                uid,
                job["title"],
                job["company"],
                job["location"],
                job["source"],
                job["url"],
                job["contract"],
                job["salary"],
                job.get("sector", ""),
                datetime.now().isoformat(),
            ),
        )
        conn.commit()
        return 1
    except sqlite3.IntegrityError:
        return 0
