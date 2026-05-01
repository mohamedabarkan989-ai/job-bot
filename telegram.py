"""Telegram bot integration."""
import sqlite3
import time
import random
import logging
import requests

from config import TG_TOKEN, TG_CHAT, TG_MAX_BYTES, ICONS, FILTER
from datetime import datetime

log = logging.getLogger(__name__)

MSG_COUNT = [0]


def tg_send(text: str) -> bool:
    """Send message to Telegram with rate limiting."""
    for i in range(3):
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={
                    "chat_id": TG_CHAT,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if r.status_code == 429:
                wait = r.json().get("parameters", {}).get("retry_after", 30)
                log.warning(f"TG rate limit. Waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 200:
                MSG_COUNT[0] += 1
                time.sleep(random.uniform(0.5, 2.0))
                if MSG_COUNT[0] % 20 == 0:
                    log.info(f"TG cooldown after {MSG_COUNT[0]} msgs")
                    time.sleep(30)
                return True
            log.error(f"TG error: {r.status_code} — {r.text[:200]}")
        except Exception as e:
            log.error(f"TG send error: {e}")
        time.sleep(10)
    return False


def format_job(row) -> str:
    """Format job posting for Telegram."""
    ct = row[6] or "?"
    icon = ICONS.get(ct, "⚫")
    parts = [
        f"{icon} <b>{row[1]}</b>",
        f"🏢 {row[2]}",
        f"📍 {row[3]}",
    ]
    if row[7]:
        parts.append(f"💰 {row[7]}")
    parts.append(f"📋 {ct}")
    if row[8]:
        parts.append(f"🏷️ {row[8]}")
    parts.extend(
        [
            f'🔗 <a href="{row[5]}">Voir l\'offre</a>',
            f"📡 {row[4]}",
            "━━━━━━━━━━━━━━━━━━",
        ]
    )
    return "\n".join(parts)


def send_to_telegram(conn: sqlite3.Connection) -> None:
    """Send unsent jobs to Telegram (FIX-4, FIX-10, FIX-11)."""
    placeholders = ",".join("?" * len(FILTER))
    rows = conn.execute(
        f"SELECT * FROM jobs "
        f"WHERE sent=0 AND contract IN ({placeholders}) "
        f"ORDER BY ts DESC LIMIT 200",
        FILTER,
    ).fetchall()

    if not rows:
        tg_send("📭 ما لقيناش عروض جداد اليوم.")
        return

    total = 0
    sent_uids = []

    current_msg = ""
    current_rows = []

    def flush(msg: str, batch_rows: list) -> None:
        nonlocal total
        if not msg.strip():
            return
        header = f"📦 <b>{len(batch_rows)} عروض</b>\n" "━━━━━━━━━━━━━━━━━━\n"
        ok = tg_send(header + msg)
        if ok:
            sent_uids.extend(r[0] for r in batch_rows)
            total += len(batch_rows)

    for row in rows:
        chunk = format_job(row) + "\n"
        if len((current_msg + chunk).encode("utf-8")) > TG_MAX_BYTES:
            flush(current_msg, current_rows)
            current_msg = chunk
            current_rows = [row]
        else:
            current_msg += chunk
            current_rows.append(row)

    flush(current_msg, current_rows)

    for uid in sent_uids:
        conn.execute("UPDATE jobs SET sent=1 WHERE uid=?", (uid,))
    conn.commit()

    log.info(f"Sent {total} jobs to Telegram")


def send_stats(conn: sqlite3.Connection) -> None:
    """Send job statistics to Telegram."""
    total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    sent = conn.execute("SELECT COUNT(*) FROM jobs WHERE sent=1").fetchone()[0]

    by_contract = conn.execute(
        "SELECT contract, COUNT(*) FROM jobs GROUP BY contract ORDER BY COUNT(*) DESC"
    ).fetchall()
    by_source = conn.execute(
        "SELECT source, COUNT(*) FROM jobs GROUP BY source ORDER BY COUNT(*) DESC"
    ).fetchall()
    by_city = conn.execute(
        "SELECT location, COUNT(*) FROM jobs GROUP BY location ORDER BY COUNT(*) DESC LIMIT 15"
    ).fetchall()

    c_lines = "\n".join(f"  {r[0]}: {r[1]}" for r in by_contract)
    s_lines = "\n".join(f"  {r[0]}: {r[1]}" for r in by_source)
    v_lines = "\n".join(f"  {r[0]}: {r[1]}" for r in by_city)

    tg_send(
        f"📊 <b>إحصائيات عروض الشغل</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📦 المجموع: {total}\n"
        f"📤 المرسلة: {sent}\n"
        f"📥 الانتظار: {total - sent}\n\n"
        f"📋 <b>حسب العقد:</b>\n{c_lines}\n\n"
        f"📡 <b>حسب المصدر:</b>\n{s_lines}\n\n"
        f"📍 <b>حسب المدينة:</b>\n{v_lines}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
