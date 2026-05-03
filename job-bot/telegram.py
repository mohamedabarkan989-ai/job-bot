import asyncio
import logging
import random
import time

import aiohttp

from config import TG_TOKEN, TG_CHAT, ICONS

log = logging.getLogger(__name__)

_API   = f"https://api.telegram.org/bot{TG_TOKEN}"
_sent  = 0
_LIMIT = 4000  # bytes, safe under TG 4096


# ── delivery ────────────────────────────────────────────────

async def _post_async(session: aiohttp.ClientSession, text: str, retries: int = 3) -> bool:
    global _sent
    for attempt in range(retries):
        try:
            async with session.post(
                f"{_API}/sendMessage",
                json={"chat_id": TG_CHAT, "text": text,
                      "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 429:
                    data = await r.json()
                    wait = data.get("parameters", {}).get("retry_after", 30 * (attempt + 1))
                    log.warning(f"[TG] 429 — waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if r.status == 200:
                    _sent += 1
                    # adaptive delay: faster early, slower after burst
                    await asyncio.sleep(random.uniform(0.3, 0.8) if _sent % 20 else 30)
                    return True
                body = await r.text()
                log.error(f"[TG] {r.status}: {body[:120]}")
                return False
        except Exception as e:
            log.error(f"[TG] attempt {attempt+1}/{retries}: {e}")
            await asyncio.sleep(5 * (attempt + 1))
    return False


async def _send_batch_async(messages: list[str]) -> list[bool]:
    """Send messages concurrently — max 3 in-flight to respect TG rate limits."""
    sem = asyncio.Semaphore(3)
    async with aiohttp.ClientSession() as session:
        async def guarded(text):
            async with sem:
                return await _post_async(session, text)
        return await asyncio.gather(*[guarded(m) for m in messages])


def _post(text: str) -> bool:
    return asyncio.run(_send_batch_async([text]))[0]


# ── formatting ──────────────────────────────────────────────

def format_job(j: dict) -> str:
    icon    = ICONS.get(j.get("contract", "?"), "⚫")
    salary  = f"\n💰 {j['salary']}" if j.get("salary") else ""
    sector  = f"\n🏷️ {j['sector']}"  if j.get("sector")  else ""
    score   = f" · ⭐{j['score']}"   if j.get("score")   else ""
    return (
        f"{icon} <b>{j['title']}</b>{score}\n"
        f"🏢 {j['company']}\n"
        f"📍 {j['location']}\n"
        f"📋 {j.get('contract','?')}"
        f"{salary}{sector}\n"
        f"🔗 <a href=\"{j['url']}\">Voir l'offre</a>\n"
        f"📡 {j['source']}\n"
        f"━━━━━━━━━━━━━━"
    )


def _build_batches(jobs: list[dict]) -> list[tuple[str, list[dict]]]:
    """Group jobs into ≤_LIMIT byte messages."""
    batches, buf, buf_rows = [], "", []
    for j in jobs:
        chunk = format_job(j) + "\n"
        if len((buf + chunk).encode()) > _LIMIT:
            if buf.strip():
                batches.append((buf, buf_rows))
            buf, buf_rows = chunk, [j]
        else:
            buf += chunk
            buf_rows.append(j)
    if buf.strip():
        batches.append((buf, buf_rows))
    return batches


# ── public API ──────────────────────────────────────────────

def send_jobs(jobs: list[dict]) -> list[str]:
    if not jobs:
        _post("📭 ما لقيناش عروض جداد اليوم.")
        return []

    batches  = _build_batches(jobs)
    messages = [
        f"📦 <b>{len(rows)} عروض</b>\n━━━━━━━━━━━━━━\n{buf}"
        for buf, rows in batches
    ]
    results  = asyncio.run(_send_batch_async(messages))

    sent_uids = []
    for ok, (_, rows) in zip(results, batches):
        if ok:
            sent_uids.extend(r["uid"] for r in rows)

    log.info(f"[TG] {len(sent_uids)}/{len(jobs)} sent in {len(batches)} batches")
    return sent_uids


def send_stats(s: dict) -> None:
    c   = "\n".join(f"  {k}: {v}" for k, v in s["by_contract"].items())
    src = "\n".join(f"  {k}: {v}" for k, v in s["by_source"].items())
    _post(
        f"📊 <b>إحصائيات</b>\n"
        f"📦 {s['total']} | 📤 {s['sent']} | ⏳ {s['total']-s['sent']}\n\n"
        f"📋 <b>العقود:</b>\n{c}\n\n"
        f"📡 <b>المصادر:</b>\n{src}"
    )


def validate() -> bool:  # pragma: no cover
    async def _check():  # pragma: no cover
        async with aiohttp.ClientSession() as s:  # pragma: no cover
            async with s.get(f"{_API}/getMe", timeout=aiohttp.ClientTimeout(total=10)) as r:  # pragma: no cover
                data = await r.json()  # pragma: no cover
                return r.status == 200 and data.get("ok", False)  # pragma: no cover
    try:
        return asyncio.run(_check())
    except Exception:
        return False
