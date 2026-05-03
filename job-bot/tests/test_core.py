import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ══ rss._score ═══════════════════════════════════════════════
def test_score_title_beats_summary():
    from rss import _score
    assert _score({"title":"CDI développeur Casablanca","summary":""}) > \
           _score({"title":"job","summary":"CDI développeur Casablanca"})

def test_score_irrelevant_zero():
    from rss import _score
    assert _score({"title":"recette tajine","summary":"cuisine marocaine"}) == 0

def test_score_city_adds_point():
    from rss import _score
    assert _score({"title":"CDI Casablanca","summary":""}) > \
           _score({"title":"CDI","summary":""})

def test_score_arabic_keyword():
    from rss import _score
    assert _score({"title":"عروض العمل بالمغرب","summary":""}) > 0

def test_score_multikeyword_accumulates():
    from rss import _score
    assert _score({"title":"CDI ingénieur data Casablanca","summary":""}) >= 6

# ══ rss._norm_contract ═══════════════════════════════════════
def test_norm_cdi():
    from rss import _norm_contract
    assert _norm_contract("Contrat CDI temps plein") == "CDI"

def test_norm_stage():
    from rss import _norm_contract
    assert _norm_contract("Stage PFE ingénieur") == "STAGE"

def test_norm_unknown():
    from rss import _norm_contract
    assert _norm_contract("contrat bizarre") == "?"

def test_norm_empty():
    from rss import _norm_contract
    assert _norm_contract("") == "?"

def test_norm_case_insensitive():
    from rss import _norm_contract
    assert _norm_contract("FREELANCE consultant") == "FREELANCE"

# ══ rss._uid ════════════════════════════════════════════════
def test_uid_deterministic():
    from rss import _uid
    assert _uid("http://a.com/1","Job A") == _uid("http://a.com/1","Job A")

def test_uid_different():
    from rss import _uid
    assert _uid("http://a.com/1","Job A") != _uid("http://a.com/2","Job B")

def test_uid_length():
    from rss import _uid
    assert len(_uid("http://x.com","title")) == 32

# ══ telegram.format_job ══════════════════════════════════════
def _job(**kw):
    base = {"title":"Dev","company":"Co","location":"Rabat","contract":"CDI",
            "url":"http://x.com","source":"S","salary":"","sector":""}
    return {**base, **kw}

def test_format_contains_title():
    from telegram import format_job
    assert "Dev Python" in format_job(_job(title="Dev Python"))

def test_format_icon_cdi():
    from telegram import format_job
    assert "🟢" in format_job(_job(contract="CDI"))

def test_format_icon_stage():
    from telegram import format_job
    assert "🟣" in format_job(_job(contract="STAGE"))

def test_format_score_shown():
    from telegram import format_job
    assert "⭐5" in format_job(_job(score=5))

def test_format_no_score():
    from telegram import format_job
    assert "⭐" not in format_job(_job())

def test_format_salary_shown():
    from telegram import format_job
    assert "💰" in format_job(_job(salary="8000 MAD"))

def test_format_salary_hidden_when_empty():
    from telegram import format_job
    assert "💰" not in format_job(_job(salary=""))

# ══ telegram._build_batches ═══════════════════════════════════
def _jobs(n):
    return [{"uid":str(i),"title":f"Job {i}","company":"Co","location":"Rabat",
             "contract":"CDI","url":"http://x.com","source":"S",
             "salary":"","sector":"","score":1} for i in range(n)]

def test_batches_split(monkeypatch):
    import telegram as tg
    monkeypatch.setattr(tg, "_LIMIT", 200)
    batches = tg._build_batches(_jobs(10))
    assert len(batches) > 1
    for buf, _ in batches:
        assert len(buf.encode()) <= 200

def test_batches_single():
    import telegram as tg
    assert len(tg._build_batches(_jobs(1))) == 1

def test_batches_empty():
    import telegram as tg
    assert tg._build_batches([]) == []

def test_batches_uids_preserved():
    import telegram as tg
    jobs = _jobs(5)
    batches = tg._build_batches(jobs)
    all_uids = [r["uid"] for _, rows in batches for r in rows]
    assert sorted(all_uids) == sorted(j["uid"] for j in jobs)

# ══ db._retry ════════════════════════════════════════════════
def test_retry_returns_default_on_failure():
    import db
    calls = {"n": 0}
    @db._retry(retries=2, backoff=0, default="fallback")
    def bad():
        calls["n"] += 1
        raise RuntimeError("boom")
    assert bad() == "fallback"
    assert calls["n"] == 2

def test_retry_succeeds_on_first():
    import db
    @db._retry(retries=3, backoff=0)
    def good():
        return 42
    assert good() == 42

def test_retry_recovers_on_second_attempt():
    import db
    calls = {"n": 0}
    @db._retry(retries=3, backoff=0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("first fail")
        return "ok"
    assert flaky() == "ok"
    assert calls["n"] == 2

# ══ trends.rss_urls_from_trends ══════════════════════════════
def test_trend_urls_count():
    from trends import rss_urls_from_trends
    assert len(rss_urls_from_trends(["dev","data"])) == 2

def test_trend_urls_encoded():
    from trends import rss_urls_from_trends
    urls = rss_urls_from_trends(["data analyst"])
    assert "data+analyst" in urls[0] or "data%20analyst" in urls[0]

def test_trend_urls_empty():
    from trends import rss_urls_from_trends
    assert rss_urls_from_trends([]) == []
