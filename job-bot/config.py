import os
from dotenv import load_dotenv
load_dotenv()

TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT  = os.environ.get("TG_CHAT",  "")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

MAX_SEND   = int(os.environ.get("MAX_SEND", 50))
TREND_DAYS = int(os.environ.get("TREND_DAYS", 7))

CITIES = ["Casablanca","Rabat","Marrakech","Tanger","Fès","Agadir","Oujda","Kénitra"]

KEYWORDS = [
    "stage","stagiaire","PFE","recrutement","CDI","CDD",
    "offre emploi","développeur","ingénieur","data",
    "عروض العمل","فرص شغل",
]

CONTRACT_MAP = {
    "cdi":"CDI","permanent":"CDI","cdd":"CDD","temporaire":"CDD",
    "civp":"CIVP","stage":"STAGE","internship":"STAGE","pfe":"STAGE",
    "freelance":"FREELANCE","saison":"SAISON",
}

ICONS = {"CDI":"🟢","CDD":"🟡","CIVP":"🔵","STAGE":"🟣","FREELANCE":"🟠","SAISON":"⚪","?":"⚫"}

FILTER = {"CDI","CDD","CIVP","STAGE"}

RSS_SOURCES = [
    "https://www.rekrute.com/rss/offres.rss",
    "https://www.emploi.ma/rss/jobs.rss",
    "https://www.anapec.org/rss/offres.xml",
    "https://ma.indeed.com/rss?q=emploi&l=Maroc&sort=date",
    "https://www.optioncarriere.ma/emploi.xml",
    "https://ma.talent.com/rss?q=emploi&l=Maroc",
    "https://www.bayt.com/en/rss/morocco-jobs/",
    "https://www.glassdoor.com/feeds/job-listing-feed.rss?locId=72&locT=N",
    "https://www.jobijoba.com/ma/rss.xml",
    "https://www.moovjobs.com/feed/",
]
