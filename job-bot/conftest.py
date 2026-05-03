import os, sys
os.environ.setdefault("TG_TOKEN",      "test_token_123")
os.environ.setdefault("TG_CHAT",       "-100123456789")
os.environ.setdefault("SUPABASE_URL",  "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY",  "test_key_abc")
sys.path.insert(0, os.path.dirname(__file__))
