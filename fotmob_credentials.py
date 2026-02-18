# FotMob browser cookies — loaded from credentials.json
#
# This file now reads from credentials.json which should be:
#   1. Mounted via Docker volume: ../credentials.json:/app/credentials.json:ro
#   2. Listed in .gitignore (credentials.json)
#
# Only `turnstile_verified` needs to be kept fresh (expires every ~1 hour).
# All other cookies are long-lived and rarely change.
#
# To refresh after a TURNSTILE_REQUIRED error:
#   1. Open Chrome and visit https://www.fotmob.com (wait for the page to load)
#   2. Run:  python3 scripts/refresh_turnstile.py
#
# The scraper hot-reloads credentials.json on every request — no restart needed.

import json
import os
from pathlib import Path
from typing import Dict

# Try to read from credentials.json (mounted in Docker)
_cred_path = Path(__file__).parent.parent / "credentials.json"

cookies: Dict[str, str] = {}

if _cred_path.exists():
    try:
        with open(_cred_path, "r") as f:
            data = json.load(f)
            cookies = data.get("cookies", {})
    except Exception:
        pass

if not cookies:
    # Fallback: try to read from environment variables (for non-Docker usage)
    cookies = {
        "_ga": os.environ.get("FOTMOB_COOKIE_GA", ""),
        "_cc_id": os.environ.get("FOTMOB_COOKIE_CC_ID", ""),
        "turnstile_verified": os.environ.get("FOTMOB_COOKIE_TURNSTILE", ""),
    }
