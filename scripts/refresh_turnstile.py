#!/usr/bin/env python3
"""Refresh the turnstile_verified cookie in fotmob_credentials.py.

Run this whenever the scraper reports TURNSTILE_REQUIRED:
    python3 scripts/refresh_turnstile.py

Requirements: Chrome must be open and you must have visited fotmob.com recently.
The scraper hot-reloads fotmob_credentials.py on every request — no restart needed.
"""
import sys
import re
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent


def get_chrome_turnstile() -> str:
    try:
        import browser_cookie3
        for domain in ("www.fotmob.com", ".fotmob.com", "fotmob.com"):
            cj = browser_cookie3.chrome(domain_name=domain)
            tv = {c.name: c.value for c in cj}.get("turnstile_verified", "")
            if tv:
                return tv
    except Exception as exc:
        print(f"browser-cookie3 error: {exc}")
    return ""


def check_age(tv: str) -> str:
    parts = tv.split(".")
    if len(parts) == 3:
        try:
            age = int(time.time()) - int(parts[1])
            remaining = 3600 - age
            if remaining > 0:
                return f"VALID (~{remaining // 60}m {remaining % 60}s remaining)"
            return f"EXPIRED ({-remaining // 60}m ago)"
        except ValueError:
            pass
    return "unknown format"


def update_credentials_py(tv: str) -> bool:
    path = ROOT / "fotmob_credentials.py"
    if not path.exists():
        print(f"Not found: {path}")
        return False
    text = path.read_text()
    updated = re.sub(
        r"('turnstile_verified'\s*:\s*')[^']+(')",
        rf"\g<1>{tv}\g<2>",
        text,
    )
    if updated == text:
        print("fotmob_credentials.py: turnstile_verified key not found — adding it")
        updated = text.rstrip().rstrip("}") + f"\n    'turnstile_verified': '{tv}',\n}}\n"
    path.write_text(updated)
    print(f"Updated fotmob_credentials.py")
    return True


def main():
    print("Reading turnstile_verified from Chrome...")
    tv = get_chrome_turnstile()

    if not tv:
        print(
            "\nERROR: Could not read turnstile_verified from Chrome.\n"
            "  1. Open Chrome and visit https://www.fotmob.com\n"
            "  2. Wait for the page to fully load\n"
            "  3. Run this script again\n"
        )
        sys.exit(1)

    status = check_age(tv)
    print(f"Found: {tv[:60]}...")
    print(f"Status: {status}")

    if "EXPIRED" in status:
        print(
            "\nWARNING: Cookie is expired. Visit https://www.fotmob.com in Chrome,\n"
            "wait for the page to load, then run this script again.\n"
        )
        sys.exit(1)

    print()
    update_credentials_py(tv)
    print(
        "\nDone. The running scraper will pick up the change automatically.\n"
        "No restart needed.\n"
    )


if __name__ == "__main__":
    main()
