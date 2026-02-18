#!/usr/bin/env python3
"""Refresh the turnstile_verified cookie in credentials.json.

Run this whenever the scraper reports TURNSTILE_REQUIRED:
    python3 scripts/refresh_turnstile.py

Requirements: Chrome must be open and you must have visited fotmob.com recently.
The scraper hot-reloads credentials.json on every request — no restart needed.
"""
import json
import sys
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


def update_credentials_json(tv: str) -> bool:
    path = ROOT / "credentials.json"
    if not path.exists():
        print(f"Not found: {path}")
        return False
    
    try:
        with open(path, "r") as f:
            data = json.load(f)
        
        if "cookies" not in data:
            data["cookies"] = {}
        
        data["cookies"]["turnstile_verified"] = tv
        
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        
        print(f"Updated credentials.json")
        return True
    except Exception as e:
        print(f"Error updating credentials.json: {e}")
        return False


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
    update_credentials_json(tv)
    print(
        "\nDone. The running scraper will pick up the change automatically.\n"
        "No restart needed.\n"
    )


if __name__ == "__main__":
    main()
