#!/usr/bin/env python3
"""Refresh the turnstile_verified cookie in credentials.json.

Run this whenever the scraper reports TURNSTILE_REQUIRED:
    python3 scripts/refresh_turnstile.py

Requirements: Chrome must be open and you must have visited fotmob.com recently.
The scraper hot-reloads credentials.json on every request — no restart needed.
"""
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

ROOT = Path(__file__).parent.parent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_chrome_turnstile() -> str:
    try:
        import browser_cookie3
        for domain in ("www.fotmob.com", ".fotmob.com", "fotmob.com"):
            cj = browser_cookie3.chrome(domain_name=domain)
            tv = {c.name: c.value for c in cj}.get("turnstile_verified", "")
            if tv:
                return tv
    except Exception:
        pass
    return ""


def get_turnstile_age_seconds(turnstile_value: str) -> Optional[int]:
    """Get age of turnstile token in seconds. Returns None if format is invalid."""
    parts = turnstile_value.split(".")
    if len(parts) == 3:
        try:
            created_time = int(parts[1])
            age = int(time.time()) - created_time
            return age
        except ValueError:
            pass
    return None


def get_turnstile_age_info(turnstile_value: str) -> Tuple[Optional[int], str]:
    """Get turnstile age in seconds and human-readable status.
    
    Returns:
        Tuple of (age_seconds, status_string)
    """
    age_seconds = get_turnstile_age_seconds(turnstile_value)
    
    if age_seconds is None:
        return None, "unknown format"
    
    if age_seconds < 0:
        return age_seconds, f"future ({-age_seconds}s ahead)"
    
    remaining = 3600 - age_seconds
    if remaining > 0:
        minutes = remaining // 60
        seconds = remaining % 60
        return age_seconds, f"valid ({minutes}m {seconds}s remaining)"
    else:
        minutes_ago = (-remaining) // 60
        return age_seconds, f"expired ({minutes_ago}m ago)"


def check_age(tv: str) -> str:
    _, status = get_turnstile_age_info(tv)
    return status


def update_credentials_json(tv: str) -> bool:
    path = ROOT / "credentials.json"
    if not path.exists():
        logger.error(f"credentials.json not found: {path}")
        return False
    
    try:
        with open(path, "r") as f:
            data = json.load(f)
        
        if "cookies" not in data:
            data["cookies"] = {}
        
        data["cookies"]["turnstile_verified"] = tv
        
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        
        logger.info("Updated credentials.json with new turnstile token")
        return True
    except Exception as e:
        logger.error(f"Error updating credentials.json: {e}")
        return False


def refresh_if_needed(max_age_seconds: int = 1800) -> Tuple[bool, Optional[str]]:
    """Check turnstile age and refresh if needed.
    
    Args:
        max_age_seconds: Maximum age before refresh (default 30 minutes = 1800 seconds)
        
    Returns:
        Tuple of (was_refreshed, message)
    """
    creds_path = ROOT / "credentials.json"
    if not creds_path.exists():
        return False, "credentials.json not found"
    
    try:
        with open(creds_path, "r") as f:
            data = json.load(f)
        
        current_tv = data.get("cookies", {}).get("turnstile_verified", "")
        if not current_tv:
            new_tv = get_chrome_turnstile()
            if new_tv and update_credentials_json(new_tv):
                age, status = get_turnstile_age_info(new_tv)
                return True, f"Refreshed (no existing token), age: {status}"
            return False, "No turnstile token found"
        
        age_seconds, status = get_turnstile_age_info(current_tv)
        
        if age_seconds is not None and age_seconds >= max_age_seconds:
            new_tv = get_chrome_turnstile()
            if new_tv:
                if update_credentials_json(new_tv):
                    new_age, new_status = get_turnstile_age_info(new_tv)
                    return True, f"Refreshed (age={age_seconds}s>{max_age_seconds}s), new age: {new_status}"
            # Auto-refresh via Chrome cookies failed (e.g. no D-Bus in Docker).
            # The existing token may still be usable — report its actual validity.
            return False, f"Token stale (age={age_seconds}s>={max_age_seconds}s, {status}), auto-refresh unavailable (run refresh_turnstile.py manually)"
        
        return False, f"Token valid (age={age_seconds}s<{max_age_seconds}s), {status}"
        
    except Exception as e:
        return False, f"Error: {str(e)}"


def main():
    logger.info("Reading turnstile_verified from Chrome...")
    tv = get_chrome_turnstile()

    if not tv:
        logger.error(
            "Could not read turnstile_verified from Chrome. "
            "1. Open Chrome and visit https://www.fotmob.com, "
            "2. Wait for the page to fully load, "
            "3. Run this script again"
        )
        sys.exit(1)

    status = check_age(tv)
    logger.info(f"Found turnstile: {tv[:60]}...")
    logger.info(f"Status: {status}")

    if "EXPIRED" in status:
        logger.warning(
            "Cookie is expired. Visit https://www.fotmob.com in Chrome, "
            "wait for the page to load, then run this script again"
        )
        sys.exit(1)

    update_credentials_json(tv)
    logger.info("Done. The running scraper will pick up the change automatically.")


if __name__ == "__main__":
    main()
