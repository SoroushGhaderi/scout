#!/usr/bin/env python3
"""
Quick config updater for FotMob credentials.

Usage:
    python scripts/update_fotmob_config.py

Then paste your cookies dict and x-mas token when prompted.
"""

import json
import os
import re
from pathlib import Path


def parse_python_dict(text: str) -> dict:
    """Parse Python dict string to JSON."""
    # Replace single quotes with double quotes for JSON
    # Handle nested quotes carefully
    text = text.strip()
    if text.startswith('{') and text.endswith('}'):
        # Use ast.literal_eval for safe parsing
        import ast
        return ast.literal_eval(text)
    return {}


def update_env_file(x_mas_token: str, cookies_json: str):
    """Update .env file with new credentials."""
    env_path = Path(__file__).parent.parent / '.env'
    
    if not env_path.exists():
        print(f"Error: .env file not found at {env_path}")
        return False
    
    with open(env_path, 'r') as f:
        content = f.read()
    
    # Update FOTMOB_X_MAS_TOKEN
    content = re.sub(
        r"FOTMOB_X_MAS_TOKEN='[^']*'",
        f"FOTMOB_X_MAS_TOKEN='{x_mas_token}'",
        content
    )
    
    # Update FOTMOB_COOKIES
    content = re.sub(
        r"FOTMOB_COOKIES='[^']*'",
        f"FOTMOB_COOKIES='{cookies_json}'",
        content
    )
    
    with open(env_path, 'w') as f:
        f.write(content)
    
    return True


def main():
    print("=" * 60)
    print("FotMob Config Updater")
    print("=" * 60)
    print()
    
    # Get x-mas token
    print("Paste your x-mas token (from headers['x-mas']):")
    print("(Press Enter when done)")
    x_mas_lines = []
    while True:
        line = input()
        if not line:
            break
        x_mas_lines.append(line)
    x_mas_token = ''.join(x_mas_lines).strip().strip("'\"")
    
    print()
    print("Paste your cookies dict (e.g., {'key': 'value', ...}):")
    print("(Type 'END' on a new line when done)")
    cookies_lines = []
    while True:
        line = input()
        if line.strip() == 'END':
            break
        cookies_lines.append(line)
    cookies_text = '\n'.join(cookies_lines)
    
    try:
        cookies_dict = parse_python_dict(cookies_text)
        if not cookies_dict:
            print("Error: Could not parse cookies dict")
            return
        
        cookies_json = json.dumps(cookies_dict, separators=(',', ': '))
        
        print()
        print(f"Parsed {len(cookies_dict)} cookies")
        print(f"x-mas token length: {len(x_mas_token)}")
        print()
        
        if update_env_file(x_mas_token, cookies_json):
            print("✓ .env file updated successfully!")
            
            # Check token freshness
            import base64
            try:
                decoded = json.loads(base64.b64decode(x_mas_token))
                code = decoded['body']['code']
                from datetime import datetime, timezone
                ts = datetime.fromtimestamp(code / 1000, tz=timezone.utc)
                now = datetime.now(timezone.utc)
                age_hours = (now - ts).total_seconds() / 3600
                print(f"✓ Token created: {ts.strftime('%Y-%m-%d %H:%M:%S')} UTC ({age_hours:.1f} hours ago)")
            except:
                pass
        else:
            print("✗ Failed to update .env file")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()
