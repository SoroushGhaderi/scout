#!/usr/bin/env python3
"""Check logging style conventions.

Rules:
1. No emoji in runtime logger messages.
2. Status-like logger messages should use bracket tags such as:
   [OK], [WARN], [ERROR], [INFO], [DEBUG], [NEXT], [STATS], [CANCELLED].

Telegram/email formatting modules are excluded.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHECK_DIRS = [ROOT / "src", ROOT / "scripts", ROOT / "config"]
EXCLUDE_PATH_PARTS = {"metrics_alerts.py", "alerting.py"}

LOGGER_CALL_RE = re.compile(
    r"""logger\.(?:debug|info|warning|error|critical|exception)\(\s*(?:f)?["']([^"']*)["']""",
    re.MULTILINE,
)
EMOJI_RE = re.compile(r"[✅❌⚠️📊💡✓✗⚡🏠ℹ️🚨⏭️⏱️🗄️☁️]")
STATUS_WORD_RE = re.compile(r"\b(OK|WARN|WARNING|ERROR|INFO|DEBUG|SUCCESS|FAILED|NEXT|STATS)\b")


def should_exclude(path: Path) -> bool:
    return any(part in path.name for part in EXCLUDE_PATH_PARTS)


def iter_python_files() -> list[Path]:
    files: list[Path] = []
    for base in CHECK_DIRS:
        if not base.exists():
            continue
        files.extend(sorted(base.rglob("*.py")))
    return files


def main() -> int:
    violations: list[str] = []

    for path in iter_python_files():
        if should_exclude(path):
            continue

        content = path.read_text(encoding="utf-8")
        for match in LOGGER_CALL_RE.finditer(content):
            msg = match.group(1)
            if EMOJI_RE.search(msg):
                violations.append(f"{path}: emoji in logger message -> {msg}")
                continue
            normalized = msg.replace("\\n", "").lstrip()
            if STATUS_WORD_RE.search(msg) and not normalized.startswith("["):
                violations.append(f"{path}: status message missing [] tag -> {msg}")

    if violations:
        print("[ERROR] Logging style violations found:")
        for item in violations:
            print(f"  - {item}")
        return 1

    print("[OK] Logging style check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
