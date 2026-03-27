# Scripts Contract

This file defines the stable command surface for Scout.

## Golden Path Commands

```bash
# 1) setup
python scripts/orchestration/setup_clickhouse.py

# 2) bronze scrape
python scripts/bronze/scrape_fotmob.py 20251208

# 3) bronze load
python scripts/bronze/load_clickhouse.py --scraper fotmob --date 20251208

# 4) silver
python scripts/silver/process.py --date 20251208

# 5) gold
python scripts/gold/process.py --date 20251208

# or full orchestration
python scripts/orchestration/pipeline.py 20251208
```

## Stability Rules

1. Layer entrypoints in `scripts/bronze|silver|gold|orchestration` are canonical.
2. Root-level layer scripts are removed.
3. Utility scripts (`ensure_directories`, `health_check`, `refresh_turnstile`) are allowed at root.
4. Do not add new root-level layer scripts.
5. Any new script must be documented in `scripts/README.md` and `SCRIPTS_AUDIT.md`.
