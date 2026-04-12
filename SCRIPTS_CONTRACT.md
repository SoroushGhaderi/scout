# Scripts Contract

This file defines the stable command surface for Scout.

## Golden Path Commands

```bash
# 1) setup
python scripts/orchestration/setup_clickhouse.py

# 2) bronze scrape
python scripts/bronze/scrape_fotmob.py 20251208

# 3) bronze load
python scripts/bronze/load_clickhouse.py --date 20251208

# 4) silver
python scripts/silver/load_clickhouse.py

# 5) gold
python scripts/gold/load_clickhouse_scenarios.py

# 6) quality gates
python scripts/quality/check_bronze_to_silver_reconciliation.py --strict

# or full orchestration
python scripts/orchestration/pipeline.py 20251208
```

## Stability Rules

1. Layer entrypoints in `scripts/bronze|silver|gold|orchestration|quality` are canonical.
2. Root-level layer scripts are removed.
3. Utility scripts (`ensure_directories`, `health_check`, `refresh_turnstile`) are allowed at root.
4. Do not add new root-level layer scripts.
5. Any new script must be documented in `scripts/README.md` and architecture docs when command surface changes.
