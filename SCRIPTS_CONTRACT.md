# Scripts Contract

This file is the source of truth for script-level behavior and code handwriting in Scout.

## 1. Canonical Command Surface

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

## 2. Scope and Intent

1. This contract applies to all Python files under `scripts/`, and to script-oriented helper modules under `src/`.
2. Refactors must preserve behavior unless a functional change is explicitly requested.
3. If a file already follows this contract and is clean, do not rewrite it.

## 3. Handwriting Rules

1. Prefer explicit, descriptive names over short/ambiguous names.
2. Keep one style across modules: naming, function shape, error handling, and logging style.
3. Make control flow obvious and linear; avoid clever one-liners.
4. Separate orchestration from logic: CLI parsing, execution, reporting, and shared helpers should be isolated.

## 4. Naming Conventions

1. Functions and variables: `snake_case`.
2. Constants: `UPPER_SNAKE_CASE` and declared near file top.
3. Classes: `PascalCase`.
4. Private helpers: prefix with `_`.
5. Booleans should read as booleans: `is_`, `has_`, `should_`, `can_`.
6. Use suffixes intentionally:
   1. `_path` for `Path` values.
   2. `_sql` for SQL strings.
   3. `_count` for integer counts.
   4. `_seconds` for duration values.

## 5. Function Design Contract

1. Functions must do one clear job.
2. Target small functions with focused responsibilities.
3. Prefer pure helpers when practical; isolate I/O boundaries.
4. Use early returns for guard clauses.
5. Avoid hidden side effects; side effects must be obvious from name/body.
6. Keep argument lists minimal and meaningful.
7. Do not keep dead code, unused variables, or unused imports.

## 6. Constants and Magic Values

1. No unexplained magic numbers/strings in logic.
2. Extract repeated literals to named constants.
3. Keep related constants grouped and clearly named.
4. Use domain-safe defaults and expose configuration through CLI/env/config modules.

## 7. Imports and Module Layout

1. Import order: standard library, third-party, local modules.
2. Avoid duplicate `sys.path` insertions; guard before inserting.
3. Keep module-level setup minimal and deterministic.
4. Avoid heavyweight runtime work at import time.

## 8. Typing, Docstrings, and Interfaces

1. Public functions require type hints.
2. CLI `main()` and parser helpers must have explicit return types.
3. Write concise docstrings for non-obvious behavior and arguments.
4. Keep docstrings aligned with actual behavior.

## 9. Logging and Error Handling

1. Use structured, consistent logging with clear action/result context.
2. Use `get_logger(__name__)` for module loggers.
3. Log outcomes at boundaries: start, success, failure, summary.
4. Never swallow exceptions silently.
5. Catch specific exceptions where possible.
6. Preserve original error details when re-raising or reporting.

## 10. CLI and Exit Code Contract

1. `0` means success; non-zero means failure.
2. Argument validation errors should return clear messages.
3. `--dry-run` must not mutate state.
4. Script summaries should be deterministic and comparable across runs.

## 11. Data Safety and Non-Regression Rules

1. Do not change schema semantics unintentionally.
2. Do not rename contract fields without coordinated updates.
3. Backward compatibility is default for script arguments and outputs.
4. Any intended behavior change must be explicitly documented in PR/commit notes.

## 12. Formatting and Quality Gates

1. Formatting baseline: `black`.
2. Import sorting baseline: `isort`.
3. Hygiene baseline: `pyflakes` clean.
4. Do not make wide mechanical rewrites unless requested.

## 13. Script Surface Stability Rules

1. Layer entrypoints in `scripts/bronze|silver|gold|orchestration|quality` are canonical.
2. Root-level layer scripts are removed.
3. Utility scripts (`ensure_directories`, `health_check`, `refresh_turnstile`) are allowed at root.
4. Do not add new root-level layer scripts.
5. Any command-surface change must be documented in `scripts/README.md` and architecture docs.

## 14. Reference and Update Policy

1. When asked to "follow the scripts contract," this file is authoritative.
2. When asked to "update the contract," edit this file first, then apply code changes against it.
3. If code and contract conflict, resolve by updating one with explicit intent and note the decision.
