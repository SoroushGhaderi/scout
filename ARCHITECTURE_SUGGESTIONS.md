# Architecture Suggestions

> Updated: 2026-03-25
> Scope: FotMob-only medallion pipeline

This document reflects the current target architecture for Scout after the layer-separation cleanup.

## 1. Product Scope

Scout is now a FotMob-only pipeline.

- All scraping, raw storage, transformations, and warehouse objects are defined around FotMob only
- Any future source should be added as a separate, explicit design decision rather than mixed into the current flow

## 2. Layer Model

### Bronze

Bronze is the raw ingestion layer.

- Input: raw FotMob API responses
- Storage: local filesystem under `data/fotmob/`
- Backup: optional S3 archive upload
- ClickHouse representation: `fotmob.bronze_*` tables
- Responsibility:
  `scripts/scrape_fotmob.py`
  `scripts/load_clickhouse.py`
  `src/storage/bronze/`
  `src/processors/bronze/`

### Silver

Silver is the cleaned relational layer inside ClickHouse.

- Input: ClickHouse Bronze tables
- Storage: ClickHouse only
- ClickHouse representation: `fotmob.silver_*` views
- Responsibility:
  `scripts/process_silver.py`
  `src/storage/silver/`
  `src/processors/silver/`

### Gold

Gold is the analytics-ready aggregation layer inside ClickHouse.

- Input: ClickHouse Silver views
- Storage: ClickHouse only
- ClickHouse representation: `fotmob.gold_*` tables
- Responsibility:
  `scripts/process_gold.py`
  `src/storage/gold/`
  `src/processors/gold/`

## 3. Naming Rules

These rules should be treated as required, not optional.

- Bronze ClickHouse tables must be named `bronze_*`
- Silver ClickHouse views must be named `silver_*`
- Gold ClickHouse tables must be named `gold_*`
- No new warehouse object should use bare names like `general`, `player`, or `timeline`
- Bare logical names are acceptable only as internal Python mapping keys before conversion to physical table names
- Python entry points should use explicit layer-aware names where possible, for example `FotMobBronzeStorage` and `FotMobBronzeMatchProcessor`

## 4. Storage Rules

- Only Bronze has a local storage path in configuration
- Silver and Gold are not filesystem layers in this project
- `config.yaml` should expose `fotmob.storage.bronze_path` only
- Any new `silver_path` or `gold_path` setting should be rejected unless the architecture changes intentionally

## 5. ClickHouse Rules

### Bronze

- All Bronze tables must use `ReplacingMergeTree(inserted_at)`
- All Bronze tables must include an `inserted_at DateTime DEFAULT now()` column
- Bronze deduplication is operationally completed with `OPTIMIZE TABLE ... FINAL DEDUPLICATE`
- Bronze tables should remain append-friendly and re-runnable

### Silver

- Silver objects should remain deterministic transformations from Bronze
- Silver objects should be created as `silver_*` views unless there is a deliberate reason to materialize them differently

### Gold

- Gold objects should be explicitly business-facing or analytics-facing aggregates
- Gold objects must read from `silver_*`, not directly from raw Bronze tables

## 6. Script Boundaries

Scripts should map to one responsibility each.

- `scripts/scrape_fotmob.py`: scrape raw FotMob data into Bronze filesystem storage
- `scripts/load_clickhouse.py`: parse Bronze files and insert into ClickHouse Bronze tables
- `scripts/process_silver.py`: create or refresh Silver views
- `scripts/process_gold.py`: create or refresh Gold tables
- `scripts/setup_clickhouse_bronze.py`: create Bronze schema only
- `scripts/setup_clickhouse_silver.py`: create Silver schema only
- `scripts/setup_clickhouse_gold.py`: create Gold schema only
- `scripts/setup_clickhouse.py`: convenience wrapper that runs the three layer setup scripts in order
- `scripts/pipeline.py`: orchestration wrapper, not a place to hide layer-specific logic

## 7. Recommended End-to-End Flow

```text
FotMob API
  -> local Bronze files
  -> ClickHouse Bronze tables
  -> ClickHouse Silver views
  -> ClickHouse Gold tables
```

Operational order:

1. Create schema
2. Scrape FotMob raw data into Bronze files
3. Load Bronze files into ClickHouse Bronze tables
4. Build Silver views
5. Build Gold tables
6. Run Bronze table optimization when needed

## 8. Guardrails For Future Changes

- Do not reintroduce mixed-source abstractions unless the repo truly becomes multi-source again
- Do not add generic warehouse table names without a layer prefix
- Do not let Silver or Gold depend on local filesystem artifacts
- Do not bypass Bronze and load raw scraper responses directly into Silver or Gold
- Do not add temporary references to other data sources back into docs or scripts

## 9. Current Cleanup Status

The architecture is now aligned with these rules in the main runtime paths:

- FotMob-only scope is enforced in docs and active scripts
- Bronze config is the only filesystem-backed layer config
- Bronze setup, Silver setup, and Gold setup have separate script entry points
- Bronze warehouse tables are layer-prefixed and use `ReplacingMergeTree(inserted_at)`
- Silver and Gold warehouse objects are layer-prefixed

Any future changes should preserve this contract.
