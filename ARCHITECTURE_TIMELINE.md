# Scout Data Pipeline Architecture & Timeline

## Overview

This document describes the data flow architecture and timeline for the Scout data pipeline, showing when FotMob and AIScore data are scraped and loaded into ClickHouse.

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
├─────────────────────────────────────────────────────────────────────────┤
│  FotMob API                    AIScore Web Scraping                    │
│  (REST API)                    (Browser Automation)                     │
└────────────┬──────────────────────────────┬────────────────────────────┘
             │                               │
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw Storage)                         │
├─────────────────────────────────────────────────────────────────────────┤
│  data/fotmob/                  data/aiscore/                             │
│  ├── matches/                  ├── matches/                              │
│  │   └── YYYYMMDD/             │   └── YYYYMMDD/                         │
│  │       └── match_*.json      │       └── match_*.json                  │
│  ├── lineage/                  ├── lineage/                             │
│  │   └── YYYYMMDD/             │   └── YYYYMMDD/                         │
│  │       └── lineage.json      │       └── lineage.json                  │
│  └── daily_listings/            └── daily_listings/                      │
│      └── YYYYMMDD/                 └── YYYYMMDD/                         │
│          └── matches.json              └── matches.json                 │
│                                                                          │
│  • Raw JSON files (unprocessed)                                          │
│  • Compressed to TAR archives                                          │
│  • Data lineage tracking                                                │
└────────────┬──────────────────────────────┬────────────────────────────┘
             │                               │
             │                               │
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    CLICKHOUSE DATA WAREHOUSE                            │
├─────────────────────────────────────────────────────────────────────────┤
│  Database: fotmob              Database: aiscore                       │
│  ├── general                   ├── matches                             │
│  ├── timeline                  ├── odds_1x2                            │
│  ├── venue                     ├── odds_asian_handicap                 │
│  ├── player                    ├── odds_over_under                     │
│  ├── shotmap                   └── ...                                 │
│  └── ...                                                                 │
│                                                                          │
│  • Analytics-ready structured data                                      │
│  • Partitioned by date for performance                                  │
│  • Automatic deduplication (AIScore: ReplacingMergeTree)                │
│  • Post-insert optimization (OPTIMIZE TABLE FINAL)                      │
│  • Data quality checks                                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Timeline: Per-Date Mode

When processing a **single date** or **date range**, the pipeline executes in this sequence:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  TIMELINE: Processing Date 2025-11-15                                    │
└─────────────────────────────────────────────────────────────────────────┘

T0: Start Pipeline
│
├─► T1: FotMob Bronze Scraping
│   │
│   ├─► Fetch match IDs from FotMob API
│   ├─► Scrape match details (parallel/sequential)
│   ├─► Save to: data/fotmob/matches/20251115/match_*.json
│   ├─► Record data lineage
│   └─► ✅ FotMob Bronze Complete
│
├─► T2: AIScore Bronze Scraping (starts after FotMob bronze completes)
│   │
│   ├─► Step 2.1: Link Scraping
│   │   ├─► Navigate to AIScore date page
│   │   ├─► Scroll and collect match links
│   │   └─► Save match links to bronze storage
│   │
│   ├─► Step 2.2: Odds Scraping
│   │   ├─► Load match links from bronze
│   │   ├─► Scrape odds for each match
│   │   └─► Save to: data/aiscore/matches/20251115/match_*.json
│   │
│   ├─► Record data lineage
│   └─► ✅ AIScore Bronze Complete
│
├─► T3: FotMob ClickHouse Loading (starts after AIScore bronze completes)
│   │
│   ├─► Load JSON files from: data/fotmob/matches/20251115/
│   ├─► Process and transform data
│   ├─► Insert into ClickHouse tables (fotmob database)
│   ├─► Optimize tables (OPTIMIZE TABLE FINAL)
│   ├─► Record data lineage
│   └─► ✅ FotMob ClickHouse Complete
│
└─► T4: AIScore ClickHouse Loading (starts after FotMob ClickHouse completes)
    │
    ├─► Load JSON files from: data/aiscore/matches/20251115/
    ├─► Process and transform data
    ├─► Insert into ClickHouse tables (aiscore database)
    ├─► Optimize tables (OPTIMIZE TABLE FINAL) - deduplication via ReplacingMergeTree
    ├─► Record data lineage
    └─► ✅ AIScore ClickHouse Complete

T5: Pipeline Complete for Date 2025-11-15
```

### Key Points:

1. **FotMob Bronze** runs first (T1)
2. **AIScore Bronze** runs after FotMob bronze completes (T2)
3. **FotMob ClickHouse** loads after AIScore bronze completes (T3)
4. **AIScore ClickHouse** loads after FotMob ClickHouse completes (T4)

**Answer to your question:** FotMob data is stored in ClickHouse **AFTER** AIScore bronze layer finishes, not before.

## Timeline: Monthly Mode

When processing an **entire month**, the pipeline batches operations:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  TIMELINE: Processing Month 2025-11                                       │
└─────────────────────────────────────────────────────────────────────────┘

T0: Start Pipeline
│
├─► T1: FotMob Bronze Scraping (All Dates)
│   │
│   ├─► Date 2025-11-01: Scrape → Save to Bronze
│   ├─► Date 2025-11-02: Scrape → Save to Bronze
│   ├─► Date 2025-11-03: Scrape → Save to Bronze
│   ├─► ... (continues for all dates in month)
│   └─► ✅ All FotMob Bronze Complete
│
├─► T2: AIScore Bronze Scraping (All Dates)
│   │
│   ├─► Date 2025-11-01: Links → Odds → Save to Bronze
│   ├─► Date 2025-11-02: Links → Odds → Save to Bronze
│   ├─► Date 2025-11-03: Links → Odds → Save to Bronze
│   ├─► ... (continues for all dates in month)
│   └─► ✅ All AIScore Bronze Complete
│
├─► T3: FotMob ClickHouse Loading (All Dates - Batch)
│   │
│   ├─► Load all dates from: data/fotmob/matches/202511*/
│   ├─► Process and transform all data
│   ├─► Insert into ClickHouse (with optimization after each date)
│   └─► ✅ All FotMob ClickHouse Complete
│
└─► T4: AIScore ClickHouse Loading (All Dates - Batch)
    │
    ├─► Load all dates from: data/aiscore/matches/202511*/
    ├─► Process and transform all data
    ├─► Insert into ClickHouse (with optimization and deduplication)
    └─► ✅ All AIScore ClickHouse Complete

T5: Pipeline Complete for Month 2025-11
```

### Key Points:

1. **All FotMob Bronze** completes first (T1)
2. **All AIScore Bronze** completes second (T2)
3. **All FotMob ClickHouse** loads third (T3) - after ALL bronze scraping
4. **All AIScore ClickHouse** loads last (T4)

## Execution Modes

### Mode 1: Per-Date Processing
```bash
python scripts/pipeline.py --start-date 20251115 --end-date 20251115
```
**Sequence:** FotMob Bronze → AIScore Bronze → FotMob ClickHouse → AIScore ClickHouse (for each date)

### Mode 2: Monthly Processing
```bash
python scripts/pipeline.py --month 202511
```
**Sequence:** All FotMob Bronze → All AIScore Bronze → All FotMob ClickHouse → All AIScore ClickHouse

### Mode 3: Manual Step-by-Step
```bash
# Step 1: FotMob Bronze
python scripts/scrape_fotmob.py 20251115

# Step 2: AIScore Bronze
python scripts/scrape_aiscore.py 20251115

# Step 3: FotMob ClickHouse
python scripts/load_clickhouse.py --scraper fotmob --date 20251115

# Step 4: AIScore ClickHouse
python scripts/load_clickhouse.py --scraper aiscore --date 20251115
```

## Data Dependencies

```
FotMob Bronze ──┐
                 ├──► (No dependency between scrapers)
AIScore Bronze ──┘

FotMob Bronze ──┐
                 ├──► FotMob ClickHouse (depends on FotMob Bronze)
                 └──► AIScore ClickHouse (depends on AIScore Bronze)
AIScore Bronze ──┘
```

**Important:** 
- FotMob and AIScore bronze scraping are **independent** (can run in parallel if desired)
- ClickHouse loading happens **after** bronze scraping completes
- FotMob ClickHouse loading does **NOT** wait for AIScore bronze (in per-date mode, it waits)
- AIScore ClickHouse loading happens after FotMob ClickHouse (sequential)

## Code References

### Pipeline Execution
- **Main Pipeline**: `scripts/pipeline.py` (lines 340-371)
- **FotMob Bronze**: `scripts/scrape_fotmob.py`
- **AIScore Bronze**: `scripts/scrape_aiscore.py`
- **ClickHouse Loading**: `scripts/load_clickhouse.py`

### Key Functions
- `load_fotmob_data()`: `scripts/load_clickhouse.py:201`
- `load_aiscore_data()`: `scripts/load_clickhouse.py:518`
- `process_single_date()`: `scripts/pipeline.py:335-358`

## Recent Updates

### Logging (2025-11-22)
- **Unified Pipeline Logs**: All pipeline execution logs are now consolidated into `logs/pipeline_{date}.log`
- **Format**: Single date (`pipeline_20251115.log`), date range (`pipeline_20251101_to_20251107.log`), or monthly (`pipeline_202511.log`)
- **Includes**: All subprocess output, orchestration messages, errors, and summaries

### Data Deduplication (2025-11-22)
- **AIScore**: All tables use `ReplacingMergeTree(inserted_at)` engine for automatic deduplication
- **FotMob**: Python-side deduplication removed; tables use `MergeTree` with post-insert optimization
- **Optimization**: `OPTIMIZE TABLE FINAL` runs after each insertion for both scrapers

### Storage Statistics (2025-11-22)
- **Auto-update**: Daily listing storage statistics are automatically recalculated after each match is scraped
- **Includes**: `files_stored`, `files_missing`, `total_size_bytes`, `completion_percentage`, etc.

## Summary

**To answer your question directly:**

> **When is FotMob data stored in ClickHouse?**

FotMob data is loaded into ClickHouse:
- **After** AIScore bronze layer finishes (in per-date mode)
- **After** all bronze scraping completes (in monthly mode)

The sequence is:
1. ✅ FotMob Bronze → 2. ✅ AIScore Bronze → 3. ✅ FotMob ClickHouse → 4. ✅ AIScore ClickHouse

This ensures that:
- All raw data is collected first (bronze layer)
- Data is processed and loaded in a controlled sequence
- No data is lost if a step fails
- Data lineage is properly tracked throughout
- Tables are optimized after insertion for better performance
- Deduplication is handled automatically (AIScore) or via optimization (FotMob)

