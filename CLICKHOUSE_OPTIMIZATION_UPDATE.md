# ClickHouse Optimization - Moved to SQL

## Summary

Table optimization in ClickHouse has been moved from Python code to SQL queries. This provides better separation of concerns and allows operators to control when optimization occurs.

## Changes Made

### 1. Created SQL Optimization Script

**File:** `clickhouse/init/03_optimize_tables.sql`

- Contains OPTIMIZE TABLE queries for all FotMob and AIScore tables
- Can be run manually or via Makefile
- Uses `FINAL DEDUPLICATE` to merge parts and remove duplicates

### 2. Removed Optimization from Python Code

**File:** `scripts/load_clickhouse.py`

**Removed:**
- `optimize_table()` function (lines 186-196)
- `optimize_all_tables()` function (lines 199-215)
- All calls to these functions throughout the script:
  - Line 597: After FotMob table processing
  - Line 689: Final FotMob optimization
  - Line 749: Special table optimization
  - Line 1069: AIScore table optimization
  - Line 1207: Final AIScore optimization loop
  - Line 1337: Daily listings optimization

**Added:**
- Note in docstring explaining optimization is handled separately

### 3. Updated Makefile

**File:** `Makefile`

**Added:**
```makefile
optimize-tables: ## Optimize and deduplicate all ClickHouse tables
	@echo "Optimizing ClickHouse tables..."
	docker-compose exec -T clickhouse clickhouse-client --user fotmob_user --password fotmob_pass < clickhouse/init/03_optimize_tables.sql
	@echo "Table optimization complete!"
```

### 4. Updated Documentation

**Files:**
- `README.md` - Added optimization step to Quick Start and Common Commands
- `DOCUMENTATION.md` - Added new "Table Optimization" section with:
  - How to optimize using Makefile
  - How to optimize using ClickHouse client
  - Manual optimization examples
  - When to optimize

## Usage

### Optimize All Tables

```bash
# Using Makefile (recommended)
make optimize-tables

# Using ClickHouse client directly
docker-compose exec -T clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  < clickhouse/init/03_optimize_tables.sql
```

### Optimize Specific Table

```bash
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  --query "OPTIMIZE TABLE fotmob.general FINAL DEDUPLICATE"
```

### Optimize Single Database

For FotMob tables only:
```bash
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  --query "OPTIMIZE TABLE fotmob.general FINAL DEDUPLICATE" \
  --query "OPTIMIZE TABLE fotmob.timeline FINAL DEDUPLICATE" \
  # ... etc
```

For AIScore tables only:
```bash
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  --query "OPTIMIZE TABLE aiscore.matches FINAL DEDUPLICATE" \
  # ... etc
```

## Benefits

1. **Separation of Concerns**: Data loading and optimization are now separate operations
2. **Performance**: Load operations complete faster without optimization overhead
3. **Flexibility**: Operators can choose when to optimize (e.g., during off-peak hours)
4. **Transparency**: Optimization is now a visible, explicit operation
5. **Control**: Can optimize specific tables or databases as needed

## When to Optimize

Run optimization:
- After bulk data loading (e.g., loading a full month of data)
- Periodically (e.g., daily, weekly, or monthly depending on data volume)
- When query performance degrades
- After deleting or updating large amounts of data

## Migration Notes

**Before:** Optimization was automatic during data loading
```python
# Old behavior - automatic optimization
stats = load_fotmob_data(client, date_str)
# Tables were optimized automatically
```

**After:** Optimization is a separate manual step
```bash
# New workflow
python scripts/load_clickhouse.py --scraper fotmob --date 20251208
make optimize-tables  # Run optimization separately
```

## Technical Details

### OPTIMIZE TABLE Syntax

```sql
OPTIMIZE TABLE database.table FINAL DEDUPLICATE;
```

- `FINAL` - Forces immediate merge of all data parts
- `DEDUPLICATE` - Removes duplicate rows based on ORDER BY key
- Works with both `MergeTree` and `ReplacingMergeTree` engines

### Tables Optimized

**FotMob (14 tables):**
- general, timeline, venue, player, shotmap, goal, cards, red_card
- period, momentum, starters, substitutes, coaches, team_form

**AIScore (5 tables):**
- matches, odds_1x2, odds_asian_handicap, odds_over_under, daily_listings

## Rollback

If you need to rollback to automatic optimization, you can:

1. Restore the removed functions in `scripts/load_clickhouse.py`
2. Re-add the optimization calls after data insertion
3. Git revert this change

However, the new approach is recommended for production environments.

---

**Date:** December 10, 2025  
**Status:** ✅ Complete

