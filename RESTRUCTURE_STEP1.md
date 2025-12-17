# Project Restructuring - Step 1: Move AIScore Storage

**Date:** December 18, 2025  
**Status:** ✅ COMPLETED

## Summary

Moved AIScore bronze storage from incorrect location inside scrapers to proper storage layer location. This fixes a critical architectural issue where storage concerns were mixed with scraper logic.

## Changes Made

### 1. Created New Storage Module
- **New File:** `src/storage/aiscore_storage.py`
- **Class Name:** `AIScoreBronzeStorage` (with `BronzeStorage` alias for backward compat)
- **Imports Fixed:** Changed from `...storage.base_bronze_storage` (relative) to `.base_bronze_storage` (correct relative)

### 2. Updated Storage Package Exports
- **File:** `src/storage/__init__.py`
- **Added:** `AIScoreBronzeStorage` to `__all__`
- Now exports: `['BaseBronzeStorage', 'BronzeStorage', 'AIScoreBronzeStorage']`

### 3. Updated Import Statements (4 files)

#### scripts/aiscore_scripts/scrape_odds.py
```python
# OLD: from src.scrapers.aiscore.bronze_storage import BronzeStorage
# NEW: from src.storage.aiscore_storage import BronzeStorage
```

#### scripts/aiscore_scripts/scrape_links.py
```python
# OLD: from src.scrapers.aiscore.bronze_storage import BronzeStorage
# NEW: from src.storage.aiscore_storage import BronzeStorage
```

#### scripts/load_clickhouse.py
```python
# OLD: from src.scrapers.aiscore.bronze_storage import BronzeStorage as AIScoreBronzeStorage
# NEW: from src.storage.aiscore_storage import AIScoreBronzeStorage
```

#### scripts/scrape_aiscore.py
```python
# OLD: from src.scrapers.aiscore.bronze_storage import BronzeStorage
# NEW: from src.storage.aiscore_storage import BronzeStorage
```

### 4. Created Deprecation Shim
- **File:** `src/scrapers/aiscore/bronze_storage.py` (replaced with deprecation warning)
- Provides backward compatibility
- Issues `DeprecationWarning` when imported from old location
- Prevents breaking any external code we might have missed

## Architecture Improvement

### Before (WRONG)
```
src/
  ├── scrapers/
  │   ├── aiscore/
  │   │   ├── bronze_storage.py ❌  # Storage in scraper package!
  │   │   ├── scraper.py
  │   │   └── ...
  └── storage/
      ├── bronze_storage.py         # FotMob storage
      └── ...
```

### After (CORRECT)
```
src/
  ├── scrapers/
  │   ├── aiscore/
  │   │   ├── bronze_storage.py ⚠️   # Deprecation shim only
  │   │   ├── scraper.py
  │   │   └── ...
  └── storage/
      ├── base_bronze_storage.py     # Base class
      ├── bronze_storage.py           # FotMob storage
      ├── aiscore_storage.py ✅       # AIScore storage (NEW)
      └── ...
```

## Why This Matters

1. **Separation of Concerns:** Storage logic is now separate from scraper logic
2. **Consistency:** Both FotMob and AIScore storage are in the same package
3. **Maintainability:** Easier to understand and modify storage implementations
4. **Testability:** Storage can be tested independently of scrapers
5. **Reusability:** Storage classes can be used by other components without importing scraper code

## Benefits

- ✅ Cleaner architecture
- ✅ Follows Single Responsibility Principle
- ✅ Matches FotMob storage location pattern
- ✅ Makes dependency relationships clearer
- ✅ Backward compatible (with deprecation warnings)

## Testing

- All imports have been updated
- Deprecation shim ensures old code continues to work
- No breaking changes for external consumers

## Next Steps (Future)

**Step 2:** Move config files to single location  
**Step 3:** Create `src/scout/core/` with interfaces and protocols  
**Step 4:** Move Docker files to `docker/` directory  
**Step 5:** Move notebooks to `notebooks/` directory  
**Step 6:** Rename `src/` to `src/scout/` for clarity  

---

## Git Status

```
 M scripts/aiscore_scripts/scrape_links.py
 M scripts/aiscore_scripts/scrape_odds.py
 M scripts/load_clickhouse.py
 M scripts/scrape_aiscore.py
 M src/scrapers/aiscore/bronze_storage.py
 M src/storage/__init__.py
?? src/storage/aiscore_storage.py
```

## Commit Message (Suggested)

```
refactor(storage): Move AIScore storage to proper location

- Move src/scrapers/aiscore/bronze_storage.py to src/storage/aiscore_storage.py
- Rename class to AIScoreBronzeStorage for consistency
- Update all imports across 4 script files
- Add backward compatibility shim with deprecation warning
- Export AIScoreBronzeStorage from storage package

This fixes architectural issue where storage logic was incorrectly
placed inside scraper package. Storage is a cross-cutting concern
and belongs in its own dedicated package.

BREAKING CHANGE: Class renamed from BronzeStorage to AIScoreBronzeStorage.
Old import path still works but issues deprecation warning.
```
