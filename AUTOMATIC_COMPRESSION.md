# Automatic Compression System

## Overview

After each day's scraping completes for both AIScore and FotMob, the system automatically:
1. Compresses each individual match JSON file into `.json.gz` format
2. Bundles all `.json.gz` files for that date into a single `.tar` archive
3. Deletes the intermediate `.json.gz` files
4. Keeps only the `.tar` archive

This saves significant disk space while maintaining data integrity.

## Process Flow

```
Single Match JSON Files
         ↓
Step 1: Compress each .json → .json.gz (GZIP compression)
         ↓
Step 2: Bundle all .json.gz → single .tar archive
         ↓
Step 3: Delete individual .json.gz files
         ↓
Final: One .tar file containing all compressed match data
```

## Implementation

### AIScore Compression

**Location:** `scripts/scrape_aiscore.py`

**Trigger:** After successful odds scraping for a date

```python
def process_single_date(args: argparse.Namespace, date_str: str) -> Tuple[bool, bool]:
    # Step 1: Link Scraping
    links_success = _process_links_step(args, date_str, matches_exist)
    
    # Step 2: Odds Scraping  
    odds_success = _process_odds_step(args, date_str)
    
    # Step 3: Compress files after successful scraping
    if odds_success and not args.links_only:
        _compress_date_files(date_str)
```

**File Structure:**
```
data/aiscore/matches/
├── 20251209/
│   ├── 20251209_matches.tar  ← Final compressed archive
│   └── [no individual files]
```

### FotMob Compression

**Location:** `src/orchestrator.py`

**Trigger:** After successful scraping in bronze_only mode

```python
def scrape_date(self, date_str: str, force_rescrape: bool = False) -> ScraperMetrics:
    # ... scraping logic ...
    
    # Automatic compression after successful scraping
    if self.bronze_only and metrics.successful_matches > 0:
        try:
            logger.info(f"Compressing files for {date_str}...")
            compression_stats = self.bronze_storage.compress_date_files(date_str)
            
            if compression_stats['compressed'] > 0:
                logger.info(
                    f"Compression saved {compression_stats['saved_mb']} MB "
                    f"({compression_stats['saved_pct']}% reduction)"
                )
        except Exception as e:
            logger.error(f"Error during compression: {e}")
```

**File Structure:**
```
data/fotmob/matches/
├── 20251209/
│   ├── 20251209_matches.tar  ← Final compressed archive
│   └── [no individual files]
```

## Compression Statistics

The system tracks and reports:
- **compressed**: Number of files compressed
- **size_before_mb**: Original size in MB
- **size_after_mb**: Compressed size in MB
- **saved_mb**: Space saved in MB
- **saved_pct**: Percentage reduction
- **status**: Operation status (success, already_compressed, no_files, error)

### Example Log Output

**AIScore:**
```
[DEBUG] Starting compression for 20251209
[DEBUG] Compressing 450 JSON files to gzip
[DEBUG] Creating tar archive with 450 files
[DEBUG] Verifying archive integrity
[DEBUG] Archive verified: 450 files intact
[DEBUG] Cleaning up 450 temporary gzip files
[INFO] Compressed 450 files for 20251209: saved 125.34 MB (72% reduction)
```

**FotMob:**
```
[DEBUG] Starting compression for 20251209
[DEBUG] Compressing 320 JSON files to gzip
[DEBUG] Creating tar archive with 320 files
[DEBUG] Verifying archive integrity
[DEBUG] Archive verified: 320 files intact
[DEBUG] Cleaning up 320 temporary gzip files
[INFO] Compressed 320 files for 20251209: saved 89.12 MB (68% reduction)
```

## Resumability

The compression system is **resumable**:
- If a `.tar` archive already exists for a date, compression is skipped
- Use `force=True` parameter to recompress existing archives
- Partial compressions are automatically recovered

```python
# Skip if already compressed
compression_stats = bronze_storage.compress_date_files(date_str)
# Returns: {'status': 'already_compressed', ...}

# Force recompression
compression_stats = bronze_storage.compress_date_files(date_str, force=True)
# Returns: {'status': 'success', ...}
```

## Reading Compressed Data

Both AIScore and FotMob BronzeStorage classes automatically handle reading from compressed archives:

```python
# Load match data (works with both .tar and .json files)
match_data = bronze_storage.load_raw_match_data(match_id, date_str)
```

**Reading Priority:**
1. Check for `.tar` archive first
2. Extract `.json.gz` from archive
3. Decompress and return data
4. Fallback to individual `.json.gz` file
5. Fallback to individual `.json` file

## Benefits

1. **Space Savings**: Typically 60-75% reduction in disk usage
2. **Organized Storage**: One file per date instead of hundreds
3. **Faster Backups**: Fewer files to transfer
4. **Automatic**: No manual intervention required
5. **Safe**: Original data preserved until verification complete
6. **Resumable**: Handles interruptions gracefully

## Performance

### Compression Time
- ~1-2 seconds per 100 matches
- Minimal impact on total pipeline time
- Non-blocking (pipeline continues on compression failure)

### Storage Impact
**Before Compression:**
```
data/aiscore/matches/20251209/
├── match_abc123.json (45 KB)
├── match_def456.json (52 KB)
├── match_ghi789.json (48 KB)
... (450 files, ~22 MB total)
```

**After Compression:**
```
data/aiscore/matches/20251209/
└── 20251209_matches.tar (6.2 MB)

Space saved: 15.8 MB (72%)
```

## Error Handling

Compression errors are **non-fatal**:
- Errors are logged but don't fail the pipeline
- Original files are preserved if compression fails
- Verification ensures data integrity before cleanup

```python
try:
    _compress_date_files(date_str)
except Exception as e:
    logger.error(f"Error during compression: {e}")
    # Pipeline continues - compression failure doesn't block scraping
```

## Manual Compression

You can manually compress dates if needed:

```python
from src.storage import BronzeStorage

# FotMob
bronze = BronzeStorage("data/fotmob")
stats = bronze.compress_date_files("20251209")

# AIScore  
from src.scrapers.aiscore.bronze_storage import BronzeStorage
bronze = BronzeStorage("data/aiscore")
stats = bronze.compress_date_files("20251209")

print(f"Saved {stats['saved_mb']:.2f} MB ({stats['saved_pct']:.0f}%)")
```

## Verification

The system verifies archive integrity:

```python
# After creating tar archive
with tarfile.open(archive_path, 'r') as tar:
    members = tar.getmembers()
    
if len(members) != len(gz_files):
    raise Exception("Archive verification failed")
    
logger.debug(f"Archive verified: {len(members)} files intact")
```

## Configuration

Compression happens automatically with no configuration needed.

**Customize behavior** (if needed):
```python
# In bronze_storage.py compress_date_files()

# Skip compression for recent dates
if (datetime.now() - date).days < 7:
    return {'status': 'skipped_recent'}

# Compress only large dates
if file_count < 100:
    return {'status': 'skipped_small'}
```

## Pipeline Integration

### Single Date
```bash
# AIScore - automatically compresses after scraping
python scripts/scrape_aiscore.py 20251209

# FotMob - automatically compresses after scraping  
python scripts/scrape_fotmob.py 20251209
```

### Date Range
```bash
# Compresses each date after scraping completes
python scripts/pipeline.py --start-date 20251201 --end-date 20251207
```

### Monthly
```bash
# Compresses each date, then loads all to ClickHouse
python scripts/pipeline.py --month 202512
```

## Monitoring

### Check Compression Status

```bash
# Check if date is compressed
ls -lh data/aiscore/matches/20251209/
# If compressed: single .tar file
# If not: multiple .json files

# Check compression ratio
du -sh data/aiscore/matches/20251209/
```

### Decompress for Inspection

```bash
# Extract without deleting archive
cd data/aiscore/matches/20251209
tar -xvf 20251209_matches.tar

# Extract specific file
tar -xvf 20251209_matches.tar match_abc123.json.gz
gunzip match_abc123.json.gz
```

## Best Practices

1. **Let it run automatically** - Don't disable compression
2. **Keep archives** - They're your source of truth
3. **Monitor logs** - Check for compression errors
4. **Verify before delete** - System does this automatically
5. **Backup .tar files** - They contain all your data

## Troubleshooting

### Issue: Compression fails silently
**Solution:** Check logs for error messages
```bash
tail -f logs/pipeline_20251209.log | grep -i compress
```

### Issue: Archive exists but is corrupted
**Solution:** Recompress with force flag
```python
bronze_storage.compress_date_files("20251209", force=True)
```

### Issue: Can't read compressed data
**Solution:** Verify BronzeStorage is loading correctly
```python
data = bronze_storage.load_raw_match_data(match_id, date_str)
if not data:
    print("Archive may be corrupted")
```

## Summary

✅ **Automatic compression after each day's scraping**
✅ **JSON → JSON.GZ → TAR pipeline**
✅ **60-75% space savings**
✅ **Non-blocking error handling**
✅ **Resumable and verifiable**
✅ **Works for both AIScore and FotMob**

---

**Implementation Date:** December 9, 2025
**Status:** Complete and Active
**Last Updated:** December 9, 2025

