# FotMob Validation System Documentation

## Overview

The FotMob Validation System provides comprehensive validation, safe field extraction, and automated response saving for FotMob API data. This system ensures data quality and helps identify missing or malformed fields before processing.

## Components

### 1. **Core Utilities** (`src/utils/fotmob_validator.py`)

#### `SafeFieldExtractor`
Provides null-safe field extraction from nested dictionaries.

```python
from src.utils.fotmob_validator import SafeFieldExtractor

extractor = SafeFieldExtractor()

# Dot notation access
match_id = extractor.safe_get(data, 'general.matchId', default=0)

# Nested key access
home_team = extractor.safe_get_nested(
    data, 'general', 'homeTeam', 'name', default='Unknown'
)
```

**Features:**
- Handles missing keys gracefully
- Returns default values instead of raising exceptions
- Supports both dot notation and nested key access
- Works with dictionaries and lists

#### `FotMobValidator`
Validates FotMob API responses against expected schema.

```python
from src.utils.fotmob_validator import FotMobValidator

validator = FotMobValidator()

# Basic validation
is_valid, errors, warnings = validator.validate_response(data)

# Validation with reporting
validator.validate_and_report(data, match_id='12345')

# Get detailed summary
summary = validator.get_validation_summary(data)
```

**Validation Levels:**
- **Required Fields**: Must be present (e.g., `matchId`, team IDs)
- **Finished Match Fields**: Expected if match is finished
- **Optional Fields**: Validated only in strict mode

**Returns:**
- `is_valid`: Boolean indicating if validation passed
- `errors`: List of critical validation errors
- `warnings`: List of non-critical issues

#### `ResponseSaver`
Saves validated API responses to organized JSON files.

```python
from src.utils.fotmob_validator import ResponseSaver

saver = ResponseSaver(output_dir='data/validated_responses')

# Save valid response
saver.save_response(data, match_id='12345', validation_summary=summary)

# Save invalid response (for debugging)
saver.save_invalid_response(data, match_id='12345', validation_summary=summary)
```

**Output Structure:**
```
data/validated_responses/
├── fotmob/
│   ├── 20251208/
│   │   ├── match_12345.json
│   │   └── match_67890.json
│   └── ...
└── invalid/
    └── fotmob/
        ├── match_11111_invalid.json
        └── ...
```

### 2. **Updated Match Processor** (`src/processors/match_processor.py`)

The `MatchProcessor` now includes automatic validation and response saving.

```python
from src.processors.match_processor import MatchProcessor

# Initialize with response saving enabled
processor = MatchProcessor(
    save_responses=True,
    response_output_dir='data/validated_responses'
)

# Process with validation
dataframes, validation_summary = processor.process_all(
    raw_response,
    validate_before_processing=True
)

# Check validation results
if validation_summary and not validation_summary['is_valid']:
    print(f"Validation issues: {validation_summary['errors']}")
```

**Changes:**
- ✅ Validates responses before processing
- ✅ Saves validated responses to JSON
- ✅ Separates valid and invalid responses
- ✅ Uses safe field extraction to prevent KeyErrors
- ✅ Returns validation summary alongside dataframes

### 3. **Validation Scripts**

#### `scripts/validate_fotmob_responses.py`
Standalone script to validate existing FotMob data files.

**Usage:**
```bash
# Validate all files in directory
python scripts/validate_fotmob_responses.py data/fotmob/matches

# Custom output directory
python scripts/validate_fotmob_responses.py data/fotmob/matches \
    --output-dir data/validation_reports

# Non-recursive search
python scripts/validate_fotmob_responses.py data/fotmob/matches/20251208 \
    --no-recursive

# Custom file pattern
python scripts/validate_fotmob_responses.py data/fotmob/matches \
    --pattern "match_*.json"
```

**Output:**
- Excel report with multiple sheets:
  - **Statistics**: Overall validation stats
  - **Summary**: Per-file validation results
  - **Errors & Warnings**: Detailed error listings
- Console summary with key metrics

#### `scripts/test_validation.py`
Test script to verify validation system functionality.

**Usage:**
```bash
python scripts/test_validation.py
```

**Tests:**
1. Safe field extraction
2. Validation logic (valid and invalid data)
3. Response saving functionality
4. Real file validation (if files available)

## Validation Rules

### Required Fields

These fields **must** be present:

| Field Path | Type | Description |
|------------|------|-------------|
| `general.matchId` | int/str | Unique match identifier |
| `general.homeTeam.id` | int | Home team ID |
| `general.homeTeam.name` | str | Home team name |
| `general.awayTeam.id` | int | Away team ID |
| `general.awayTeam.name` | str | Away team name |
| `header.status.finished` | bool | Match finished flag |
| `header.status.started` | bool | Match started flag |

### Finished Match Fields

These fields are **expected** for finished matches:

| Field Path | Type | Description |
|------------|------|-------------|
| `header.status.scoreStr` | str | Final score string |
| `header.teams` | list | Team details array |

### Optional Fields

These fields are **validated only in strict mode**:

| Field Path | Type | Description |
|------------|------|-------------|
| `content.shotmap` | dict/None | Shot map data |
| `content.lineup` | dict/None | Lineup data |
| `content.playerStats` | dict/None | Player statistics |
| `content.matchFacts` | dict/None | Match facts/events |
| `content.stats` | dict/None | Match statistics |

## Integration Guide

### 1. Using in Match Processing Pipeline

```python
from src.processors.match_processor import MatchProcessor

# Initialize processor with validation
processor = MatchProcessor(save_responses=True)

# Process match data
dataframes, validation = processor.process_all(raw_data)

# Handle validation results
if validation:
    if validation['is_valid']:
        print(f"✓ Match {validation['match_id']} validated successfully")
    else:
        print(f"⚠ Validation issues for match {validation['match_id']}:")
        for error in validation['errors']:
            print(f"  - {error}")
```

### 2. Using in Scraper

```python
from src.scrapers.fotmob.match_scraper import MatchScraper
from src.utils.fotmob_validator import save_validated_response

scraper = MatchScraper()
match_data = scraper.fetch_match_details(match_id)

# Validate and save
file_path, is_valid, summary = save_validated_response(
    match_data,
    match_id=str(match_id),
    source='fotmob',
    validate=True
)

if is_valid:
    print(f"✓ Validated and saved to {file_path}")
else:
    print(f"⚠ Validation failed: {summary['errors']}")
```

### 3. Batch Validation of Existing Files

```bash
# Validate all FotMob files
python scripts/validate_fotmob_responses.py data/fotmob/matches \
    --output-dir data/validation_reports \
    --report-name fotmob_full_validation

# Review the generated Excel report
open data/validation_reports/fotmob_full_validation_*.xlsx
```

## Validation Report Format

### Excel Report Sheets

#### 1. Statistics Sheet
```
Metric                | Count
----------------------|------
Total Files           | 150
Valid                 | 145
Invalid               | 3
Errors                | 2
Finished Matches      | 140
Total Errors          | 8
Total Warnings        | 15
With Shotmap          | 138
With Lineup           | 142
With Player Stats     | 140
```

#### 2. Summary Sheet
```
File            | Match ID | Home Team | Away Team | Status | Errors | Warnings | Has Shotmap | ...
----------------|----------|-----------|-----------|--------|--------|----------|-------------|----
match_123.json  | 4947772  | Roma      | Lille     | valid  | 0      | 2        | True        | ...
match_456.json  | 4947777  | Team A    | Team B    | invalid| 3      | 5        | False       | ...
```

#### 3. Errors & Warnings Sheet
```
File            | Match ID | Type    | Message
----------------|----------|---------|----------------------------------
match_456.json  | 4947777  | Error   | Missing required field: general.homeTeam.id
match_456.json  | 4947777  | Warning | Missing expected field: content.shotmap
```

## Error Handling

### Common Validation Errors

1. **Missing Required Field**
   ```
   Missing required field: general.matchId
   ```
   **Solution:** Check if API response is complete. May indicate API change or network issue.

2. **Invalid Type**
   ```
   Invalid type for general.matchId: expected int, got str
   ```
   **Solution:** Field type changed. Update validation rules or add type conversion.

3. **Missing Finished Match Field**
   ```
   Missing expected field for finished match: header.status.scoreStr
   ```
   **Solution:** Warning only. May be normal for some match types.

### Handling Validation Failures

```python
from src.processors.match_processor import MatchProcessor
from src.storage.dlq import DeadLetterQueue

processor = MatchProcessor(save_responses=True)
dlq = DeadLetterQueue()

try:
    dataframes, validation = processor.process_all(raw_data)
    
    if validation and not validation['is_valid']:
        # Log validation errors but continue processing
        print(f"Validation warnings for match {validation['match_id']}")
        
        # Optionally send to DLQ for review
        if validation['error_count'] > 5:
            dlq.send_to_dlq(
                'validation_failures',
                raw_data,
                f"Too many validation errors: {validation['error_count']}",
                context={'validation': validation}
            )
    
    # Process dataframes normally
    # ...
    
except Exception as e:
    print(f"Processing failed: {e}")
    dlq.send_to_dlq('processing_failures', raw_data, str(e))
```

## Best Practices

1. **Always Validate Before Processing**
   ```python
   dataframes, validation = processor.process_all(data, validate_before_processing=True)
   ```

2. **Use Safe Extraction in Custom Code**
   ```python
   # Instead of:
   match_id = data['general']['matchId']  # Can raise KeyError
   
   # Use:
   match_id = extractor.safe_get_nested(data, 'general', 'matchId', default=None)
   ```

3. **Review Validation Reports Regularly**
   - Run weekly validation on all stored data
   - Track validation failure rates over time
   - Investigate new error patterns

4. **Save Responses for Debugging**
   - Keep validated responses for at least 30 days
   - Use invalid responses to identify API changes
   - Reference original responses when debugging issues

5. **Update Validation Rules**
   - Add new required fields as they're identified
   - Adjust type expectations based on actual data
   - Document any API changes in validation code

## Troubleshooting

### Validation Script Not Finding Files

```bash
# Check if files exist
ls -la data/fotmob/matches/**/*.json

# Use correct pattern
python scripts/validate_fotmob_responses.py data/fotmob/matches --pattern "*.json"
```

### High Memory Usage with Large Datasets

```python
# Process in batches
from pathlib import Path

files = list(Path('data/fotmob/matches').rglob('*.json'))
batch_size = 100

for i in range(0, len(files), batch_size):
    batch = files[i:i+batch_size]
    # Validate batch
    # ...
```

### Validation Reports Not Generated

```bash
# Check output directory permissions
ls -la data/validation_reports

# Specify absolute path
python scripts/validate_fotmob_responses.py data/fotmob/matches \
    --output-dir /absolute/path/to/reports
```

## API Reference

See inline documentation in:
- `src/utils/fotmob_validator.py` - Core validation utilities
- `scripts/validate_fotmob_responses.py` - Validation script
- `scripts/test_validation.py` - Test utilities

## Change Log

### Version 1.0 (2025-12-08)
- ✨ Initial validation system
- ✨ Safe field extraction utilities
- ✨ Response saving functionality
- ✨ Validation reporting scripts
- ✨ Integration with match processor

