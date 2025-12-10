# FotMob Validation System - Quick Start Guide

## Installation

No additional dependencies required. The validation system uses only standard libraries and existing project dependencies.

## Quick Start

### 1. Test the Validation System

```bash
# Run validation tests
python scripts/test_validation.py
```

Expected output:
```
================================================================================
FOTMOB VALIDATION SYSTEM TESTS
================================================================================

================================================================================
TEST 1: Safe Field Extraction
================================================================================
✓ Extracted match_id: 12345
✓ Extracted home_team: Team A
✓ Missing field with default: NOT_FOUND

✓ Safe extraction tests passed!

...
✓ ALL TESTS COMPLETED
```

### 2. Validate Existing FotMob Files

```bash
# Validate all match files
python scripts/validate_fotmob_responses.py data/fotmob/matches

# Or validate specific date
python scripts/validate_fotmob_responses.py data/fotmob/matches/20251208
```

Expected output:
```
Found 150 files to validate
Progress: 150/150
Report saved to: data/validation_reports/fotmob_validation_report_20251208_120000.xlsx

================================================================================
VALIDATION SUMMARY
================================================================================

Total files: 150
  ✓ Valid: 145 (96.7%)
  ✗ Invalid: 3 (2.0%)
  ⚠ Errors: 2 (1.3%)

Issues found:
  Validation errors: 8
  Validation warnings: 15

✓ Full report saved to: data/validation_reports/fotmob_validation_report_20251208_120000.xlsx
```

### 3. Use in Your Code

#### Option A: Process with Automatic Validation

```python
from src.processors.match_processor import MatchProcessor

# Initialize processor (saves validated responses by default)
processor = MatchProcessor(save_responses=True)

# Load your raw match data
import json
with open('data/fotmob/matches/20251208/match_4947772.json') as f:
    raw_data = json.load(f)

# Extract data if wrapped
if 'data' in raw_data:
    match_data = raw_data['data']
else:
    match_data = raw_data

# Process with validation
dataframes, validation = processor.process_all(match_data)

# Check results
if validation:
    print(f"Match ID: {validation['match_id']}")
    print(f"Valid: {validation['is_valid']}")
    print(f"Errors: {validation['error_count']}")
    
# Use processed dataframes
print(dataframes['general'].head())
```

#### Option B: Validate Only

```python
from src.utils.fotmob_validator import FotMobValidator

validator = FotMobValidator()

# Validate response
is_valid, errors, warnings = validator.validate_response(match_data)

if is_valid:
    print("✓ Data is valid")
else:
    print(f"✗ Found {len(errors)} errors:")
    for error in errors:
        print(f"  - {error}")
```

#### Option C: Safe Field Extraction

```python
from src.utils.fotmob_validator import SafeFieldExtractor

extractor = SafeFieldExtractor()

# Extract with null safety
match_id = extractor.safe_get(match_data, 'general.matchId', default=0)
home_team = extractor.safe_get_nested(
    match_data, 'general', 'homeTeam', 'name', default='Unknown'
)

print(f"Match {match_id}: {home_team} vs ...")
```

## Common Use Cases

### Use Case 1: Validate Before Loading to ClickHouse

```python
from src.processors.match_processor import MatchProcessor
from src.storage.clickhouse_client import ClickHouseClient

processor = MatchProcessor(save_responses=True)
client = ClickHouseClient()

# Process with validation
dataframes, validation = processor.process_all(raw_data)

# Only load if valid
if validation and validation['is_valid']:
    for table_name, df in dataframes.items():
        client.insert_dataframe(df, table_name)
    print(f"✓ Loaded match {validation['match_id']}")
else:
    print(f"⚠ Skipped invalid match {validation['match_id']}")
```

### Use Case 2: Batch Validation with Progress Tracking

```python
from pathlib import Path
from src.utils.fotmob_validator import FotMobValidator
import json

validator = FotMobValidator()
match_dir = Path('data/fotmob/matches')

results = []
for i, file_path in enumerate(match_dir.rglob('*.json'), 1):
    with open(file_path) as f:
        data = json.load(f)
    
    if 'data' in data:
        data = data['data']
    
    summary = validator.get_validation_summary(data)
    results.append(summary)
    
    if i % 10 == 0:
        print(f"Progress: {i} files processed")

# Analyze results
valid_count = sum(1 for r in results if r['is_valid'])
print(f"\nValidated {len(results)} files")
print(f"Valid: {valid_count}/{len(results)} ({valid_count/len(results)*100:.1f}%)")
```

### Use Case 3: Find Missing Data

```python
from src.utils.fotmob_validator import FotMobValidator
import json
from pathlib import Path

validator = FotMobValidator()

# Find matches without shotmap data
matches_without_shotmap = []

for file_path in Path('data/fotmob/matches').rglob('*.json'):
    with open(file_path) as f:
        data = json.load(f)
    
    if 'data' in data:
        data = data['data']
    
    summary = validator.get_validation_summary(data)
    
    if not summary['data_completeness']['has_shotmap']:
        matches_without_shotmap.append({
            'file': file_path.name,
            'match_id': summary['match_id'],
            'home_team': summary['home_team'],
            'away_team': summary['away_team']
        })

print(f"Found {len(matches_without_shotmap)} matches without shotmap data")
for match in matches_without_shotmap[:10]:
    print(f"  - {match['match_id']}: {match['home_team']} vs {match['away_team']}")
```

## Output Files

### 1. Validated Responses
Location: `data/validated_responses/fotmob/YYYYMMDD/match_*.json`

Format:
```json
{
  "match_id": "4947772",
  "source": "fotmob",
  "saved_at": "2025-12-08T12:00:00",
  "validation": {
    "is_valid": true,
    "error_count": 0,
    "warning_count": 2,
    "data_completeness": {
      "has_shotmap": true,
      "has_lineup": true,
      "has_player_stats": true
    }
  },
  "data": {
    "general": {...},
    "header": {...},
    "content": {...}
  }
}
```

### 2. Invalid Responses
Location: `data/validated_responses/invalid/fotmob/match_*_invalid.json`

Same format as above, but with `validation_failed: true` and detailed error information.

### 3. Validation Reports
Location: `data/validation_reports/fotmob_validation_report_*.xlsx`

Excel file with three sheets:
- **Statistics**: Overall metrics
- **Summary**: Per-file results
- **Errors & Warnings**: Detailed issues

## Troubleshooting

### Problem: No files found

```bash
# Check file location
ls data/fotmob/matches/

# Use correct path
python scripts/validate_fotmob_responses.py data/fotmob/matches/20251002
```

### Problem: Import errors

```python
# Add project root to Python path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Now import
from src.utils.fotmob_validator import FotMobValidator
```

### Problem: Permission errors when saving

```bash
# Create output directories
mkdir -p data/validated_responses
mkdir -p data/validation_reports

# Check permissions
chmod -R 755 data/
```

## Next Steps

1. ✅ **Run tests**: `python scripts/test_validation.py`
2. ✅ **Validate existing data**: `python scripts/validate_fotmob_responses.py data/fotmob/matches`
3. ✅ **Review report**: Open the generated Excel file
4. ✅ **Integrate in pipeline**: Update your processing scripts to use `MatchProcessor` with validation
5. ✅ **Monitor**: Set up regular validation runs to catch API changes

## Getting Help

- **Full documentation**: See `docs/VALIDATION_SYSTEM.md`
- **Code examples**: Check `scripts/test_validation.py`
- **API reference**: Read inline docs in `src/utils/fotmob_validator.py`

## Tips

💡 **Always validate before processing**: Catch issues early
💡 **Save responses**: Useful for debugging and reprocessing
💡 **Use safe extraction**: Avoid KeyError exceptions
💡 **Review reports regularly**: Identify patterns in data quality
💡 **Update validation rules**: As the API evolves

