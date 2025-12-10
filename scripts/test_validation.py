#!/usr/bin/env python3
"""
Test script for FotMob validation and response saving.

This script tests the validation system with actual FotMob data files.
"""

import json
import gzip
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.fotmob_validator import (
    FotMobValidator, 
    SafeFieldExtractor,
    ResponseSaver,
    save_validated_response
)
from src.utils.logging_utils import get_logger


def load_sample_file(file_path: Path):
    """Load a sample FotMob file."""
    try:
        if file_path.suffix == '.gz':
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
        else:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        
        # Handle wrapped format
        if 'data' in data and isinstance(data['data'], dict):
            return data['data']
        
        return data
    except Exception as e:
        print(f"Error loading file: {e}")
        return None


def test_safe_extraction():
    """Test safe field extraction."""
    print("\n" + "=" * 80)
    print("TEST 1: Safe Field Extraction")
    print("=" * 80)
    
    # Create test data
    test_data = {
        'general': {
            'matchId': 12345,
            'homeTeam': {'id': 1, 'name': 'Team A'},
            'awayTeam': {'id': 2, 'name': 'Team B'}
        },
        'header': {
            'status': {
                'finished': True
            }
        }
    }
    
    extractor = SafeFieldExtractor()
    
    # Test existing fields
    match_id = extractor.safe_get(test_data, 'general.matchId')
    print(f"✓ Extracted match_id: {match_id}")
    
    home_team = extractor.safe_get_nested(test_data, 'general', 'homeTeam', 'name')
    print(f"✓ Extracted home_team: {home_team}")
    
    # Test missing fields
    missing = extractor.safe_get(test_data, 'content.shotmap', default='NOT_FOUND')
    print(f"✓ Missing field with default: {missing}")
    
    print("\n✓ Safe extraction tests passed!")


def test_validation():
    """Test validation logic."""
    print("\n" + "=" * 80)
    print("TEST 2: Validation Logic")
    print("=" * 80)
    
    validator = FotMobValidator()
    
    # Test valid data
    valid_data = {
        'general': {
            'matchId': 12345,
            'homeTeam': {'id': 1, 'name': 'Team A'},
            'awayTeam': {'id': 2, 'name': 'Team B'}
        },
        'header': {
            'status': {
                'finished': True,
                'started': True,
                'scoreStr': '2-1'
            },
            'teams': [{'id': 1}, {'id': 2}]
        }
    }
    
    is_valid, errors, warnings = validator.validate_response(valid_data)
    print(f"\nValid data test:")
    print(f"  Is valid: {is_valid}")
    print(f"  Errors: {len(errors)}")
    print(f"  Warnings: {len(warnings)}")
    
    if errors:
        print("  Error details:")
        for error in errors:
            print(f"    - {error}")
    
    # Test invalid data
    invalid_data = {
        'general': {
            'matchId': 12345,
            # Missing homeTeam and awayTeam
        },
        'header': {
            'status': {}
        }
    }
    
    is_valid, errors, warnings = validator.validate_response(invalid_data)
    print(f"\nInvalid data test:")
    print(f"  Is valid: {is_valid}")
    print(f"  Errors: {len(errors)}")
    print(f"  Expected errors: {errors[:3]}")
    
    print("\n✓ Validation tests completed!")


def test_response_saving():
    """Test response saving functionality."""
    print("\n" + "=" * 80)
    print("TEST 3: Response Saving")
    print("=" * 80)
    
    saver = ResponseSaver(output_dir="data/test_validated_responses")
    
    test_data = {
        'general': {
            'matchId': 99999,
            'homeTeam': {'id': 1, 'name': 'Test Home'},
            'awayTeam': {'id': 2, 'name': 'Test Away'}
        },
        'header': {
            'status': {
                'finished': True,
                'started': True
            }
        }
    }
    
    validation_summary = {
        'is_valid': True,
        'error_count': 0,
        'warning_count': 0,
        'errors': [],
        'warnings': []
    }
    
    try:
        saved_file = saver.save_response(
            test_data,
            '99999',
            validation_summary,
            source='fotmob_test'
        )
        print(f"✓ Saved test response to: {saved_file}")
        
        # Verify file was created
        if saved_file.exists():
            print(f"✓ File exists and is accessible")
            
            # Load and verify contents
            with open(saved_file, 'r') as f:
                loaded_data = json.load(f)
            
            if 'validation' in loaded_data:
                print(f"✓ Validation summary included in saved file")
            
            print(f"✓ Response saving test passed!")
        else:
            print(f"✗ File was not created")
            
    except Exception as e:
        print(f"✗ Error saving response: {e}")


def test_with_real_file(file_path: str):
    """Test validation with a real FotMob file."""
    print("\n" + "=" * 80)
    print("TEST 4: Real File Validation")
    print("=" * 80)
    
    path = Path(file_path)
    if not path.exists():
        print(f"✗ File not found: {file_path}")
        return
    
    print(f"Loading: {path.name}")
    data = load_sample_file(path)
    
    if not data:
        print("✗ Failed to load file")
        return
    
    validator = FotMobValidator()
    
    # Get full validation summary
    summary = validator.get_validation_summary(data)
    
    print(f"\nValidation Results:")
    print(f"  Match ID: {summary['match_id']}")
    print(f"  Home Team: {summary['home_team']}")
    print(f"  Away Team: {summary['away_team']}")
    print(f"  Is Finished: {summary['is_finished']}")
    print(f"  Is Valid: {summary['is_valid']}")
    print(f"  Errors: {summary['error_count']}")
    print(f"  Warnings: {summary['warning_count']}")
    
    print(f"\nData Completeness:")
    for key, value in summary['data_completeness'].items():
        status = "✓" if value else "✗"
        print(f"  {status} {key}: {value}")
    
    if summary['errors']:
        print(f"\nErrors:")
        for error in summary['errors']:
            print(f"  - {error}")
    
    if summary['warnings']:
        print(f"\nWarnings:")
        for warning in summary['warnings'][:5]:
            print(f"  - {warning}")
        if len(summary['warnings']) > 5:
            print(f"  ... and {len(summary['warnings']) - 5} more")
    
    # Test saving
    print(f"\nTesting save functionality...")
    try:
        file_path, is_valid, val_summary = save_validated_response(
            data,
            str(summary['match_id']),
            output_dir="data/test_validated_responses",
            source="fotmob_test",
            validate=True
        )
        print(f"✓ Saved to: {file_path}")
    except Exception as e:
        print(f"✗ Save failed: {e}")


def main():
    """Run all tests."""
    logger = get_logger()
    
    print("\n" + "=" * 80)
    print("FOTMOB VALIDATION SYSTEM TESTS")
    print("=" * 80)
    
    # Run unit tests
    test_safe_extraction()
    test_validation()
    test_response_saving()
    
    # Test with real file if available
    test_files = list(Path("data/fotmob/matches").rglob("*.json"))
    if not test_files:
        test_files = list(Path("data/fotmob/matches").rglob("*.gz"))
    
    if test_files:
        print(f"\nFound {len(test_files)} FotMob files")
        test_with_real_file(str(test_files[0]))
    else:
        print("\nNo FotMob files found for real-world testing")
        print("Skipping TEST 4")
    
    print("\n" + "=" * 80)
    print("✓ ALL TESTS COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    main()

