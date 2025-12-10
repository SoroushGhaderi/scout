#!/usr/bin/env python3
"""
Validate FotMob API responses and generate validation reports.

This script validates saved FotMob match data files and generates
comprehensive validation reports, including:
- Field presence validation
- Data type validation
- Completeness checks
- Error and warning summaries
"""

import json
import gzip
import argparse
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.fotmob_validator import FotMobValidator, SafeFieldExtractor
from src.utils.logging_utils import get_logger


class ValidationReportGenerator:
    """Generate validation reports for FotMob responses."""
    
    def __init__(self, output_dir: str = "data/validation_reports"):
        """
        Initialize report generator.
        
        Args:
            output_dir: Directory to save validation reports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger()
        self.validator = FotMobValidator()
        self.extractor = SafeFieldExtractor()
    
    def load_match_file(self, file_path: Path) -> Dict[str, Any]:
        """Load match data from JSON or GZIP file."""
        try:
            if file_path.suffix == '.gz':
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            
            # Handle wrapped format (with match_id, scraped_at, data)
            if 'data' in data and isinstance(data['data'], dict):
                return data['data']
            
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load {file_path}: {e}")
            return {}
    
    def validate_file(self, file_path: Path) -> Dict[str, Any]:
        """Validate a single match file."""
        self.logger.info(f"Validating: {file_path.name}")
        
        data = self.load_match_file(file_path)
        
        if not data:
            return {
                'file': str(file_path),
                'status': 'error',
                'error': 'Failed to load file',
                'match_id': None,
            }
        
        # Get validation summary
        summary = self.validator.get_validation_summary(data)
        
        # Add file info
        summary['file'] = str(file_path)
        summary['file_name'] = file_path.name
        summary['status'] = 'valid' if summary['is_valid'] else 'invalid'
        
        return summary
    
    def validate_directory(
        self, 
        input_dir: Path,
        pattern: str = "*.json",
        recursive: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Validate all match files in a directory.
        
        Args:
            input_dir: Directory containing match files
            pattern: File pattern to match
            recursive: If True, search recursively
        
        Returns:
            List of validation summaries
        """
        if not input_dir.exists():
            self.logger.error(f"Directory does not exist: {input_dir}")
            return []
        
        # Find all matching files
        if recursive:
            files = list(input_dir.rglob(pattern))
        else:
            files = list(input_dir.glob(pattern))
        
        # Also check for .gz files
        if recursive:
            files.extend(list(input_dir.rglob("*.gz")))
        else:
            files.extend(list(input_dir.glob("*.gz")))
        
        self.logger.info(f"Found {len(files)} files to validate")
        
        # Validate each file
        results = []
        for i, file_path in enumerate(files, 1):
            self.logger.info(f"Progress: {i}/{len(files)}")
            result = self.validate_file(file_path)
            results.append(result)
        
        return results
    
    def generate_report(
        self,
        results: List[Dict[str, Any]],
        report_name: str = "validation_report"
    ) -> Path:
        """
        Generate validation report from results.
        
        Args:
            results: List of validation summaries
            report_name: Name for the report file
        
        Returns:
            Path to generated report
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = self.output_dir / f"{report_name}_{timestamp}.xlsx"
        
        # Create summary dataframe
        summary_records = []
        for result in results:
            summary_records.append({
                'File': result.get('file_name', 'Unknown'),
                'Match ID': result.get('match_id', 'N/A'),
                'Home Team': result.get('home_team', 'N/A'),
                'Away Team': result.get('away_team', 'N/A'),
                'Status': result.get('status', 'unknown'),
                'Is Finished': result.get('is_finished', False),
                'Errors': result.get('error_count', 0),
                'Warnings': result.get('warning_count', 0),
                'Has Shotmap': result.get('data_completeness', {}).get('has_shotmap', False),
                'Has Lineup': result.get('data_completeness', {}).get('has_lineup', False),
                'Has Player Stats': result.get('data_completeness', {}).get('has_player_stats', False),
                'Validated At': result.get('validated_at', 'N/A'),
            })
        
        df_summary = pd.DataFrame(summary_records)
        
        # Create detailed errors dataframe
        error_records = []
        for result in results:
            match_id = result.get('match_id', 'N/A')
            file_name = result.get('file_name', 'Unknown')
            
            for error in result.get('errors', []):
                error_records.append({
                    'File': file_name,
                    'Match ID': match_id,
                    'Type': 'Error',
                    'Message': error
                })
            
            for warning in result.get('warnings', []):
                error_records.append({
                    'File': file_name,
                    'Match ID': match_id,
                    'Type': 'Warning',
                    'Message': warning
                })
        
        df_errors = pd.DataFrame(error_records) if error_records else pd.DataFrame()
        
        # Create statistics dataframe
        stats = {
            'Total Files': len(results),
            'Valid': sum(1 for r in results if r.get('status') == 'valid'),
            'Invalid': sum(1 for r in results if r.get('status') == 'invalid'),
            'Errors': sum(1 for r in results if r.get('status') == 'error'),
            'Finished Matches': sum(1 for r in results if r.get('is_finished', False)),
            'Total Errors': sum(r.get('error_count', 0) for r in results),
            'Total Warnings': sum(r.get('warning_count', 0) for r in results),
            'With Shotmap': sum(1 for r in results if r.get('data_completeness', {}).get('has_shotmap', False)),
            'With Lineup': sum(1 for r in results if r.get('data_completeness', {}).get('has_lineup', False)),
            'With Player Stats': sum(1 for r in results if r.get('data_completeness', {}).get('has_player_stats', False)),
        }
        
        df_stats = pd.DataFrame([stats]).T
        df_stats.columns = ['Count']
        
        # Save to Excel with multiple sheets (or CSV if openpyxl not available)
        try:
            with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
                df_stats.to_excel(writer, sheet_name='Statistics')
                df_summary.to_excel(writer, sheet_name='Summary', index=False)
                if not df_errors.empty:
                    df_errors.to_excel(writer, sheet_name='Errors & Warnings', index=False)
            
            self.logger.info(f"Report saved to: {report_file}")
            return report_file
            
        except ImportError:
            # Fallback to CSV if openpyxl not installed
            self.logger.warning("openpyxl not installed, saving as CSV files instead")
            
            csv_base = report_file.with_suffix('')
            stats_file = Path(str(csv_base) + '_statistics.csv')
            summary_file = Path(str(csv_base) + '_summary.csv')
            errors_file = Path(str(csv_base) + '_errors.csv')
            
            df_stats.to_csv(stats_file)
            df_summary.to_csv(summary_file, index=False)
            if not df_errors.empty:
                df_errors.to_csv(errors_file, index=False)
            
            self.logger.info(f"Reports saved to: {csv_base}_*.csv")
            return summary_file
    
    def print_summary(self, results: List[Dict[str, Any]]):
        """Print validation summary to console."""
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        
        total = len(results)
        valid = sum(1 for r in results if r.get('status') == 'valid')
        invalid = sum(1 for r in results if r.get('status') == 'invalid')
        errors = sum(1 for r in results if r.get('status') == 'error')
        
        print(f"\nTotal files: {total}")
        print(f"  ✓ Valid: {valid} ({valid/total*100:.1f}%)")
        print(f"  ✗ Invalid: {invalid} ({invalid/total*100:.1f}%)")
        print(f"  ⚠ Errors: {errors} ({errors/total*100:.1f}%)")
        
        total_errors = sum(r.get('error_count', 0) for r in results)
        total_warnings = sum(r.get('warning_count', 0) for r in results)
        
        print(f"\nIssues found:")
        print(f"  Validation errors: {total_errors}")
        print(f"  Validation warnings: {total_warnings}")
        
        # Data completeness
        with_shotmap = sum(1 for r in results if r.get('data_completeness', {}).get('has_shotmap', False))
        with_lineup = sum(1 for r in results if r.get('data_completeness', {}).get('has_lineup', False))
        with_stats = sum(1 for r in results if r.get('data_completeness', {}).get('has_player_stats', False))
        
        print(f"\nData completeness:")
        print(f"  With shotmap: {with_shotmap}/{total} ({with_shotmap/total*100:.1f}%)")
        print(f"  With lineup: {with_lineup}/{total} ({with_lineup/total*100:.1f}%)")
        print(f"  With player stats: {with_stats}/{total} ({with_stats/total*100:.1f}%)")
        
        # Show sample of invalid files
        invalid_results = [r for r in results if r.get('status') == 'invalid']
        if invalid_results:
            print(f"\nSample invalid files (showing first 5):")
            for result in invalid_results[:5]:
                print(f"  - {result.get('file_name', 'Unknown')}: "
                      f"{result.get('error_count', 0)} errors")
                for error in result.get('errors', [])[:2]:
                    print(f"    • {error}")
        
        print("\n" + "=" * 80)


def main():
    """Main entry point for validation script."""
    parser = argparse.ArgumentParser(
        description="Validate FotMob API responses"
    )
    parser.add_argument(
        'input_dir',
        type=str,
        help='Directory containing FotMob match files'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/validation_reports',
        help='Output directory for validation reports'
    )
    parser.add_argument(
        '--pattern',
        type=str,
        default='*.json',
        help='File pattern to match (default: *.json)'
    )
    parser.add_argument(
        '--no-recursive',
        action='store_true',
        help='Do not search recursively'
    )
    parser.add_argument(
        '--report-name',
        type=str,
        default='fotmob_validation_report',
        help='Name for the validation report'
    )
    
    args = parser.parse_args()
    
    # Create report generator
    generator = ValidationReportGenerator(args.output_dir)
    
    # Validate directory
    input_path = Path(args.input_dir)
    results = generator.validate_directory(
        input_path,
        pattern=args.pattern,
        recursive=not args.no_recursive
    )
    
    if not results:
        print("No files validated. Check input directory and pattern.")
        return 1
    
    # Generate report
    report_file = generator.generate_report(results, args.report_name)
    
    # Print summary
    generator.print_summary(results)
    
    print(f"\n✓ Full report saved to: {report_file}")
    
    return 0


if __name__ == "__main__":
    exit(main())

