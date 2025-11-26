"""
League Analysis Script
======================

Purpose: Analyze scraped match data to extract and categorize all league names.

This script:
1. Scans 30 days of daily_listings to find all unique leagues
2. Counts matches per league
3. Groups leagues by country
4. Generates a comprehensive league report

Usage:
    python scripts/analyze_leagues.py
    python scripts/analyze_leagues.py --days 30
    python scripts/analyze_leagues.py --start-date 20251101
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Set

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))
from utils import add_project_to_path, generate_date_range
add_project_to_path()

from src.scrapers.aiscore.bronze_storage import BronzeStorage
from src.scrapers.aiscore.config import Config


def analyze_leagues_from_listings(
    bronze_storage: BronzeStorage,
    dates: List[str]
) -> Dict:
    """
    Analyze league data from daily listings.

    Args:
        bronze_storage: BronzeStorage instance
        dates: List of dates in YYYYMMDD format

    Returns:
        Dictionary with league statistics
    """
    # Storage for analysis
    league_stats = defaultdict(lambda: {
        'total_matches': 0,
        'countries': set(),
        'dates_seen': set(),
        'match_examples': []
    })

    country_leagues = defaultdict(set)
    total_matches_analyzed = 0
    dates_with_data = 0

    print(f"\n{'='*80}")
    print(f"Analyzing {len(dates)} days of match data...")
    print(f"{'='*80}\n")

    for idx, date_str in enumerate(dates, 1):
        # Load daily listing
        listing = bronze_storage.load_daily_listing(date_str)

        if not listing:
            print(f"[{idx}/{len(dates)}] {date_str}: No data")
            continue

        matches = listing.get('matches', [])
        if not matches:
            print(f"[{idx}/{len(dates)}] {date_str}: No matches")
            continue

        dates_with_data += 1
        matches_today = 0

        for match in matches:
            # Extract league info
            league_name = match.get('league_name', 'Unknown')
            country = match.get('country', 'Unknown')

            if league_name == 'Unknown' and country == 'Unknown':
                continue  # Skip matches with no league info

            # Update statistics
            league_stats[league_name]['total_matches'] += 1
            league_stats[league_name]['countries'].add(country)
            league_stats[league_name]['dates_seen'].add(date_str)

            # Store example match URLs (up to 3 per league)
            if len(league_stats[league_name]['match_examples']) < 3:
                league_stats[league_name]['match_examples'].append({
                    'date': date_str,
                    'url': match.get('match_url', '')[:80],
                    'country': country
                })

            # Track country->league mapping
            country_leagues[country].add(league_name)

            total_matches_analyzed += 1
            matches_today += 1

        print(f"[{idx}/{len(dates)}] {date_str}: {matches_today} matches")

    print(f"\n{'='*80}")
    print(f"Analysis Summary")
    print(f"{'='*80}")
    print(f"Total dates scanned:     {len(dates)}")
    print(f"Dates with data:         {dates_with_data}")
    print(f"Total matches analyzed:  {total_matches_analyzed}")
    print(f"Unique leagues found:    {len(league_stats)}")
    print(f"Unique countries found:  {len(country_leagues)}")
    print(f"{'='*80}\n")

    # Convert sets to lists for JSON serialization
    for league_name, stats in league_stats.items():
        stats['countries'] = sorted(list(stats['countries']))
        stats['dates_seen'] = sorted(list(stats['dates_seen']))

    return {
        'league_stats': dict(league_stats),
        'country_leagues': {
            country: sorted(list(leagues))
            for country, leagues in country_leagues.items()
        },
        'summary': {
            'total_dates': len(dates),
            'dates_with_data': dates_with_data,
            'total_matches': total_matches_analyzed,
            'unique_leagues': len(league_stats),
            'unique_countries': len(country_leagues)
        }
    }


def print_league_report(analysis: Dict, top_n: int = 50):
    """Print formatted league report."""

    league_stats = analysis['league_stats']
    country_leagues = analysis['country_leagues']

    # Sort leagues by match count
    sorted_leagues = sorted(
        league_stats.items(),
        key=lambda x: x[1]['total_matches'],
        reverse=True
    )

    print(f"\n{'='*80}")
    print(f"TOP {top_n} LEAGUES BY MATCH COUNT")
    print(f"{'='*80}\n")
    print(f"{'Rank':<6} {'League Name':<40} {'Matches':<10} {'Countries':<30}")
    print(f"{'-'*6} {'-'*40} {'-'*10} {'-'*30}")

    for rank, (league_name, stats) in enumerate(sorted_leagues[:top_n], 1):
        countries_str = ', '.join(stats['countries'][:3])
        if len(stats['countries']) > 3:
            countries_str += f" (+{len(stats['countries']) - 3} more)"

        league_display = league_name[:38] + '..' if len(league_name) > 40 else league_name

        print(f"{rank:<6} {league_display:<40} {stats['total_matches']:<10} {countries_str:<30}")

    # Print leagues by country
    print(f"\n{'='*80}")
    print(f"LEAGUES BY COUNTRY")
    print(f"{'='*80}\n")

    sorted_countries = sorted(
        country_leagues.items(),
        key=lambda x: len(x[1]),
        reverse=True
    )

    for country, leagues in sorted_countries[:20]:
        print(f"\n{country} ({len(leagues)} leagues):")
        for league in sorted(leagues)[:10]:
            matches = league_stats[league]['total_matches']
            print(f"  - {league}: {matches} matches")
        if len(leagues) > 10:
            print(f"  ... and {len(leagues) - 10} more leagues")


def save_analysis_report(analysis: Dict, output_file: Path):
    """Save detailed analysis to JSON file."""

    print(f"\nSaving detailed analysis to: {output_file}")

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)

    print(f"✓ Analysis saved successfully")
    print(f"  File size: {output_file.stat().st_size / 1024:.1f} KB")


def generate_league_config_template(analysis: Dict, output_file: Path, min_matches: int = 10):
    """
    Generate a template config with suggested leagues to include.

    Args:
        analysis: Analysis dictionary
        output_file: Path to save template
        min_matches: Minimum matches per league to include in template
    """

    league_stats = analysis['league_stats']

    # Filter leagues by minimum match count
    suggested_leagues = [
        league_name
        for league_name, stats in league_stats.items()
        if stats['total_matches'] >= min_matches and league_name != 'Unknown'
    ]

    # Sort alphabetically for easier review
    suggested_leagues.sort()

    template = {
        "scraping": {
            "filter_by_leagues": True,
            "allowed_leagues": suggested_leagues,
            "league_filter_notes": f"Generated from analysis of {analysis['summary']['total_dates']} days",
            "league_filter_criteria": f"Leagues with >= {min_matches} matches",
            "total_leagues_included": len(suggested_leagues)
        }
    }

    print(f"\nGenerating league filter template...")
    print(f"  Criteria: Leagues with >= {min_matches} matches")
    print(f"  Leagues included: {len(suggested_leagues)}")

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=2, ensure_ascii=False)

    print(f"✓ Template saved to: {output_file}")


def main():
    """Main execution."""

    parser = argparse.ArgumentParser(
        description='Analyze league data from scraped matches',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='Number of days to analyze (default: 30)'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYYMMDD format (default: 30 days ago from today)'
    )

    parser.add_argument(
        '--top-n',
        type=int,
        default=50,
        help='Number of top leagues to display (default: 50)'
    )

    parser.add_argument(
        '--min-matches',
        type=int,
        default=10,
        help='Minimum matches per league for config template (default: 10)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='analysis_reports',
        help='Directory to save reports (default: analysis_reports)'
    )

    args = parser.parse_args()

    # Determine date range
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y%m%d')
        end_date = start_date + timedelta(days=args.days - 1)
        dates = generate_date_range(
            start_date.strftime('%Y%m%d'),
            end_date.strftime('%Y%m%d')
        )
    else:
        # Default: last N days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days - 1)
        dates = generate_date_range(
            start_date.strftime('%Y%m%d'),
            end_date.strftime('%Y%m%d')
        )

    # Initialize storage
    config = Config()
    bronze_storage = BronzeStorage(config.storage.bronze_path)

    # Run analysis
    analysis = analyze_leagues_from_listings(bronze_storage, dates)

    # Print report
    print_league_report(analysis, top_n=args.top_n)

    # Save detailed analysis
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    analysis_file = output_dir / f'league_analysis_{timestamp}.json'
    save_analysis_report(analysis, analysis_file)

    # Generate config template
    config_template_file = output_dir / f'league_config_template_{timestamp}.json'
    generate_league_config_template(
        analysis,
        config_template_file,
        min_matches=args.min_matches
    )

    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*80}")
    print(f"\nNext steps:")
    print(f"1. Review the league analysis: {analysis_file}")
    print(f"2. Check the suggested config: {config_template_file}")
    print(f"3. Edit your .env file to add: AISCORE_FILTER_BY_LEAGUES=true")
    print(f"4. Copy desired leagues from template to your config")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user. Exiting...")
        sys.exit(130)
    except Exception as e:
        print(f"\nError during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
