"""Bronze layer storage for raw AIScore API responses (JSON format).

This module provides AIScore-specific bronze storage implementation that extends
the base bronze storage with AIScore-specific functionality.

Note: Moved from src/scrapers/aiscore/bronze_storage.py to proper storage location.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from .base_bronze_storage import BaseBronzeStorage


class AIScoreBronzeStorage(BaseBronzeStorage):
    """AIScore-specific Bronze layer storage.

    Extends BaseBronzeStorage with AIScore-specific functionality:
    - Complete match saving with odds data
    - Match listing by date
    - Status tracking for scraped matches

    Structure:
        data/aiscore/
            ├── matches/
            │   └── YYYYMMDD/
            │       ├── match_9gklzi1nyx1um7x.json
            │       └── match_abc123.json
            └── lineage/
                └── YYYYMMDD/
                    └── lineage.json
            └── daily_listings/
                └── YYYYMMDD/
                    └── matches.json

    Data lineage is tracked separately in:
        data/aiscore/lineage/{date}/lineage.json

    Daily listings track match IDs to prevent duplicate requests.
    """

    @property
    def scraper_name(self) -> str:
        """Return the scraper name."""
        return "aiscore"

    @property
    def source_name(self) -> str:
        """Return the data source name."""
        return "aiscore_web"

    def __init__(self, base_path: str = "data/aiscore"):
        """Initialize AIScore Bronze storage.

        Args:
            base_path: Base directory for raw data (default: data/aiscore)
        """
        super().__init__(base_path)

    def save_complete_match(
        self,
        match_id: str,
        match_url: str,
        game_date: str,
        scrape_date: str,
        scrape_start: datetime,
        scrape_end: datetime,
        scrape_status: str,
        teams: Dict[str, str],
        match_result: Optional[str],
        odds_1x2: List[Dict[str, Any]],
        odds_asian_handicap: List[Dict[str, Any]],
        odds_over_under: List[Dict[str, Any]],
        league: Optional[str] = None
    ) -> Path:
        """Save complete match data to Bronze layer.

        This is the main method to call after scraping a match.

        Args:
            match_id: Match identifier
            match_url: Full match URL
            game_date: Date of the game (YYYYMMDD or YYYY-MM-DD)
            scrape_date: Date of scraping (YYYYMMDD or YYYY-MM-DD)
            scrape_start: Scraping start time
            scrape_end: Scraping end time
            scrape_status: success/partial/failed
            teams: {home, away}
            match_result: Final score (e.g., "2-1")
            odds_1x2: List of 1X2 odds
            odds_asian_handicap: List of Asian Handicap odds
            odds_over_under: List of Over/Under odds
            league: League/competition name (optional)

        Returns:
            Path to saved file
        """
        match_data = {
            "match_id": match_id,
            "match_url": match_url,
            "game_date": game_date,
            "scrape_timestamp": scrape_end.isoformat(),
            "scrape_status": scrape_status,
            "scrape_duration": (scrape_end - scrape_start).total_seconds(),
            "teams": teams,
            "match_result": match_result,
            "league": league,
            "odds_1x2": odds_1x2,
            "odds_asian_handicap": odds_asian_handicap,
            "odds_over_under": odds_over_under,
            "odds_counts": {
                "odds_1x2": len(odds_1x2),
                "odds_asian_handicap": len(odds_asian_handicap),
                "odds_over_under_goals": len([
                    o for o in odds_over_under
                    if o.get('market_type') == 'goals'
                ]),
                "odds_over_under_corners": len([
                    o for o in odds_over_under
                    if o.get('market_type') == 'corners'
                ]),
                "total_odds": (
                    len(odds_1x2)
                    + len(odds_asian_handicap)
                    + len(odds_over_under)
                )
            }
        }

        file_path = self.save_raw_match_data(match_id, match_data, scrape_date)
        total_odds = match_data["odds_counts"]["total_odds"]
        self.logger.debug(
            f"Saved match {match_id}: {total_odds} odds, status={scrape_status}"
        )
        return file_path

    def date_has_matches(self, date_str: str) -> bool:
        """Check if any matches exist for a given date.

        Args:
            date_str: Date string YYYYMMDD format (or YYYY-MM-DD, will be converted)

        Returns:
            True if any match files exist for the date, False otherwise
        """
        date_str_normalized = self._normalize_date_safe(date_str)

        date_dir = self.matches_dir / date_str_normalized
        if not date_dir.exists():
            return False

        return any(date_dir.glob("match_*.json"))

    def list_matches_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        """List all matches for a given date by scanning the matches directory.

        This replaces read_daily_list functionality.

        Args:
            date_str: Date string YYYYMMDD format (or YYYY-MM-DD, will be converted)

        Returns:
            List of match data dictionaries
        """
        date_str_normalized = self._normalize_date_safe(date_str)

        date_dir = self.matches_dir / date_str_normalized
        if not date_dir.exists():
            return []

        matches = []
        for file_path in date_dir.glob("match_*.json"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    match_data = json.load(f)

                    data = match_data.get("data", {})
                    matches.append({
                        "match_id": match_data.get("match_id"),
                        "match_url": data.get("match_url", ""),
                        "game_date": data.get("game_date", date_str),
                        "scrape_timestamp": match_data.get("scraped_at"),
                        "scrape_status": data.get("scrape_status", "unknown"),
                        "teams": data.get("teams", {}),
                        "league": data.get("league")
                    })
            except Exception as e:
                self.logger.warning(f"Error reading match file {file_path}: {e}")

        return matches

    def read_daily_list(self, scrape_date: str) -> Optional[Dict[str, Any]]:
        """DEPRECATED: Daily listings removed. Use list_matches_for_date() instead.

        Returns a structure compatible with old code, but reads from matches directory.
        """
        matches = self.list_matches_for_date(scrape_date)
        if not matches:
            return None

        return {
            "scrape_date": scrape_date,
            "total_matches": len(matches),
            "matches": matches
        }

    def read_manifest(
        self,
        game_date: str,
        match_id: str
    ) -> Optional[Dict[str, Any]]:
        """DEPRECATED: Manifests removed. Use load_raw_match_data() instead.

        Returns match data in a format compatible with old code.
        """
        match_data = self.load_raw_match_data(match_id, game_date)
        if not match_data:
            return None

        data = match_data.get("data", {}) if isinstance(match_data, dict) else {}
        return {
            "match_id": match_id,
            "game_date": game_date,
            "scrape_status": data.get("scrape_status", "unknown"),
            "scrape_duration": data.get("scrape_duration", 0),
            "teams": data.get("teams", {}),
            "match_result": data.get("match_result"),
            "league": data.get("league"),
            "odds_results": data.get("odds_counts", {})
        }

    def read_odds_data(
        self,
        game_date: str,
        match_id: str
    ) -> Optional[Dict[str, Any]]:
        """DEPRECATED: Use load_raw_match_data() instead.

        Returns odds data from the match file.
        """
        match_data = self.load_raw_match_data(match_id, game_date)
        if not match_data:
            return None

        data = match_data.get("data", {}) if isinstance(match_data, dict) else match_data
        return {
            "match_id": match_id,
            "match_url": data.get("match_url", ""),
            "game_date": game_date,
            "scrape_timestamp": match_data.get("scraped_at") if isinstance(match_data, dict) else None,
            "odds_1x2": data.get("odds_1x2", []),
            "odds_asian_handicap": data.get("odds_asian_handicap", []),
            "odds_over_under": data.get("odds_over_under", [])
        }

    def update_storage_statistics_in_daily_list(
        self,
        date_str: str
    ) -> bool:
        """Update storage statistics in the daily listing file based on actual files.

        This recalculates storage stats after scraping completes.

        Args:
            date_str: Date string YYYYMMDD format (or YYYY-MM-DD, will be converted)

        Returns:
            True if update was successful, False otherwise
        """
        try:
            date_str_normalized = self._normalize_date(date_str)
        except ValueError:
            self.logger.warning(f"Invalid date format: {date_str}")
            return False

        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"

        if not listing_file.exists():
            self.logger.debug(f"Daily listing file not found: {listing_file}")
            return False

        try:
            with open(listing_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            match_ids = []
            if "match_ids" in data:
                match_ids = [str(mid) for mid in data.get("match_ids", [])]
            elif "matches" in data:
                match_ids = [
                    str(m.get("match_id"))
                    for m in data.get("matches", [])
                    if m.get("match_id")
                ]

            if not match_ids:
                self.logger.warning(
                    f"No match IDs found in daily listing for {date_str_normalized}"
                )
                return False

            matches_date_dir = self.matches_dir / date_str_normalized
            storage_stats = self._get_storage_stats(
                date_str_normalized, match_ids, matches_date_dir
            )

            data["storage"] = storage_stats
            data["scraped_at"] = datetime.now().isoformat()

            temp_file = listing_file.parent / ".matches.json.tmp"
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                with open(temp_file, 'r', encoding='utf-8') as f:
                    json.load(f)

                if listing_file.exists():
                    listing_file.unlink()
                temp_file.rename(listing_file)

                self.logger.debug(
                    f"Storage stats for {date_str_normalized}: {storage_stats['files_stored']}/{len(match_ids)} "
                    f"({storage_stats['completion_percentage']:.0f}% complete, {storage_stats['total_size_mb']:.2f} MB)"
                )
                return True
            except Exception as e:
                if temp_file.exists():
                    temp_file.unlink()
                self.logger.error(f"Error updating storage statistics: {e}")
                raise
        except Exception as e:
            self.logger.error(f"Error loading daily listing: {e}")
            return False

    def update_match_status_in_daily_list(
        self,
        date_str: str,
        match_id: str,
        status: str
    ) -> bool:
        """Update the scrape_status for a match in the daily listing file.

        Args:
            date_str: Date string YYYYMMDD format (or YYYY-MM-DD, will be converted)
            match_id: Match ID to update
            status: New status (e.g., 'success', 'failed', 'pending', 'partial')

        Returns:
            True if update was successful, False otherwise
        """
        try:
            date_str_normalized = self._normalize_date(date_str)
        except ValueError:
            self.logger.warning(f"Invalid date format: {date_str}")
            return False

        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"

        if not listing_file.exists():
            self.logger.debug(f"Daily listing file not found: {listing_file}")
            return False

        try:
            with open(listing_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            matches = data.get('matches', [])
            match_found = False
            match_id_str = str(match_id)

            for match in matches:
                match_id_in_list = str(match.get('match_id', ''))
                if match_id_in_list == match_id_str:
                    match['scrape_status'] = status

                    if status.startswith('success') or status == 'partial':
                        match['odds_scraped'] = True
                        match['odds_scrape_status'] = (
                            'completed' if status.startswith('success') else 'partial'
                        )
                        match['odds_scraped_at'] = datetime.now().isoformat()
                    elif status in ['failed', 'failed_by_timeout', 'no_odds_available']:
                        match['odds_scrape_status'] = 'failed'
                    elif status == 'pending':
                        match['odds_scrape_status'] = 'pending'

                    match_found = True
                    self.logger.debug(f"Match {match_id} status updated to '{status}'")
                    break

            if not match_found:
                self.logger.warning(
                    f"Match {match_id} not found in daily listing for "
                    f"{date_str_normalized} (searched {len(matches)} matches)"
                )
                if matches:
                    sample_ids = [str(m.get('match_id', 'N/A')) for m in matches[:5]]
                    self.logger.debug(f"Sample match IDs in listing: {sample_ids}")
                return False

            temp_file = listing_file.parent / ".matches.json.tmp"
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                with open(temp_file, 'r', encoding='utf-8') as f:
                    json.load(f)

                if listing_file.exists():
                    listing_file.unlink()
                temp_file.rename(listing_file)

                # Verify
                with open(listing_file, 'r', encoding='utf-8') as f:
                    verify_data = json.load(f)
                verify_matches = verify_data.get('matches', [])
                for verify_match in verify_matches:
                    if str(verify_match.get('match_id', '')) == match_id_str:
                        if verify_match.get('scrape_status') == status:
                            self.logger.debug(
                                f"Verified: Match {match_id} status '{status}' persisted successfully"
                            )
                        else:
                            self.logger.warning(
                                f"Warning: Match {match_id} status mismatch - "
                                f"expected '{status}', got '{verify_match.get('scrape_status')}'"
                            )
                        break

                return True
            except Exception as write_e:
                self.logger.error(f"Error writing status update: {write_e}")
                if temp_file.exists():
                    try:
                        temp_file.unlink()
                    except Exception:
                        pass
                raise
        except Exception as e:
            self.logger.error(f"Error updating match status for {match_id}: {e}")
            temp_file = listing_file.parent / ".matches.json.tmp"
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass
            return False


# Backward compatibility alias
BronzeStorage = AIScoreBronzeStorage
