"""Bronze layer storage for raw API responses (JSON format).

Bronze layer contains unprocessed, raw data exactly as received from the API.
This provides an audit trail and enables reprocessing if needed.
"""

import json
import os
import gzip
import tarfile
import io
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from ...utils.lineage import LineageTracker
from ...utils.date_utils import format_date_compact_to_display_partial

logger = logging.getLogger(__name__)


class BronzeStorage:
    """
    Store raw API responses in Bronze layer (data lake).
    
    Bronze layer contains unprocessed, raw data exactly as received from the API.
    This provides an audit trail and enables reprocessing if needed.
    
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
    
    Data lineage is tracked separately in: data/aiscore/lineage/{date}/lineage.json
    Daily listings track match IDs to prevent duplicate requests.
    """
    
    def __init__(self, base_path: str = "data/aiscore"):
        """
        Initialize Bronze storage.
        
        Args:
            base_path: Base directory for raw data (default: data/aiscore)
        """
        self.base_path = Path(base_path)
        self.logger = logging.getLogger(__name__)
        
        # Initialize lineage tracker (lineage stored in data/aiscore/lineage/)
        # Use base_path directly so lineage goes to data/aiscore/lineage/ (not data/aiscore/aiscore/lineage/)
        self.lineage_tracker = LineageTracker(base_path=str(self.base_path))
        
        # Create base directories with conflict checking
        if self.base_path.exists() and self.base_path.is_file():
            raise OSError(
                f"Cannot create directory '{self.base_path}': A file with that name already exists. "
                f"Please remove or rename the file at {self.base_path.absolute()}"
            )
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Matches directory
        self.matches_dir = self.base_path / "matches"
        if self.matches_dir.exists() and self.matches_dir.is_file():
            raise OSError(
                f"Cannot create directory '{self.matches_dir}': A file with that name already exists. "
                f"Please remove or rename the file at {self.matches_dir.absolute()}"
            )
        self.matches_dir.mkdir(parents=True, exist_ok=True)
        
        # Daily listings directory
        self.daily_listings_dir = self.base_path / "daily_listings"
        if self.daily_listings_dir.exists() and self.daily_listings_dir.is_file():
            raise OSError(
                f"Cannot create directory '{self.daily_listings_dir}': A file with that name already exists. "
                f"Please remove or rename the file at {self.daily_listings_dir.absolute()}"
            )
        self.daily_listings_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Bronze storage initialized: {base_path}")
    
    def save_raw_match_data(
        self,
        match_id: str,
        raw_data: Dict[str, Any],
        date_str: Optional[str] = None
    ) -> Path:
        """
        Save raw API response to Bronze layer.
        
        Bronze layer = One API call = One JSON file = Complete or Failed
        
        Args:
            match_id: Match ID
            raw_data: Raw API response (dict) - complete response from one API call
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            Path to saved file
        """
        try:
            # Normalize date to YYYYMMDD format
            if date_str:
                # Convert YYYY-MM-DD to YYYYMMDD if needed
                if len(date_str) == 10 and '-' in date_str:
                    date_str_normalized = date_str.replace('-', '')
                elif len(date_str) == 8 and date_str.isdigit():
                    date_str_normalized = date_str
                else:
                    raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD or YYYY-MM-DD")
            else:
                date_str_normalized = datetime.now().strftime("%Y%m%d")
            
            date_dir = self.matches_dir / date_str_normalized
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # ATOMIC FILE WRITE: Write to temp, verify, then rename
            file_path = date_dir / f"match_{match_id}.json"
            temp_path = date_dir / f".match_{match_id}.json.tmp"
            scraped_at = datetime.now().isoformat()
            
            # Add metadata
            data_with_metadata = {
                "match_id": match_id,
                "scraped_at": scraped_at,
                "date": date_str_normalized,
                "data": raw_data
            }
            
            try:
                # Step 1: Write to temporary file
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(data_with_metadata, f, indent=2, ensure_ascii=False)
                
                # Step 2: Verify the file is valid JSON (can be read back)
                with open(temp_path, 'r', encoding='utf-8') as f:
                    json.load(f)  # Throws JSONDecodeError if corrupt
                
                # Step 3: Atomic rename (replaces existing file if present)
                temp_path.replace(file_path)
                
                # Step 4: Get file size AFTER successful write
                file_size_kb = os.path.getsize(file_path) / 1024
                
                # Step 5: Record data lineage
                try:
                    self.lineage_tracker.record_scrape(
                        scraper="aiscore",
                        source="aiscore_web",
                        source_id=match_id,
                        date=date_str_normalized,
                        file_path=file_path,
                        metadata={
                            "file_size_kb": round(file_size_kb, 2),
                            "scraped_at": scraped_at
                        }
                    )
                except Exception as e:
                    self.logger.warning(f"Could not record lineage for {match_id}: {e}")
                
                self.logger.debug(f"Saved raw data for match {match_id} to {file_path} ({file_size_kb:.2f} KB)")
                return file_path
                
            except Exception as write_error:
                # Clean up temp file if it exists
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except:
                        pass
                raise write_error
            
        except Exception as e:
            # Failure tracking removed - use data lineage instead
            self.logger.error(f"Error saving raw data for match {match_id}: {e}", exc_info=True)
            raise
    
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
        # Build complete match data
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
                "odds_over_under_goals": len([o for o in odds_over_under if o.get('market_type') == 'goals']),
                "odds_over_under_corners": len([o for o in odds_over_under if o.get('market_type') == 'corners']),
                "total_odds": len(odds_1x2) + len(odds_asian_handicap) + len(odds_over_under)
            }
        }
        
        # Use scrape_date for file location (when it was scraped)
        file_path = self.save_raw_match_data(match_id, match_data, scrape_date)
        
        self.logger.info(f"Complete match saved: {match_id} to {file_path}")
        
        return file_path
    
    def load_raw_match_data(
        self,
        match_id: str,
        date_str: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Load raw match data from Bronze layer.
        Supports: tar archive, individual .json.gz, and .json files.
        
        Args:
            match_id: Match ID
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            Raw match data dict, or None if not found
        """
        try:
            if date_str:
                # Normalize date to YYYYMMDD format
                if len(date_str) == 10 and '-' in date_str:
                    date_str_normalized = date_str.replace('-', '')
                elif len(date_str) == 8 and date_str.isdigit():
                    date_str_normalized = date_str
                else:
                    self.logger.warning(f"Invalid date format: {date_str}, trying as-is")
                    date_str_normalized = date_str
                
                date_dir = self.matches_dir / date_str_normalized
                
                # Try 1: Extract from tar archive containing .json.gz files (new format)
                archive_path = date_dir / f"{date_str_normalized}_matches.tar"
                if archive_path.exists():
                    try:
                        with tarfile.open(archive_path, 'r') as tar:
                            member_name = f"match_{match_id}.json.gz"
                            try:
                                member = tar.getmember(member_name)
                                f = tar.extractfile(member)
                                if f:
                                    # Decompress the .json.gz content
                                    with gzip.open(io.BytesIO(f.read()), 'rt', encoding='utf-8') as gz:
                                        data = json.load(gz)
                                    return data  # Return full data structure (with 'data' key)
                            except KeyError:
                                # File not in archive
                                pass
                    except Exception as e:
                        self.logger.error(f"Error reading archive {archive_path}: {e}")
                
                # Try 2: Individual .json.gz file (old compressed format)
                file_path_gz = date_dir / f"match_{match_id}.json.gz"
                if file_path_gz.exists():
                    try:
                        with gzip.open(file_path_gz, 'rt', encoding='utf-8') as f:
                            return json.load(f)
                    except Exception as e:
                        self.logger.error(f"Error reading gzip file {file_path_gz}: {e}")
                
                # Try 3: Individual .json file (uncompressed)
                file_path = date_dir / f"match_{match_id}.json"
                if file_path.exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
            else:
                # Search all date directories
                for date_dir in self.matches_dir.iterdir():
                    if date_dir.is_dir():
                        # Try tar archive first
                        archive_path = date_dir / f"{date_dir.name}_matches.tar"
                        if archive_path.exists():
                            try:
                                with tarfile.open(archive_path, 'r') as tar:
                                    member_name = f"match_{match_id}.json.gz"
                                    try:
                                        member = tar.getmember(member_name)
                                        f = tar.extractfile(member)
                                        if f:
                                            with gzip.open(io.BytesIO(f.read()), 'rt', encoding='utf-8') as gz:
                                                return json.load(gz)
                                    except KeyError:
                                        pass
                            except Exception:
                                pass
                        
                        # Try .json.gz file
                        file_path_gz = date_dir / f"match_{match_id}.json.gz"
                        if file_path_gz.exists():
                            try:
                                with gzip.open(file_path_gz, 'rt', encoding='utf-8') as f:
                                    return json.load(f)
                            except Exception:
                                pass
                        
                        # Try .json file
                        file_path = date_dir / f"match_{match_id}.json"
                        if file_path.exists():
                            with open(file_path, 'r', encoding='utf-8') as f:
                                return json.load(f)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading raw data for match {match_id}: {e}", exc_info=True)
            return None
    
    def match_exists(self, match_id: str, date_str: Optional[str] = None) -> bool:
        """
        Check if a match file exists.
        
        Args:
            match_id: Match ID
            date_str: Optional date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            True if raw data exists, False otherwise
        """
        if date_str:
            # Normalize date to YYYYMMDD format
            if len(date_str) == 10 and '-' in date_str:
                date_str_normalized = date_str.replace('-', '')
            elif len(date_str) == 8 and date_str.isdigit():
                date_str_normalized = date_str
            else:
                date_str_normalized = date_str  # Try as-is
            
            date_dir = self.matches_dir / date_str_normalized
            file_path = date_dir / f"match_{match_id}.json"
            return file_path.exists()
        else:
            # Search all date directories
            for date_dir in self.matches_dir.iterdir():
                if date_dir.is_dir():
                    file_path = date_dir / f"match_{match_id}.json"
                    if file_path.exists():
                        return True
            return False
    
    def date_has_matches(self, date_str: str) -> bool:
        """
        Check if any matches exist for a given date.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            True if any match files exist for the date, False otherwise
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            date_str_normalized = date_str  # Try as-is
        
        date_dir = self.matches_dir / date_str_normalized
        if not date_dir.exists():
            return False
        
        # Check if any match files exist
        return any(date_dir.glob("match_*.json"))
    
    def list_matches_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        """
        List all matches for a given date by scanning the matches directory.
        This replaces read_daily_list functionality.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            List of match data dictionaries
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            date_str_normalized = date_str  # Try as-is
        
        date_dir = self.matches_dir / date_str_normalized
        if not date_dir.exists():
            return []
        
        matches = []
        for file_path in date_dir.glob("match_*.json"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    match_data = json.load(f)
                    # Extract match info from the data structure
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
    
    # Backward compatibility methods (deprecated - use new structure)
    def read_daily_list(self, scrape_date: str) -> Optional[Dict[str, Any]]:
        """
        DEPRECATED: Daily listings removed. Use list_matches_for_date() instead.
        
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
    
    def read_manifest(self, game_date: str, match_id: str) -> Optional[Dict[str, Any]]:
        """
        DEPRECATED: Manifests removed. Use load_raw_match_data() instead.
        
        Returns match data in a format compatible with old code.
        """
        match_data = self.load_raw_match_data(match_id, game_date)
        if not match_data:
            return None
        
        data = match_data.get("data", {})
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
    
    def read_odds_data(self, game_date: str, match_id: str) -> Optional[Dict[str, Any]]:
        """
        DEPRECATED: Use load_raw_match_data() instead.
        
        Returns odds data from the match file.
        """
        match_data = self.load_raw_match_data(match_id, game_date)
        if not match_data:
            return None
        
        data = match_data.get("data", {})
        return {
            "match_id": match_id,
            "match_url": data.get("match_url", ""),
            "game_date": game_date,
            "scrape_timestamp": match_data.get("scraped_at"),
            "odds_1x2": data.get("odds_1x2", []),
            "odds_asian_handicap": data.get("odds_asian_handicap", []),
            "odds_over_under": data.get("odds_over_under", [])
        }
    
    def save_daily_listing(self, date_str: str, match_ids: List[int]) -> Path:
        """
        Save daily listing of match IDs for a date with comprehensive metadata.
        Used to track games and prevent duplicate scraping.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
            match_ids: List of match IDs for the date
        
        Returns:
            Path to saved daily listing file
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD or YYYY-MM-DD")
        
        date_dir = self.daily_listings_dir / date_str_normalized
        date_dir.mkdir(parents=True, exist_ok=True)
        
        listing_file = date_dir / "matches.json"
        
        # Gather storage statistics
        matches_date_dir = self.matches_dir / date_str_normalized
        storage_stats = self._get_storage_stats(date_str_normalized, match_ids, matches_date_dir)
        
        listing_data = {
            "date": date_str_normalized,
            "scraped_at": datetime.now().isoformat(),
            "match_ids": [int(mid) for mid in match_ids],
            "total_matches": len(match_ids),
            "storage": storage_stats
        }
        
        # Atomic write
        temp_file = date_dir / ".matches.json.tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(listing_data, f, indent=2, ensure_ascii=False)
            temp_file.replace(listing_file)
            self.logger.info(
                f"Saved daily listing for {date_str_normalized}: {len(match_ids)} matches, "
                f"{storage_stats['files_stored']} files stored ({storage_stats['total_size_mb']:.2f} MB)"
            )
            return listing_file
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise
    
    def _get_storage_stats(
        self, 
        date_str: str, 
        match_ids: List, 
        matches_date_dir: Path
    ) -> Dict[str, Any]:
        """
        Gather comprehensive storage statistics for a date.
        
        Args:
            date_str: Date string in YYYYMMDD format
            match_ids: List of match IDs (can be strings or integers)
            matches_date_dir: Path to matches directory for the date
        
        Returns:
            Dictionary with storage statistics
        """
        stats = {
            "files_stored": 0,
            "files_missing": 0,
            "total_size_bytes": 0,
            "total_size_mb": 0.0,
            "files_in_archive": 0,
            "files_individual": 0,
            "archive_size_bytes": 0,
            "archive_size_mb": 0.0,
            "scraped_match_ids": [],
            "missing_match_ids": []
        }
        
        if not matches_date_dir.exists():
            stats["files_missing"] = len(match_ids)
            stats["missing_match_ids"] = [str(mid) for mid in match_ids]
            return stats
        
        # Check for tar archive
        archive_path = matches_date_dir / f"{date_str}_matches.tar"
        if archive_path.exists():
            archive_size = archive_path.stat().st_size
            stats["archive_size_bytes"] = archive_size
            stats["archive_size_mb"] = archive_size / (1024 * 1024)
            
            # Count files in archive
            try:
                with tarfile.open(archive_path, 'r') as tar:
                    archive_files = [m.name for m in tar.getmembers() if m.name.startswith("match_") and m.name.endswith(".json.gz")]
                    stats["files_in_archive"] = len(archive_files)
            except Exception as e:
                self.logger.warning(f"Error reading archive {archive_path}: {e}")
        
        # Check individual files (AIScore uses .json files)
        for match_id in match_ids:
            match_id_str = str(match_id)
            file_path = matches_date_dir / f"match_{match_id_str}.json"
            
            if file_path.exists():
                stats["files_individual"] += 1
                file_size = file_path.stat().st_size
                stats["total_size_bytes"] += file_size
                stats["scraped_match_ids"].append(match_id_str)
            else:
                stats["files_missing"] += 1
                stats["missing_match_ids"].append(match_id_str)
        
        stats["files_stored"] = stats["files_individual"] + stats["files_in_archive"]
        stats["total_size_mb"] = stats["total_size_bytes"] / (1024 * 1024)
        
        # Calculate completion percentage
        if len(match_ids) > 0:
            stats["completion_percentage"] = round(
                (stats["files_stored"] / len(match_ids)) * 100, 2
            )
        else:
            stats["completion_percentage"] = 0.0
        
        return stats
    
    def load_daily_listing(self, date_str: str) -> Optional[Dict[str, Any]]:
        """
        Load daily listing of match IDs for a date.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            Daily listing dict, or None if not found
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            return None
        
        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"
        if not listing_file.exists():
            return None
        
        try:
            with open(listing_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading daily listing for {date_str}: {e}")
            return None
    
    def daily_listing_exists(self, date_str: str) -> bool:
        """
        Check if daily listing exists for a date.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            True if daily listing exists, False otherwise
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            return False
        
        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"
        return listing_file.exists()
    
    def get_match_ids_for_date(self, date_str: str) -> List[int]:
        """
        Get list of match IDs for a date from daily listing.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            List of match IDs, or empty list if not found
        """
        listing = self.load_daily_listing(date_str)
        if listing:
            # Support both structures: match_ids array or matches array
            if "match_ids" in listing:
                return listing.get("match_ids", [])
            elif "matches" in listing:
                return [m.get("match_id") for m in listing.get("matches", []) if m.get("match_id")]
        return []
    
    def update_storage_statistics_in_daily_list(self, date_str: str) -> bool:
        """
        Update storage statistics in the daily listing file based on actual files.
        This recalculates storage stats after scraping completes.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            True if update was successful, False otherwise
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            self.logger.warning(f"Invalid date format: {date_str}")
            return False
        
        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"
        
        if not listing_file.exists():
            self.logger.debug(f"Daily listing file not found: {listing_file}")
            return False
        
        try:
            # Load existing data
            with open(listing_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Get match IDs from the listing (support both structures)
            match_ids = []
            if "match_ids" in data:
                match_ids = [str(mid) for mid in data.get("match_ids", [])]
            elif "matches" in data:
                match_ids = [str(m.get("match_id")) for m in data.get("matches", []) if m.get("match_id")]
            
            if not match_ids:
                self.logger.warning(f"No match IDs found in daily listing for {date_str_normalized}")
                return False
            
            # Calculate storage statistics
            matches_date_dir = self.matches_dir / date_str_normalized
            storage_stats = self._get_storage_stats(date_str_normalized, match_ids, matches_date_dir)
            
            # Update storage statistics in the data
            data["storage"] = storage_stats
            
            # Update scraped_at timestamp
            data["scraped_at"] = datetime.now().isoformat()
            
            # Atomic write
            temp_file = listing_file.parent / ".matches.json.tmp"
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                
                # Verify JSON is valid
                with open(temp_file, 'r', encoding='utf-8') as f:
                    json.load(f)  # Will raise if invalid
                
                # Atomic rename (Windows-safe)
                if listing_file.exists():
                    listing_file.unlink()
                temp_file.rename(listing_file)
                
                self.logger.info(
                    f"Updated storage statistics for {date_str_normalized}: "
                    f"{storage_stats['files_stored']}/{len(match_ids)} files stored "
                    f"({storage_stats['total_size_mb']:.2f} MB, {storage_stats['completion_percentage']:.2f}% complete)"
                )
                return True
            except Exception as write_e:
                self.logger.error(f"Error writing storage statistics update: {write_e}")
                if temp_file.exists():
                    temp_file.unlink()
                raise
            
        except Exception as e:
            self.logger.error(f"Error updating storage statistics for {date_str}: {e}")
            return False
    
    def update_match_status_in_daily_list(self, date_str: str, match_id: str, status: str) -> bool:
        """
        Update the scrape_status for a match in the daily listing file.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
            match_id: Match ID to update
            status: New status (e.g., 'success', 'failed', 'pending', 'partial')
        
        Returns:
            True if update was successful, False otherwise
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            self.logger.warning(f"Invalid date format: {date_str}")
            return False
        
        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"
        
        if not listing_file.exists():
            self.logger.debug(f"Daily listing file not found: {listing_file}")
            return False
        
        try:
            # Load existing data
            with open(listing_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Find and update the match
            matches = data.get('matches', [])
            match_found = False
            
            # Normalize match_id to string for comparison (handle both str and int)
            match_id_str = str(match_id)
            
            for match in matches:
                # Compare as strings to handle type mismatches
                match_id_in_list = str(match.get('match_id', ''))
                if match_id_in_list == match_id_str:
                    match['scrape_status'] = status
                    match_found = True
                    self.logger.info(f"Updated match {match_id} status to '{status}' in daily listing")
                    break
            
            if not match_found:
                self.logger.warning(f"Match {match_id} not found in daily listing for {date_str_normalized} (searched {len(matches)} matches)")
                # Debug: log first few match IDs for troubleshooting
                if matches:
                    sample_ids = [str(m.get('match_id', 'N/A')) for m in matches[:5]]
                    self.logger.debug(f"Sample match IDs in listing: {sample_ids}")
                return False
            
            # Atomic write
            temp_file = listing_file.parent / ".matches.json.tmp"
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                
                # Verify JSON is valid
                with open(temp_file, 'r', encoding='utf-8') as f:
                    json.load(f)  # Will raise if invalid
                
                # Atomic rename (Windows-safe)
                if listing_file.exists():
                    listing_file.unlink()
                temp_file.rename(listing_file)
                
                # Verify the update was persisted
                with open(listing_file, 'r', encoding='utf-8') as f:
                    verify_data = json.load(f)
                    verify_matches = verify_data.get('matches', [])
                    for verify_match in verify_matches:
                        if str(verify_match.get('match_id', '')) == match_id_str:
                            if verify_match.get('scrape_status') == status:
                                self.logger.debug(f"Verified: Match {match_id} status '{status}' persisted successfully")
                            else:
                                self.logger.warning(f"Warning: Match {match_id} status mismatch - expected '{status}', got '{verify_match.get('scrape_status')}'")
                            break
                
                return True
            except Exception as write_e:
                self.logger.error(f"Error writing status update: {write_e}")
                raise
            
        except Exception as e:
            self.logger.error(f"Error updating match status for {match_id}: {e}")
            # Clean up temp file if it exists
            temp_file = listing_file.parent / ".matches.json.tmp"
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            return False
    
    def compress_date_files(self, date_str: str, force: bool = False) -> Dict[str, Any]:
        """
        Compress all JSON files for a specific date:
        Step 1: Compress each .json to .json.gz (GZIP compression)
        Step 2: Bundle all .json.gz into ONE tar archive
        Step 3: Delete individual .json.gz files
        
        Final result: ONE tar file containing all compressed .json.gz files
        
        RESUMABLE: If archive already exists, skips compression unless force=True.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
            force: If True, recompress even if archive exists (default: False)
        
        Returns:
            Dictionary with compression statistics and processing log
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD or YYYY-MM-DD")
        
        date_str = date_str_normalized
        
        date_dir = self.matches_dir / date_str
        archive_path = date_dir / f"{date_str}_matches.tar"
        
        # RESUMABILITY: Check if already compressed
        if not force and archive_path.exists():
            archive_size_mb = archive_path.stat().st_size / (1024 * 1024)
            self.logger.info(f"Archive already exists: {archive_path} ({archive_size_mb:.2f} MB), skipping compression")
            return {
                "compressed": 0,
                "size_before_mb": 0,
                "size_after_mb": archive_size_mb,
                "saved_mb": 0,
                "saved_pct": 0,
                "archive_file": str(archive_path),
                "status": "already_compressed"
            }
        
        if not date_dir.exists():
            self.logger.warning(f"Date directory not found: {date_dir}")
            return {"compressed": 0, "size_before_mb": 0, "size_after_mb": 0, "archive_file": None, "status": "no_directory"}
        
        json_files = list(date_dir.glob("match_*.json"))
        existing_gz_files = list(date_dir.glob("match_*.json.gz"))
        
        # Calculate total size before
        all_files = json_files + existing_gz_files
        if not all_files:
            self.logger.info(f"No files to compress in {date_dir}")
            return {"compressed": 0, "size_before_mb": 0, "size_after_mb": 0, "archive_file": None, "status": "no_files"}
        
        total_before = sum(f.stat().st_size for f in all_files)
        
        try:
            # STEP 1: Compress .json files to .json.gz (if any exist)
            gz_files = list(existing_gz_files)  # Start with existing .json.gz
            
            if json_files:
                self.logger.info(f"Step 1: Compressing {len(json_files)} .json files to .json.gz...")
                for json_file in json_files:
                    # Read original
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Write compressed (compact format, no indentation)
                    gz_file = json_file.with_suffix('.json.gz')
                    with gzip.open(gz_file, 'wt', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False)
                    
                    gz_files.append(gz_file)
                    
                    # Remove original JSON file
                    json_file.unlink()
            else:
                self.logger.info(f"Step 1: Using {len(existing_gz_files)} existing .json.gz files...")
            
            self.logger.info(f"Step 2: Bundling {len(gz_files)} .json.gz files into single tar archive...")
            
            # STEP 2: Create tar archive with all .json.gz files (NO additional compression)
            archive_path = date_dir / f"{date_str}_matches.tar"
            
            with tarfile.open(archive_path, 'w') as tar:  # 'w' = no compression, just tar
                for gz_file in gz_files:
                    # Add .json.gz file to archive with just the filename
                    tar.add(gz_file, arcname=gz_file.name)
            
            # Get final archive size
            total_after = archive_path.stat().st_size
            
            # TAR VERIFICATION: Verify archive integrity BEFORE deleting originals
            self.logger.info(f"Verifying tar archive integrity...")
            try:
                with tarfile.open(archive_path, 'r') as tar:
                    members = tar.getmembers()
                    member_count = len(members)
                    self.logger.info(f"[OK] Tar archive verified: {member_count} files intact")
            except Exception as verify_error:
                self.logger.error(f"[FAILED] Tar archive verification failed: {verify_error}")
                # Don't delete originals if verification fails
                if archive_path.exists():
                    archive_path.unlink()
                raise
            
            # STEP 3: ONLY NOW delete individual .json.gz files (archive is verified)
            self.logger.info(f"Step 3: Cleaning up {len(gz_files)} individual .json.gz files...")
            for gz_file in gz_files:
                if gz_file.exists():
                    gz_file.unlink()
            
            # Calculate savings
            saved_bytes = total_before - total_after
            saved_mb = saved_bytes / (1024 * 1024)
            saved_pct = (saved_bytes / total_before * 100) if total_before > 0 else 0
            
            size_before_mb = total_before / (1024 * 1024)
            size_after_mb = total_after / (1024 * 1024)
            
            self.logger.info(
                f"[OK] Archive complete for {date_str}: "
                f"{len(gz_files)} files -> {archive_path.name}, "
                f"{size_before_mb:.2f} MB -> {size_after_mb:.2f} MB "
                f"(saved {saved_mb:.2f} MB, {saved_pct:.1f}%)"
            )
            
            return {
                "compressed": len(gz_files),
                "size_before_mb": round(size_before_mb, 2),
                "size_after_mb": round(size_after_mb, 2),
                "saved_mb": round(saved_mb, 2),
                "saved_pct": round(saved_pct, 1),
                "archive_file": str(archive_path),
                "status": "success"
            }
            
        except Exception as e:
            self.logger.error(f"Error during compression for {date_str}: {e}", exc_info=True)
            return {
                "compressed": 0,
                "size_before_mb": 0,
                "size_after_mb": 0,
                "saved_mb": 0,
                "saved_pct": 0,
                "archive_file": None,
                "status": "error",
                "error": str(e)
            }

