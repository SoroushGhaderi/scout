"""Bronze layer manifest generation for data lake architecture.

This module creates detailed JSON manifests for each scraped match,
storing raw data and metadata following data lakehouse patterns.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

from .models import (
    MatchLink,
    Odds1X2,
    OddsAsianHandicap,
    OddsOverUnder
)

logger = logging.getLogger(__name__)


@dataclass
class ScrapingManifest:
    """Manifest for bronze layer raw data with metadata.
    
    Attributes:
        match_id: Unique match identifier
        match_url: Full URL of the match
        game_date: Date of the game (YYYYMMDD format)
        scrape_timestamp: When scraping occurred (ISO format)
        scrape_duration: Duration in seconds
        scrape_status: success/partial/failed
        teams: Team names (home and away)
        match_result: Final score if available
        league: League/competition name
        odds_statistics: Count of odds by type
        odds_data: Complete odds data by market type
        errors: List of errors encountered
        metadata: Additional metadata
    """
    
    # Core identifiers
    match_id: str
    match_url: str
    game_date: str
    
    # Scraping metadata
    scrape_timestamp: str
    scrape_duration: float
    scrape_status: str  # success, partial, failed
    
    # Match information
    teams: Dict[str, str]  # home, away
    match_result: Optional[str]
    league: Optional[str]
    match_time: Optional[str]  # Match state: FT, HT, Live, etc.
    
    # Odds statistics
    odds_statistics: Dict[str, int]  # Count by type
    
    # Complete odds data
    odds_data: Dict[str, List[Dict[str, Any]]]
    
    # Error tracking
    errors: List[str]
    warnings: List[str]
    
    # Additional metadata
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert manifest to dictionary."""
        return asdict(self)
    
    def to_json(self, indent: int = 2) -> str:
        """Convert manifest to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


class ManifestWriter:
    """Writer for bronze layer manifests in data lake structure.
    
    Creates organized directory structure:
    bronze/
    └── YYYYMMDD/
        ├── match_12345.json
        ├── match_12346.json
        └── ...
    
    Each JSON file contains:
    - Match metadata (teams, result, league)
    - Scraping statistics
    - Complete odds data
    - Error tracking
    - Timestamps and duration
    
    Example:
        >>> writer = ManifestWriter("data/bronze")
        >>> manifest = writer.create_manifest(match_data, odds_data)
        >>> writer.write_manifest(manifest)
    """
    
    def __init__(self, base_path: str = "data/bronze"):
        """Initialize manifest writer.
        
        Args:
            base_path: Base directory for bronze layer
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"ManifestWriter initialized: {self.base_path}")
    
    def create_manifest(
        self,
        match_info: Dict[str, Any],
        odds_1x2: List[Odds1X2],
        odds_asian_handicap: List[OddsAsianHandicap],
        odds_over_under: List[OddsOverUnder],
        scrape_start: datetime,
        scrape_end: datetime,
        errors: List[str] = None,
        warnings: List[str] = None
    ) -> ScrapingManifest:
        """Create manifest from scraped data.
        
        Args:
            match_info: Basic match information (id, url, date, teams, etc.)
            odds_1x2: List of 1X2 odds objects
            odds_asian_handicap: List of Asian Handicap odds
            odds_over_under: List of Over/Under odds
            scrape_start: Scraping start time
            scrape_end: Scraping end time
            errors: List of errors encountered
            warnings: List of warnings
        
        Returns:
            ScrapingManifest with complete metadata
        """
        duration = (scrape_end - scrape_start).total_seconds()
        
        # Determine scrape status
        total_odds = len(odds_1x2) + len(odds_asian_handicap) + len(odds_over_under)
        if errors:
            status = "failed" if total_odds == 0 else "partial"
        else:
            status = "success"
        
        # Create odds statistics
        odds_stats = {
            "odds_1x2": len(odds_1x2),
            "odds_asian_handicap": len(odds_asian_handicap),
            "odds_over_under_goals": len([o for o in odds_over_under if o.market_type == "goals"]),
            "odds_over_under_corners": len([o for o in odds_over_under if o.market_type == "corners"]),
            "total_odds": total_odds
        }
        
        # Unique bookmakers
        bookmakers_1x2 = set(o.bookmaker for o in odds_1x2)
        bookmakers_ou = set(o.bookmaker for o in odds_over_under)
        all_bookmakers = bookmakers_1x2 | bookmakers_ou
        
        odds_stats["unique_bookmakers"] = len(all_bookmakers)
        odds_stats["bookmakers"] = sorted(list(all_bookmakers))
        
        # Asian Handicap unique time points
        if odds_asian_handicap:
            unique_times = set(o.match_time for o in odds_asian_handicap if o.match_time)
            odds_stats["asian_handicap_time_points"] = len(unique_times)
        
        # Convert odds to dictionaries
        odds_data = {
            "odds_1x2": [self._odds_to_dict(o) for o in odds_1x2],
            "odds_asian_handicap": [self._odds_to_dict(o) for o in odds_asian_handicap],
            "odds_over_under": [self._odds_to_dict(o) for o in odds_over_under]
        }
        
        # Create manifest
        manifest = ScrapingManifest(
            match_id=match_info.get("match_id", "unknown"),
            match_url=match_info.get("match_url", ""),
            game_date=match_info.get("game_date", "unknown"),
            scrape_timestamp=scrape_end.isoformat(),
            scrape_duration=duration,
            scrape_status=status,
            teams={
                "home": match_info.get("home_team", "Unknown"),
                "away": match_info.get("away_team", "Unknown")
            },
            match_result=match_info.get("match_result"),
            league=match_info.get("league"),
            match_time=match_info.get("match_time"),
            odds_statistics=odds_stats,
            odds_data=odds_data,
            errors=errors or [],
            warnings=warnings or [],
            metadata={
                "scraper_version": "2.0.0",
                "data_layer": "bronze",
                "source": "aiscore.com",
                "scrape_start": scrape_start.isoformat(),
                "scrape_end": scrape_end.isoformat(),
                "match_info_available": {
                    "teams": bool(match_info.get("home_team")),
                    "result": bool(match_info.get("match_result")),
                    "league": bool(match_info.get("league")),
                    "match_time": bool(match_info.get("match_time"))
                }
            }
        )
        
        return manifest
    
    def write_manifest(self, manifest: ScrapingManifest) -> Path:
        """Write manifest to JSON file in date-organized directory.
        
        Args:
            manifest: Scraping manifest to write
        
        Returns:
            Path to written file
        
        Example:
            >>> manifest = writer.create_manifest(...)
            >>> file_path = writer.write_manifest(manifest)
            >>> print(f"Written to {file_path}")
        """
        # Handle missing game_date
        if not manifest.game_date or manifest.game_date == "unknown":
            from datetime import datetime
            manifest.game_date = datetime.now().strftime("%Y%m%d")
            logger.warning(f"game_date was missing, using today: {manifest.game_date}")
        
        # Create date directory
        date_dir = self.base_path / manifest.game_date
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Create file path
        file_name = f"match_{manifest.match_id}.json"
        file_path = date_dir / file_name
        
        # Write JSON
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(manifest.to_json())
            
            logger.info(f"Manifest written: {file_path}")
            logger.info(f"  - Status: {manifest.scrape_status}")
            logger.info(f"  - Total odds: {manifest.odds_statistics['total_odds']}")
            logger.info(f"  - Duration: {manifest.scrape_duration:.2f}s")
            
            return file_path
        
        except Exception as e:
            logger.error(f"Failed to write manifest {file_path}: {e}")
            raise
    
    def _odds_to_dict(self, odds: Any) -> Dict[str, Any]:
        """Convert odds object to dictionary.
        
        Args:
            odds: Odds object (Odds1X2, OddsAsianHandicap, etc.)
        
        Returns:
            Dictionary representation
        """
        if hasattr(odds, '__dict__'):
            data = odds.__dict__.copy()
        elif hasattr(odds, 'to_dict'):
            data = odds.to_dict()
        else:
            data = asdict(odds)
        
        # Convert datetime objects to ISO strings
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        
        return data
    
    def read_manifest(self, game_date: str, match_id: str) -> Optional[ScrapingManifest]:
        """Read existing manifest from bronze layer.
        
        Args:
            game_date: Date in YYYYMMDD format
            match_id: Match identifier
        
        Returns:
            ScrapingManifest if file exists, None otherwise
        """
        file_path = self.base_path / game_date / f"match_{match_id}.json"
        
        if not file_path.exists():
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return ScrapingManifest(**data)
        
        except Exception as e:
            logger.error(f"Failed to read manifest {file_path}: {e}")
            return None
    
    def get_date_statistics(self, game_date: str) -> Dict[str, Any]:
        """Get statistics for all manifests in a date directory.
        
        Args:
            game_date: Date in YYYYMMDD format
        
        Returns:
            Dictionary with aggregated statistics
        """
        date_dir = self.base_path / game_date
        
        if not date_dir.exists():
            return {
                "game_date": game_date,
                "total_matches": 0,
                "error": "Date directory not found"
            }
        
        manifest_files = list(date_dir.glob("match_*.json"))
        
        if not manifest_files:
            return {
                "game_date": game_date,
                "total_matches": 0
            }
        
        # Aggregate statistics
        stats = {
            "game_date": game_date,
            "total_matches": len(manifest_files),
            "status_counts": {"success": 0, "partial": 0, "failed": 0},
            "total_odds": 0,
            "odds_by_type": {
                "odds_1x2": 0,
                "odds_asian_handicap": 0,
                "odds_over_under": 0
            },
            "total_bookmakers": set(),
            "average_duration": 0,
            "leagues": set()
        }
        
        total_duration = 0
        
        for file_path in manifest_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    manifest_data = json.load(f)
                
                # Status counts
                status = manifest_data.get("scrape_status", "unknown")
                if status in stats["status_counts"]:
                    stats["status_counts"][status] += 1
                
                # Odds statistics
                odds_stats = manifest_data.get("odds_statistics", {})
                stats["total_odds"] += odds_stats.get("total_odds", 0)
                
                for odds_type in ["odds_1x2", "odds_asian_handicap", "odds_over_under"]:
                    stats["odds_by_type"][odds_type] += odds_stats.get(odds_type, 0)
                
                # Bookmakers
                bookmakers = odds_stats.get("bookmakers", [])
                stats["total_bookmakers"].update(bookmakers)
                
                # Duration
                total_duration += manifest_data.get("scrape_duration", 0)
                
                # League
                league = manifest_data.get("league")
                if league:
                    stats["leagues"].add(league)
            
            except Exception as e:
                logger.warning(f"Failed to read manifest {file_path}: {e}")
                continue
        
        # Calculate averages
        if stats["total_matches"] > 0:
            stats["average_duration"] = total_duration / stats["total_matches"]
        
        # Convert sets to sorted lists
        stats["total_bookmakers"] = len(stats["total_bookmakers"])
        stats["unique_leagues"] = len(stats["leagues"])
        stats["leagues"] = sorted(list(stats["leagues"]))
        
        return stats
    
    def list_dates(self) -> List[str]:
        """List all available dates in bronze layer.
        
        Returns:
            List of dates in YYYYMMDD format
        """
        date_dirs = [d.name for d in self.base_path.iterdir() if d.is_dir()]
        return sorted(date_dirs, reverse=True)
    
    def get_manifest_path(self, game_date: str, match_id: str) -> Path:
        """Get path to manifest file.
        
        Args:
            game_date: Date in YYYYMMDD format
            match_id: Match identifier
        
        Returns:
            Path to manifest file
        """
        return self.base_path / game_date / f"match_{match_id}.json"

