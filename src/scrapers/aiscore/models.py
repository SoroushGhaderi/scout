"""Data models for the scraper"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum


class ScrapingStatus(Enum):
    """Scraping status enum"""
    PENDING = 0
    SCRAPED = 1
    FAILED = 2


@dataclass
class MatchLink:
    """Value object for match link"""
    url: str
    match_id: str
    source_date: str
    discovered_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Validate and normalize data"""
        if not self.url:
            raise ValueError("URL cannot be empty")
        
        if not self.match_id:
            self.match_id = self._extract_match_id()
        
        if not self.source_date:
            raise ValueError("Source date cannot be empty")
    
    def _extract_match_id(self) -> str:
        """Extract match ID from URL"""
        parts = self.url.split('/')
        
        # Get last part
        match_id = parts[-1] if parts else ''
        
        # Remove invalid suffixes
        invalid = ['h2h', 'statistics', 'odds', 'predictions', 'lineups']
        if match_id.lower() in invalid and len(parts) >= 2:
            match_id = parts[-2]
        
        return match_id
    
    def to_tuple(self) -> tuple:
        """Convert to tuple (legacy method, not used)"""
        return (
            self.url,
            self.match_id,
            self.source_date,
            self.discovered_at.strftime('%Y-%m-%d %H:%M:%S')
        )


@dataclass
class OddsData:
    """Value object for odds data"""
    match_id: str
    match_url: str
    bookmaker: str
    home_odds: Optional[float] = None
    draw_odds: Optional[float] = None
    away_odds: Optional[float] = None
    odds_type: Optional[str] = None  # e.g., "1X2", "Over/Under", etc.
    additional_info: Optional[str] = None
    scraped_at: datetime = field(default_factory=datetime.now)
    
    def to_tuple(self) -> tuple:
        """Convert to tuple (legacy method, not used)"""
        return (
            self.match_id,
            self.match_url,
            self.bookmaker,
            self.home_odds,
            self.draw_odds,
            self.away_odds,
            self.odds_type,
            self.additional_info,
            self.scraped_at.strftime('%Y-%m-%d %H:%M:%S')
        )


@dataclass
class Odds1X2:
    """1X2 Match Result Odds"""
    match_id: str
    match_url: str
    bookmaker: str
    home_odds: Optional[float] = None
    draw_odds: Optional[float] = None
    away_odds: Optional[float] = None
    scraped_at: datetime = field(default_factory=datetime.now)
    
    def to_tuple(self) -> tuple:
        return (
            self.match_id, self.match_url, self.bookmaker,
            self.home_odds, self.draw_odds, self.away_odds,
            self.scraped_at.strftime('%Y-%m-%d %H:%M:%S')
        )


@dataclass
class OddsAsianHandicap:
    """Asian Handicap Odds - 6 metrics format"""
    match_id: str
    match_url: str
    match_time: str  # e.g., "90+10'"
    moment_result: str  # e.g., "2-2"
    home_handicap: str  # e.g., "-0/0.5"
    home_odds: float
    away_handicap: str  # e.g., "+0/0.5"
    away_odds: float
    scraped_at: datetime = field(default_factory=datetime.now)
    
    def to_tuple(self) -> tuple:
        return (
            self.match_id, self.match_url, self.match_time,
            self.moment_result, self.home_handicap, self.home_odds,
            self.away_handicap, self.away_odds,
            self.scraped_at.strftime('%Y-%m-%d %H:%M:%S')
        )


@dataclass
class OddsOverUnder:
    """Over/Under (Total Goals/Corners) Odds"""
    match_id: str
    match_url: str
    bookmaker: str
    total_line: float  # e.g., 2.5, 3.0
    over_odds: Optional[float] = None
    under_odds: Optional[float] = None
    market_type: str = "goals"  # 'goals', 'corners', etc.
    scraped_at: datetime = field(default_factory=datetime.now)
    
    def to_tuple(self) -> tuple:
        return (
            self.match_id, self.match_url, self.bookmaker,
            self.total_line, self.over_odds, self.under_odds,
            self.market_type, self.scraped_at.strftime('%Y-%m-%d %H:%M:%S')
        )


@dataclass
class ScrapingResult:
    """Result of a scraping operation"""
    success: bool
    links_found: int
    links_inserted: int
    duplicates: int
    errors: int
    duration: float
    error_message: Optional[str] = None
    
    def __str__(self) -> str:
        # Use ASCII-safe characters for Windows compatibility
        status = "[SUCCESS]" if self.success else "[FAILED]"
        return (
            f"{status}\n"
            f"  Links found: {self.links_found}\n"
            f"  Links inserted: {self.links_inserted}\n"
            f"  Duplicates: {self.duplicates}\n"
            f"  Errors: {self.errors}\n"
            f"  Duration: {self.duration:.2f}s"
        )

