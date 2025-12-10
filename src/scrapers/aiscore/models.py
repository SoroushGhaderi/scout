"""Data models for the scraper."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum


class ScrapingStatus(Enum):
    """Scraping status enum."""
    PENDING = 0
    SCRAPED = 1
    FAILED = 2


@dataclass
class MatchLk:
    """Value object for match link."""
    url: str
    match_id: str
    source_date: str
    discovered_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        """Validate and normalize data."""
        if not self.url:
            raise ValueError("URL can not be empty")

        if not self.match_id:
            self.match_id = self._extract_match_id()

        if not self.source_date:
            raise ValueError("Source date can not be empty")

    def _extract_match_id(self) -> str:
        """Extract match ID from URL."""
        parts = self.url.split('/')

        match_id = parts[-1] if parts else ''

        invalid = ['h2h', 'statistics', 'odds', 'predictions', 'lineups']
        if match_id.lower() in invalid and len(parts) >= 2:
            match_id = parts[-2]

        return match_id

    def to_tuple(self) -> tuple:
        """Convert to tuple (legacy method, not used)."""
        return (
            self.url,
            self.match_id,
            self.source_date,
            self.discovered_at.strftime('%Y-%m-%d %H:%M:%S')
        )


@dataclass
class OddsData:
    """Value object for odds data."""
    match_id: str
    match_url: str
    bookmaker: str
    home_odds: Optional[float] = None
    draw_odds: Optional[float] = None
    away_odds: Optional[float] = None
    odds_type: Optional[str] = None
    additional_info: Optional[str] = None
    scraped_at: datetime = field(default_factory=datetime.now)

    def to_tuple(self) -> tuple:
        """Convert to tuple (legacy method, not used)."""
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
    """1X2 Match Result Odds."""
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
    """Asian Handicap Odds - 6 metrics format."""
    match_id: str
    match_url: str
    match_time: str
    moment_result: str
    home_handicap: Optional[str] = None
    home_odds: Optional[float] = None
    away_handicap: Optional[str] = None
    away_odds: Optional[float] = None
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
    """Over/Under (Total Goals/Corners) Odds."""
    match_id: str
    match_url: str
    bookmaker: str
    match_time: Optional[str] = None
    moment_result: Optional[str] = None
    total_line: Optional[float] = None
    over_odds: Optional[float] = None
    under_odds: Optional[float] = None
    market_type: str = "goals"
    scraped_at: datetime = field(default_factory=datetime.now)

    def to_tuple(self) -> tuple:
        return (
            self.match_id, self.match_url, self.bookmaker,
            self.match_time, self.moment_result,
            self.total_line, self.over_odds, self.under_odds,
            self.market_type, self.scraped_at.strftime('%Y-%m-%d %H:%M:%S')
        )


@dataclass
class ScrapingResult:
    """Result of a scraping operation."""
    success: bool
    links_found: int
    links_inserted: int
    duplicates: int
    errors: int
    duration: float
    error_message: Optional[str] = None

    def __str__(self) -> str:
        status = "[SUCCESS]" if self.success else "[FAILED]"
        return (
            f"{status}\n"
            f" Links found: {self.links_found}\n"
            f" Links inserted: {self.links_inserted}\n"
            f" Duplicates: {self.duplicates}\n"
            f" Errors: {self.errors}\n"
            f" Duration: {self.duration:.2f}s"
        )
