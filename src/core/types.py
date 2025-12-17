"""Common type definitions and aliases for Scout project.

This module provides type aliases and custom types used throughout the application.
Using type aliases improves code readability and makes refactoring easier.

Usage:
    from src.core.types import MatchID, DateStr, JSONDict
    
    def process_match(match_id: MatchID, date: DateStr) -> JSONDict:
        ...
"""

from typing import Dict, List, Any, Union, Optional, TypedDict, Literal
from pathlib import Path
from datetime import datetime


# =============================================================================
# Basic Type Aliases
# =============================================================================

# String types
MatchID = str  # Unique match identifier (numeric for FotMob, alphanumeric for AIScore)
DateStr = str  # Date string (YYYYMMDD or YYYY-MM-DD format)
URL = str  # URL string
FilePath = Union[str, Path]  # File path (string or Path object)

# Data structures
JSONDict = Dict[str, Any]  # Generic JSON dictionary
JSONList = List[Any]  # Generic JSON list
Headers = Dict[str, str]  # HTTP headers dictionary
QueryParams = Dict[str, Union[str, int, float, bool]]  # URL query parameters

# Numeric types
Score = str  # Match score (e.g., "2-1", "0-0")
Percentage = float  # Percentage value (0.0 to 100.0)
Timestamp = Union[datetime, str]  # Timestamp (datetime object or ISO string)


# =============================================================================
# Status Types
# =============================================================================

MatchStatusType = Literal[
    "Finished",
    "FullTime",
    "FT",
    "After Extra Time",
    "AET",
    "After Penalties",
    "AP",
    "Live",
    "Not Started",
    "Postponed",
    "Cancelled",
    "Abandoned",
]

ScrapeStatus = Literal[
    "success",
    "partial",
    "failed",
    "pending",
    "timeout",
    "error",
]

Environment = Literal[
    "development",
    "staging",
    "production",
    "testing",
]


# =============================================================================
# Structured Types (TypedDict)
# =============================================================================

class TeamData(TypedDict, total=False):
    """Team information structure."""
    name: str
    id: str
    country: Optional[str]
    logo: Optional[str]


class MatchMetadata(TypedDict, total=False):
    """Match metadata structure."""
    match_id: MatchID
    date: DateStr
    scraped_at: str
    scraper: str
    source: str
    status: ScrapeStatus


class ScraperMetrics(TypedDict, total=False):
    """Scraper execution metrics."""
    total_matches: int
    successful_matches: int
    failed_matches: int
    skipped_matches: int
    duration_seconds: float
    start_time: str
    end_time: str
    errors: List[str]


class StorageStats(TypedDict, total=False):
    """Storage statistics structure."""
    files_stored: int
    files_missing: int
    total_size_bytes: int
    total_size_mb: float
    files_in_archive: int
    files_individual: int
    completion_percentage: float


class HealthCheckResult(TypedDict, total=False):
    """Health check result structure."""
    check: str
    status: Literal["OK", "WARNING", "ERROR"]
    message: str
    details: Optional[JSONDict]


class OddsData(TypedDict, total=False):
    """Odds data structure."""
    bookmaker: str
    odds_home: Optional[float]
    odds_draw: Optional[float]
    odds_away: Optional[float]
    timestamp: str


class PlayerData(TypedDict, total=False):
    """Player data structure."""
    player_id: str
    name: str
    number: Optional[int]
    position: Optional[str]
    team: Optional[str]


class EventData(TypedDict, total=False):
    """Match event data structure."""
    event_id: str
    type: str
    minute: int
    player: Optional[str]
    team: Optional[str]
    description: Optional[str]


# =============================================================================
# Function Return Types
# =============================================================================

MatchDataResult = Optional[JSONDict]  # Match data or None if not found
MatchListResult = List[MatchID]  # List of match IDs
ValidationResult = List[str]  # List of validation error messages (empty if valid)
ProcessingResult = Union[JSONDict, None]  # Processed data or None if processing failed


# =============================================================================
# Configuration Types
# =============================================================================

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

RetryableStatusCodes = tuple[int, ...]  # HTTP status codes that should trigger retry


# =============================================================================
# Database Types
# =============================================================================

DatabaseRow = Dict[str, Any]  # Single database row
DatabaseRows = List[DatabaseRow]  # Multiple database rows
QueryResult = Optional[DatabaseRows]  # Query result or None if no results


# =============================================================================
# Type Guards and Validators
# =============================================================================

def is_valid_match_id(value: Any) -> bool:
    """Check if value is a valid match ID.
    
    Args:
        value: Value to check
        
    Returns:
        True if valid match ID, False otherwise
    """
    if not isinstance(value, str):
        return False
    return len(value) > 0 and (value.isdigit() or value.isalnum())


def is_valid_date_str(value: Any) -> bool:
    """Check if value is a valid date string.
    
    Args:
        value: Value to check
        
    Returns:
        True if valid date string, False otherwise
    """
    if not isinstance(value, str):
        return False
    
    # Check YYYYMMDD format
    if len(value) == 8 and value.isdigit():
        return True
    
    # Check YYYY-MM-DD format
    if len(value) == 10 and value[4] == '-' and value[7] == '-':
        parts = value.split('-')
        return all(part.isdigit() for part in parts)
    
    return False


def is_valid_url(value: Any) -> bool:
    """Check if value is a valid URL.
    
    Args:
        value: Value to check
        
    Returns:
        True if valid URL, False otherwise
    """
    if not isinstance(value, str):
        return False
    return value.startswith(('http://', 'https://'))


__all__ = [
    # Basic aliases
    'MatchID',
    'DateStr',
    'URL',
    'FilePath',
    'JSONDict',
    'JSONList',
    'Headers',
    'QueryParams',
    'Score',
    'Percentage',
    'Timestamp',
    # Status types
    'MatchStatusType',
    'ScrapeStatus',
    'Environment',
    # Structured types
    'TeamData',
    'MatchMetadata',
    'ScraperMetrics',
    'StorageStats',
    'HealthCheckResult',
    'OddsData',
    'PlayerData',
    'EventData',
    # Function return types
    'MatchDataResult',
    'MatchListResult',
    'ValidationResult',
    'ProcessingResult',
    # Configuration types
    'LogLevel',
    'RetryableStatusCodes',
    # Database types
    'DatabaseRow',
    'DatabaseRows',
    'QueryResult',
    # Validators
    'is_valid_match_id',
    'is_valid_date_str',
    'is_valid_url',
]
