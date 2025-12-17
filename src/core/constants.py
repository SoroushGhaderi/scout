"""Project-wide constants for Scout.

This module defines constants used throughout the application.
Centralizing constants improves maintainability and reduces magic numbers.

Categories:
- Date and Time Formats
- HTTP Status Codes
- File Extensions
- Match Statuses
- Scraper Names
- Storage Layers
"""

from enum import Enum


# =============================================================================
# Date and Time Formats
# =============================================================================

DATE_FORMAT_COMPACT = "%Y%m%d"  # 20241218
DATE_FORMAT_DISPLAY = "%Y-%m-%d"  # 2024-12-18
DATE_FORMAT_ISO = "%Y-%m-%dT%H:%M:%S"  # 2024-12-18T15:30:45
DATETIME_FORMAT_FILENAME = "%Y%m%d_%H%M%S"  # 20241218_153045


# =============================================================================
# HTTP Status Codes (Common)
# =============================================================================

class HttpStatus:
    """HTTP status codes commonly encountered."""
    OK = 200
    CREATED = 201
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    RATE_LIMITED = 429
    INTERNAL_ERROR = 500
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504


# =============================================================================
# File Extensions
# =============================================================================

class FileExtension:
    """File extensions used in the project."""
    JSON = ".json"
    JSON_GZ = ".json.gz"
    TAR = ".tar"
    PARQUET = ".parquet"
    CSV = ".csv"
    LOG = ".log"


# =============================================================================
# Match Statuses (FotMob)
# =============================================================================

class MatchStatus:
    """Match status values from FotMob API."""
    FINISHED = "Finished"
    FULL_TIME = "FullTime"
    FT = "FT"
    AFTER_EXTRA_TIME = "After Extra Time"
    AET = "AET"
    AFTER_PENALTIES = "After Penalties"
    AP = "AP"
    LIVE = "Live"
    NOT_STARTED = "Not Started"
    POSTPONED = "Postponed"
    CANCELLED = "Cancelled"
    ABANDONED = "Abandoned"
    
    # Completed match statuses
    COMPLETED_STATUSES = (
        FINISHED,
        FULL_TIME,
        FT,
        AFTER_EXTRA_TIME,
        AET,
        AFTER_PENALTIES,
        AP,
    )
    
    # Active match statuses
    ACTIVE_STATUSES = (LIVE,)
    
    # Pending match statuses
    PENDING_STATUSES = (NOT_STARTED,)
    
    # Cancelled/postponed statuses
    CANCELLED_STATUSES = (POSTPONED, CANCELLED, ABANDONED)


# =============================================================================
# Scraper Names
# =============================================================================

class Scraper:
    """Scraper identifiers."""
    FOTMOB = "fotmob"
    AISCORE = "aiscore"


# =============================================================================
# Storage Layers (Data Lake Architecture)
# =============================================================================

class StorageLayer:
    """Data lake layer names."""
    BRONZE = "bronze"  # Raw data
    SILVER = "silver"  # Cleaned/validated data
    GOLD = "gold"      # Aggregated/analytics-ready data


# =============================================================================
# Data Sources
# =============================================================================

class DataSource:
    """Data source identifiers."""
    FOTMOB_API = "fotmob_api"
    AISCORE_WEB = "aiscore_web"


# =============================================================================
# Default Values
# =============================================================================

class Defaults:
    """Default values for various operations."""
    
    # Timeouts (seconds)
    HTTP_TIMEOUT = 30
    DATABASE_TIMEOUT = 30
    BROWSER_PAGE_LOAD_TIMEOUT = 30
    BROWSER_ELEMENT_WAIT_TIMEOUT = 10
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 2.0
    RETRY_INITIAL_WAIT = 2.0
    RETRY_MAX_WAIT = 10.0
    
    # Rate limiting
    REQUEST_DELAY_MIN = 2.0
    REQUEST_DELAY_MAX = 4.0
    RATE_LIMIT_REQUESTS_PER_MINUTE = 30
    
    # Batch sizes
    BATCH_SIZE_MATCHES = 100
    BATCH_SIZE_DATABASE_INSERT = 1000
    
    # Cache TTL (seconds)
    CACHE_TTL_SHORT = 300      # 5 minutes
    CACHE_TTL_MEDIUM = 3600    # 1 hour
    CACHE_TTL_LONG = 86400     # 24 hours
    
    # File sizes
    MAX_LOG_FILE_SIZE_MB = 10
    MAX_BACKUP_COUNT = 5
    
    # Compression
    COMPRESSION_LEVEL = 6  # gzip compression level (1-9)


# =============================================================================
# Environment Variables (Keys)
# =============================================================================

class EnvVar:
    """Environment variable names."""
    
    # General
    ENVIRONMENT = "ENVIRONMENT"
    LOG_LEVEL = "LOG_LEVEL"
    DATA_DIR = "DATA_DIR"
    
    # FotMob
    FOTMOB_X_MAS_TOKEN = "FOTMOB_X_MAS_TOKEN"
    FOTMOB_USER_AGENT = "FOTMOB_USER_AGENT"
    FOTMOB_BRONZE_PATH = "FOTMOB_BRONZE_PATH"
    
    # AIScore
    AISCORE_BRONZE_PATH = "AISCORE_BRONZE_PATH"
    AISCORE_HEADLESS = "AISCORE_HEADLESS"
    
    # ClickHouse
    CLICKHOUSE_HOST = "CLICKHOUSE_HOST"
    CLICKHOUSE_PORT = "CLICKHOUSE_PORT"
    CLICKHOUSE_USER = "CLICKHOUSE_USER"
    CLICKHOUSE_PASSWORD = "CLICKHOUSE_PASSWORD"
    CLICKHOUSE_DATABASE = "CLICKHOUSE_DATABASE"
    
    # Feature flags
    ENABLE_METRICS = "ENABLE_METRICS"
    ENABLE_HEALTH_CHECKS = "ENABLE_HEALTH_CHECKS"


# =============================================================================
# Regex Patterns
# =============================================================================

class Pattern:
    """Common regex patterns."""
    
    # Date patterns
    DATE_YYYYMMDD = r"^\d{8}$"  # 20241218
    DATE_YYYY_MM_DD = r"^\d{4}-\d{2}-\d{2}$"  # 2024-12-18
    
    # Match ID patterns
    FOTMOB_MATCH_ID = r"^\d+$"  # Numeric only
    AISCORE_MATCH_ID = r"^[a-z0-9]+$"  # Alphanumeric lowercase
    
    # URL patterns
    URL_HTTPS = r"^https?://[^\s<>\"]+|www\.[^\s<>\"]+$"


# =============================================================================
# Table Names (ClickHouse)
# =============================================================================

class TableName:
    """ClickHouse table names."""
    
    # FotMob tables
    FOTMOB_GENERAL = "general"
    FOTMOB_TIMELINE = "timeline"
    FOTMOB_VENUE = "venue"
    FOTMOB_PLAYER = "player"
    FOTMOB_SHOTMAP = "shotmap"
    FOTMOB_GOAL = "goal"
    FOTMOB_CARDS = "cards"
    FOTMOB_RED_CARD = "red_card"
    FOTMOB_PERIOD = "period"
    FOTMOB_MOMENTUM = "momentum"
    FOTMOB_STARTERS = "starters"
    FOTMOB_SUBSTITUTES = "substitutes"
    FOTMOB_COACHES = "coaches"
    FOTMOB_TEAM_FORM = "team_form"
    
    # AIScore tables
    AISCORE_MATCHES = "matches"
    AISCORE_ODDS_1X2 = "odds_1x2"
    AISCORE_ODDS_ASIAN_HANDICAP = "odds_asian_handicap"
    AISCORE_ODDS_OVER_UNDER = "odds_over_under"
    AISCORE_DAILY_LISTINGS = "daily_listings"


# =============================================================================
# Error Messages (Common)
# =============================================================================

class ErrorMessage:
    """Common error messages."""
    
    # Configuration
    MISSING_REQUIRED_CONFIG = "Required configuration '{key}' is missing"
    INVALID_CONFIG_VALUE = "Invalid value for '{key}': {value}"
    
    # Storage
    FILE_NOT_FOUND = "File not found: {path}"
    PERMISSION_DENIED = "Permission denied: {path}"
    DISK_SPACE_LOW = "Low disk space: {free_gb:.1f} GB free"
    
    # Scraper
    CONNECTION_FAILED = "Failed to connect to {url}"
    REQUEST_TIMEOUT = "Request timeout after {timeout}s"
    RATE_LIMIT_EXCEEDED = "Rate limit exceeded, retry after {retry_after}s"
    PARSE_ERROR = "Failed to parse response: {error}"
    
    # Database
    DB_CONNECTION_FAILED = "Failed to connect to database: {error}"
    DB_QUERY_FAILED = "Query failed: {error}"
    
    # Validation
    REQUIRED_FIELD_MISSING = "Required field '{field}' is missing"
    INVALID_FIELD_TYPE = "Field '{field}' has invalid type: expected {expected}, got {actual}"
    INVALID_FIELD_VALUE = "Field '{field}' has invalid value: {value}"


__all__ = [
    # Date formats
    'DATE_FORMAT_COMPACT',
    'DATE_FORMAT_DISPLAY',
    'DATE_FORMAT_ISO',
    'DATETIME_FORMAT_FILENAME',
    # Enums and classes
    'HttpStatus',
    'FileExtension',
    'MatchStatus',
    'Scraper',
    'StorageLayer',
    'DataSource',
    'Defaults',
    'EnvVar',
    'Pattern',
    'TableName',
    'ErrorMessage',
]
