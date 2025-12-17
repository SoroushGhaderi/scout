"""Custom exception hierarchy for Scout project.

This module defines a comprehensive exception hierarchy for the entire application.
All exceptions inherit from ScoutError for easy catching and handling.

Exception Hierarchy:
    ScoutError (base)
    ├── ConfigurationError
    ├── StorageError
    │   ├── StorageReadError
    │   ├── StorageWriteError
    │   └── StorageNotFoundError
    ├── ScraperError
    │   ├── ScraperConnectionError
    │   ├── ScraperTimeoutError
    │   ├── ScraperRateLimitError
    │   └── ScraperParseError
    ├── ProcessorError
    │   └── ValidationError
    ├── DatabaseError
    │   ├── DatabaseConnectionError
    │   └── DatabaseQueryError
    └── OrchestratorError

Usage:
    from src.core.exceptions import StorageError, ScraperError
    
    try:
        storage.save_match(...)
    except StorageError as e:
        logger.error(f"Storage failed: {e}")
"""


class ScoutError(Exception):
    """Base exception for all Scout errors.
    
    All custom exceptions in the Scout project should inherit from this.
    This allows catching all Scout-specific errors with a single except block.
    
    Attributes:
        message: Error message
        details: Optional dictionary with additional error details
    """
    
    def __init__(self, message: str, details: dict = None):
        """Initialize Scout error.
        
        Args:
            message: Error message
            details: Optional dictionary with additional context
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self) -> str:
        """String representation of error."""
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({details_str})"
        return self.message
    
    def to_dict(self) -> dict:
        """Convert error to dictionary.
        
        Returns:
            Dictionary with error information
        """
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'details': self.details,
        }


# =============================================================================
# Configuration Errors
# =============================================================================

class ConfigurationError(ScoutError):
    """Configuration-related errors.
    
    Raised when:
    - Configuration file is invalid
    - Required configuration values are missing
    - Configuration validation fails
    """
    pass


# =============================================================================
# Storage Errors
# =============================================================================

class StorageError(ScoutError):
    """Base exception for storage-related errors.
    
    All storage errors inherit from this class.
    """
    pass


class StorageReadError(StorageError):
    """Error reading from storage.
    
    Raised when:
    - File cannot be read
    - Data is corrupted
    - Permission denied
    """
    pass


class StorageWriteError(StorageError):
    """Error writing to storage.
    
    Raised when:
    - File cannot be written
    - Disk space full
    - Permission denied
    """
    pass


class StorageNotFoundError(StorageError):
    """Requested data not found in storage.
    
    Raised when:
    - Match file doesn't exist
    - Archive doesn't contain requested match
    """
    pass


# =============================================================================
# Scraper Errors
# =============================================================================

class ScraperError(ScoutError):
    """Base exception for scraper-related errors.
    
    All scraper errors inherit from this class.
    """
    pass


class ScraperConnectionError(ScraperError):
    """Error connecting to data source.
    
    Raised when:
    - API is unreachable
    - Network timeout
    - DNS resolution fails
    """
    pass


class ScraperTimeoutError(ScraperError):
    """Request timeout error.
    
    Raised when:
    - Request takes too long
    - Page load timeout (Selenium)
    - Element wait timeout
    """
    pass


class ScraperRateLimitError(ScraperError):
    """Rate limit exceeded.
    
    Raised when:
    - API returns 429 status
    - Too many requests in time window
    - IP temporarily blocked
    """
    pass


class ScraperParseError(ScraperError):
    """Error parsing scraped data.
    
    Raised when:
    - HTML structure changed
    - JSON response malformed
    - Expected data missing
    """
    pass


class ScraperAuthenticationError(ScraperError):
    """Authentication failed.
    
    Raised when:
    - API key invalid
    - Token expired
    - Credentials rejected
    """
    pass


class ScraperCloudflareChallengeError(ScraperError):
    """Cloudflare challenge detected.
    
    Raised when:
    - Cloudflare CAPTCHA appears
    - Browser fingerprint detected
    - JavaScript challenge fails
    """
    pass


# =============================================================================
# Processor Errors
# =============================================================================

class ProcessorError(ScoutError):
    """Base exception for data processing errors.
    
    All processor errors inherit from this class.
    """
    pass


class ValidationError(ProcessorError):
    """Data validation error.
    
    Raised when:
    - Required fields missing
    - Data type mismatch
    - Value out of valid range
    - Business logic validation fails
    """
    
    def __init__(self, message: str, field: str = None, value: any = None, details: dict = None):
        """Initialize validation error.
        
        Args:
            message: Error message
            field: Name of field that failed validation (optional)
            value: Value that failed validation (optional)
            details: Additional error details (optional)
        """
        super().__init__(message, details)
        self.field = field
        self.value = value
    
    def to_dict(self) -> dict:
        """Convert error to dictionary."""
        data = super().to_dict()
        if self.field:
            data['field'] = self.field
        if self.value is not None:
            data['value'] = str(self.value)
        return data


# =============================================================================
# Database Errors
# =============================================================================

class DatabaseError(ScoutError):
    """Base exception for database-related errors.
    
    All database errors inherit from this class.
    """
    pass


class DatabaseConnectionError(DatabaseError):
    """Database connection error.
    
    Raised when:
    - Cannot connect to database
    - Connection lost
    - Authentication failed
    """
    pass


class DatabaseQueryError(DatabaseError):
    """Database query error.
    
    Raised when:
    - SQL syntax error
    - Query timeout
    - Constraint violation
    - Data type mismatch
    """
    
    def __init__(self, message: str, query: str = None, details: dict = None):
        """Initialize database query error.
        
        Args:
            message: Error message
            query: SQL query that failed (optional)
            details: Additional error details (optional)
        """
        super().__init__(message, details)
        self.query = query
    
    def to_dict(self) -> dict:
        """Convert error to dictionary."""
        data = super().to_dict()
        if self.query:
            data['query'] = self.query
        return data


# =============================================================================
# Orchestrator Errors
# =============================================================================

class OrchestratorError(ScoutError):
    """Orchestration workflow error.
    
    Raised when:
    - Pipeline step fails
    - Dependency missing
    - Workflow interrupted
    """
    pass


# =============================================================================
# Utility Functions
# =============================================================================

def format_error(error: Exception) -> dict:
    """Format any exception into a structured dictionary.
    
    Args:
        error: Exception to format
        
    Returns:
        Dictionary with error information
    """
    if isinstance(error, ScoutError):
        return error.to_dict()
    
    return {
        'error_type': error.__class__.__name__,
        'message': str(error),
        'details': {},
    }


__all__ = [
    # Base
    'ScoutError',
    # Configuration
    'ConfigurationError',
    # Storage
    'StorageError',
    'StorageReadError',
    'StorageWriteError',
    'StorageNotFoundError',
    # Scraper
    'ScraperError',
    'ScraperConnectionError',
    'ScraperTimeoutError',
    'ScraperRateLimitError',
    'ScraperParseError',
    'ScraperAuthenticationError',
    'ScraperCloudflareChallengeError',
    # Processor
    'ProcessorError',
    'ValidationError',
    # Database
    'DatabaseError',
    'DatabaseConnectionError',
    'DatabaseQueryError',
    # Orchestrator
    'OrchestratorError',
    # Utilities
    'format_error',
]
