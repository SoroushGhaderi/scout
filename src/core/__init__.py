"""Core functionality for Scout project.

This package contains:
- interfaces.py: Protocol definitions for all major components
- exceptions.py: Custom exception hierarchy  
- constants.py: Project-wide constants
- types.py: Common type definitions and aliases

These provide the foundation for the entire application architecture.

Usage:
    # Import protocols
    from src.core import StorageProtocol, ScraperProtocol
    
    # Import exceptions
    from src.core import ScoutError, StorageError, ScraperError
    
    # Import constants
    from src.core.constants import MatchStatus, Defaults, HttpStatus
    
    # Import types
    from src.core.types import MatchID, DateStr, JSONDict
"""

from .interfaces import (
    StorageProtocol,
    ScraperProtocol,
    ProcessorProtocol,
    OrchestratorProtocol,
    ConfigProtocol,
    CacheProtocol,
    MetricsProtocol,
    LoggerProtocol,
)
from .exceptions import (
    ScoutError,
    ConfigurationError,
    StorageError,
    StorageReadError,
    StorageWriteError,
    StorageNotFoundError,
    ScraperError,
    ScraperConnectionError,
    ScraperTimeoutError,
    ScraperRateLimitError,
    ScraperParseError,
    ProcessorError,
    ValidationError,
    DatabaseError,
    DatabaseConnectionError,
    DatabaseQueryError,
    OrchestratorError,
    format_error,
)

__all__ = [
    # Protocols/Interfaces
    'StorageProtocol',
    'ScraperProtocol',
    'ProcessorProtocol',
    'OrchestratorProtocol',
    'ConfigProtocol',
    'CacheProtocol',
    'MetricsProtocol',
    'LoggerProtocol',
    # Base exceptions
    'ScoutError',
    'ConfigurationError',
    # Storage exceptions
    'StorageError',
    'StorageReadError',
    'StorageWriteError',
    'StorageNotFoundError',
    # Scraper exceptions
    'ScraperError',
    'ScraperConnectionError',
    'ScraperTimeoutError',
    'ScraperRateLimitError',
    'ScraperParseError',
    # Processor exceptions
    'ProcessorError',
    'ValidationError',
    # Database exceptions
    'DatabaseError',
    'DatabaseConnectionError',
    'DatabaseQueryError',
    # Orchestrator exceptions
    'OrchestratorError',
    # Utilities
    'format_error',
]

__version__ = '1.0.0'
