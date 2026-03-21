"""FotMob configuration system for Scout.

Configuration is loaded from two sources (in order of precedence):
1. config.yaml - Primary application settings (required)
2. .env file - Environment-specific & sensitive data (optional overrides)

Usage:
    from config import FotMobConfig
    
    fotmob_config = FotMobConfig()

All configuration classes load defaults from config.yaml and can be overridden
via environment variables in .env. See config.yaml for all available options.
"""

from .base import (
    BaseConfig,
    StorageConfig,
    LoggingConfig,
    MetricsConfig,
    RetryConfig,
)
from .fotmob import FotMobConfig

__all__ = [
    # Base classes
    'BaseConfig',
    'StorageConfig',
    'LoggingConfig',
    'MetricsConfig',
    'RetryConfig',
    # Scraper configs
    'FotMobConfig',
]

__version__ = '1.0.0'
