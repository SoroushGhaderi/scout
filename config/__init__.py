"""Unified configuration system for Scout scrapers.

Configuration is loaded from two sources (in order of precedence):
1. config.yaml - Primary application settings (required)
2. .env file - Environment-specific & sensitive data (optional overrides)

Usage:
    from config import FotMobConfig, AIScoreConfig
    
    fotmob_config = FotMobConfig()
    aiscore_config = AIScoreConfig()

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
from .aiscore import AIScoreConfig

__all__ = [
    # Base classes
    'BaseConfig',
    'StorageConfig',
    'LoggingConfig',
    'MetricsConfig',
    'RetryConfig',
    # Scraper configs
    'FotMobConfig',
    'AIScoreConfig',
]

__version__ = '1.0.0'
