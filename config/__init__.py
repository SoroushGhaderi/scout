"""Unified configuration system for Scout scrapers.

Configuration is read from environment variables (.env file).

Usage:
    from config import FotMobConfig, AIScoreConfig
    
    fotmob_config = FotMobConfig()
    aiscore_config = AIScoreConfig()

All configuration classes provide sensible defaults and can be overridden
via environment variables. See .env.example for available options.
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
