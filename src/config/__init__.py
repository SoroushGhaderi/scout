"""
Unified configuration system for scrapers.

Configuration is read ONLY from environment variables (.env file).

Usage:
    from src.config import FotMobConfig, AIScoreConfig
    
    config = FotMobConfig()
    config = AIScoreConfig()
"""

from .base import BaseConfig, StorageConfig, LoggingConfig, MetricsConfig, RetryConfig
from .fotmob_config import FotMobConfig
from .aiscore_config import AIScoreConfig

# Backward compatibility: export old FotMobConfig as default
__all__ = [
    'BaseConfig',
    'StorageConfig',
    'LoggingConfig',
    'MetricsConfig',
    'RetryConfig',
    'FotMobConfig',
    'AIScoreConfig',
]

