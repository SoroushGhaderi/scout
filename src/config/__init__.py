"""DEPRECATED: Configuration - Moved to project root config/ directory.

This module is kept for backward compatibility only.

MIGRATION GUIDE:
    OLD: from src.config import FotMobConfig, AIScoreConfig
    NEW: from config import FotMobConfig, AIScoreConfig
"""
import warnings

# Forward imports from new location
try:
    from config import (
        BaseConfig,
        StorageConfig,
        LoggingConfig,
        MetricsConfig,
        RetryConfig,
        FotMobConfig,
        AIScoreConfig,
    )
except ImportError:
    # Fallback to old location if new location doesn't work
    from .base import BaseConfig, StorageConfig, LoggingConfig, MetricsConfig, RetryConfig
    from .fotmob_config import FotMobConfig
    from .aiscore_config import AIScoreConfig

warnings.warn(
    "Importing from src.config is deprecated. Configuration has been moved to project root. "
    "Please update imports: 'from config import FotMobConfig, AIScoreConfig'",
    DeprecationWarning,
    stacklevel=2
)

__all__ = [
    'BaseConfig',
    'StorageConfig',
    'LoggingConfig',
    'MetricsConfig',
    'RetryConfig',
    'FotMobConfig',
    'AIScoreConfig',
]
