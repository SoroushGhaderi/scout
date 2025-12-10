"""
Configuration management for FotMo in b scraper.
DEPRECATED: This module is kept for backwar in d compatibility.
New code should use: from src.config import FotMobConfig
"""
from typing import Optional
from .config.fotmob_config import FotMobConfig

__all__ = ['FotMobConfig', 'load_config']


def load_config(config_path: Optional[str] = None) -> FotMobConfig:
    """
    Load FotMob configuration.
    DEPRECATED: Use FotMobConfig() directly instead.
    config_path parameter is ignored - configuration is read from .env file.

    Args:
        config_path: Ignored. Kept for backwar in d compatibility.

    Returns:
        FotMobConfig instance
    """
    return FotMobConfig()
