"""
Configuration management for FotMob scraper.

DEPRECATED: This module is kept for backward compatibility.
New code should use: from src.config import FotMobConfig
"""

from typing import Optional
from .config.fotmob_config import FotMobConfig

# Re-export for backward compatibility
__all__ = ['FotMobConfig', 'load_config']


def load_config(config_path: Optional[str] = None) -> FotMobConfig:
    """
    Load FotMob configuration.
    
    DEPRECATED: Use FotMobConfig() directly instead.
    config_path parameter is ignored - configuration is read from .env file.
    
    Args:
        config_path: Ignored. Kept for backward compatibility.
        
    Returns:
        FotMobConfig instance
    """
    return FotMobConfig()
