"""DEPRECATED: Configuration management - Moved to project root config/ directory.

This module is kept for backward compatibility only.

MIGRATION GUIDE:
    OLD: from src.config import FotMobConfig
    NEW: from config import FotMobConfig
    
    OLD: from src.config.fotmob_config import FotMobConfig
    NEW: from config import FotMobConfig
"""
import warnings
from typing import Optional

# Forward imports from new location
try:
    from config import FotMobConfig, AIScoreConfig
except ImportError:
    # Fallback to old location if new location doesn't work
    from .config.fotmob_config import FotMobConfig
    from .config.aiscore_config import AIScoreConfig

warnings.warn(
    "Importing from src.config is deprecated. Please import from config package instead: "
    "'from config import FotMobConfig, AIScoreConfig'",
    DeprecationWarning,
    stacklevel=2
)

__all__ = ['FotMobConfig', 'AIScoreConfig', 'load_config']


def load_config(config_path: Optional[str] = None) -> FotMobConfig:
    """Load FotMob configuration.
    
    DEPRECATED: Use FotMobConfig() directly instead.
    config_path parameter is ignored - configuration is read from .env file.

    Args:
        config_path: Ignored. Kept for backward compatibility.

    Returns:
        FotMobConfig instance
    """
    warnings.warn(
        "load_config() is deprecated. Use FotMobConfig() directly instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return FotMobConfig()
