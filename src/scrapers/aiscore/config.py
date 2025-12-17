"""DEPRECATED: AIScore Configuration - Moved to project root config/ directory.

This module is kept for backward compatibility only.

MIGRATION GUIDE:
    OLD: from src.scrapers.aiscore.config import Config
    NEW: from config import AIScoreConfig
"""
import warnings

# Forward imports from new location
try:
    from config import AIScoreConfig as NewAIScoreConfig
except ImportError:
    # Fallback to old location if new location doesn't work
    from ...config.aiscore_config import AIScoreConfig as NewAIScoreConfig

warnings.warn(
    "Importing from src.scrapers.aiscore.config is deprecated. "
    "Configuration has been moved to project root. "
    "Please update imports: 'from config import AIScoreConfig'",
    DeprecationWarning,
    stacklevel=2
)

__all__ = ['Config']


class Config(NewAIScoreConfig):
    """AIScore configuration (backward compatibility wrapper).

    DEPRECATED: Use AIScoreConfig from config package instead.
    This class maintains backward compatibility with the old Config class
    while using the new unified configuration system under the hood.
    """
    pass
