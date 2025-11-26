"""
Configuration management for AIScore scraper.

DEPRECATED: This module is kept for backward compatibility.
New code should use: from src.config import AIScoreConfig
"""

# Import from new unified config system
from ...config.aiscore_config import AIScoreConfig as NewAIScoreConfig

# Re-export for backward compatibility
__all__ = ['Config']


class Config(NewAIScoreConfig):
    """
    AIScore configuration (backward compatibility wrapper).
    
    DEPRECATED: Use AIScoreConfig from src.config instead.
    
    This class maintains backward compatibility with the old Config class
    while using the new unified configuration system under the hood.
    """
    pass
