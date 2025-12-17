"""DEPRECATED: AIScore Bronze Storage - Moved to src.storage.aiscore_storage

This module has been moved to properly separate storage concerns from scraper logic.

Please update your imports:
    OLD: from src.scrapers.aiscore.bronze_storage import BronzeStorage
    NEW: from src.storage.aiscore_storage import AIScoreBronzeStorage
    OR:  from src.storage.aiscore_storage import BronzeStorage  # Backward compat alias
"""

import warnings
from src.storage.aiscore_storage import AIScoreBronzeStorage, BronzeStorage

# Issue deprecation warning
warnings.warn(
    "Importing BronzeStorage from src.scrapers.aiscore.bronze_storage is deprecated. "
    "Please import from src.storage.aiscore_storage instead.",
    DeprecationWarning,
    stacklevel=2
)

__all__ = ['BronzeStorage', 'AIScoreBronzeStorage']
