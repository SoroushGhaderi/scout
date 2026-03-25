"""Processors package — Bronze, Silver, and Gold layer data transformers."""

from .bronze.match_processor import MatchProcessor
from .silver import FotMobSilverProcessor
from .gold import FotMobGoldProcessor

__all__ = ["MatchProcessor", "FotMobSilverProcessor", "FotMobGoldProcessor"]
