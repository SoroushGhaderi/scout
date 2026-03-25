"""Bronze layer storage package — raw API responses (JSON format)."""
from .base import BaseBronzeStorage
from .fotmob import BronzeStorage

__all__ = ["BaseBronzeStorage", "BronzeStorage"]
