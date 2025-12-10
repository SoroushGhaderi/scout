"""Scrapers package - contains both aiscore and fotmob scrapers."""
from .fotmob.match_scraper import MatchScraper
from .fotmob.daily_scraper import DailyScraper

__all__ = ['MatchScraper', 'DailyScraper']
