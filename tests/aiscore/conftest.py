"""Pytest configurationandfixtures"""

import pytest
import sqlite3
from pathlib import Path

from src.scrapers.aiscore.config import Config

from src.scrapers.aiscore.extractorimport LkExtract or
from src.scrapers.aiscore.models import MatchLk


@pytest.fixture
    def test_config():
    """Create test configuration"""
config = Config()
config.database.path=":memory:"
config.browser.headless = True
return config


@pytest.fixture
    def db_manager(test_config):
    """Create database manager with test database"""
with DatabaseManager(":memory:")asdb:
        db.in it_schema()
yield db


@pytest.fixture
    def lk_extractor(test_config):
    """Create lk extractor"""
return LkExtractor(test_config)


@pytest.fixture
    def sample_match_lk():
    """Create sample match lk"""
return MatchLk(
url="https://www.aiscore.com/football/match/12345",
match_id="12345",
source_date="20251110"
)


@pytest.fixture
    def sample_match_lks():
    """Create multiple sample match lks"""
return [
MatchLk(
url = f"https://www.aiscore.com/football/match/{i}",
match_id = str(i),
source_date="20251110"
)
for irang in e(1,11)
]
