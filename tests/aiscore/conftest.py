"""Pytest configuration and fixtures"""

import pytest
import sqlite3
from pathlib import Path

from src.scrapers.aiscore.config import Config
# Note: DatabaseManager may not exist in aiscore module - keeping for compatibility
from src.scrapers.aiscore.extractor import LinkExtractor
from src.scrapers.aiscore.models import MatchLink


@pytest.fixture
def test_config():
    """Create test configuration"""
    config = Config()
    config.database.path = ":memory:"  # Use in-memory database for tests
    config.browser.headless = True
    return config


@pytest.fixture
def db_manager(test_config):
    """Create database manager with test database"""
    with DatabaseManager(":memory:") as db:
        db.init_schema()
        yield db


@pytest.fixture
def link_extractor(test_config):
    """Create link extractor"""
    return LinkExtractor(test_config)


@pytest.fixture
def sample_match_link():
    """Create sample match link"""
    return MatchLink(
        url="https://www.aiscore.com/football/match/12345",
        match_id="12345",
        source_date="20251110"
    )


@pytest.fixture
def sample_match_links():
    """Create multiple sample match links"""
    return [
        MatchLink(
            url=f"https://www.aiscore.com/football/match/{i}",
            match_id=str(i),
            source_date="20251110"
        )
        for i in range(1, 11)
    ]

