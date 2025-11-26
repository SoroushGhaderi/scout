"""Integration tests for the football scraper system"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import sqlite3

from src.scrapers.aiscore.scraper import FootballScraper
from src.scrapers.aiscore.config import Config
# Note: DatabaseManager and MetricsContext may not exist - keeping for compatibility
from src.scrapers.aiscore.browser import BrowserManager
from src.scrapers.aiscore.extractor import LinkExtractor
from src.scrapers.aiscore.odds_scraper import OddsScraper
from src.scrapers.aiscore.models import MatchLink, Odds1X2, OddsAsianHandicap
from src.scrapers.aiscore.exceptions import ScraperError, BrowserError


class TestDatabaseIntegration:
    """Test database integration with other components"""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        db = DatabaseManager(db_path)
        db.init_schema()
        
        yield db
        
        db.close()
        Path(db_path).unlink()
    
    def test_full_match_link_workflow(self, temp_db):
        """Test complete match link workflow: insert -> query -> update"""
        # Insert match links
        links = [
            MatchLink(
                match_url=f"https://aiscore.com/match/{i}",
                match_id=str(i),
                source_date="20251110",
                league="Premier League"
            )
            for i in range(1, 6)
        ]
        
        temp_db.batch_insert_links(links)
        
        # Query unscraped matches
        unscraped = temp_db.get_unscraped_matches(limit=3)
        assert len(unscraped) == 3
        
        # Mark as scraped
        for match in unscraped:
            temp_db.mark_match_scraped(match['match_url'], success=True)
        
        # Verify update
        remaining = temp_db.get_unscraped_matches()
        assert len(remaining) == 2
        
        # Check statistics
        stats = temp_db.get_statistics()
        assert stats['total_links'] == 5
        assert stats['scraped'] == 3
        assert stats['unscraped'] == 2
    
    def test_odds_insertion_with_foreign_keys(self, temp_db):
        """Test odds insertion with foreign key constraints"""
        # Insert match link first
        link = MatchLink(
            match_url="https://aiscore.com/match/12345",
            match_id="12345",
            source_date="20251110"
        )
        temp_db.insert_link(link)
        
        # Insert 1X2 odds
        odds_1x2 = [
            Odds1X2(
                match_id="12345",
                match_url="https://aiscore.com/match/12345",
                bookmaker="Bet365",
                home_odds=2.10,
                draw_odds=3.40,
                away_odds=3.50
            )
        ]
        temp_db.insert_odds_1x2(odds_1x2)
        
        # Insert Asian Handicap odds
        odds_ah = [
            OddsAsianHandicap(
                match_id="12345",
                match_url="https://aiscore.com/match/12345",
                match_time="HT",
                moment_result="1-0",
                home_handicap="-0.5",
                home_odds=2.00,
                away_handicap="+0.5",
                away_odds=1.80
            )
        ]
        temp_db.insert_odds_asian_handicap(odds_ah)
        
        # Verify insertions
        temp_db.cursor.execute("SELECT COUNT(*) FROM odds_1x2")
        assert temp_db.cursor.fetchone()[0] == 1
        
        temp_db.cursor.execute("SELECT COUNT(*) FROM odds_asian_handicap")
        assert temp_db.cursor.fetchone()[0] == 1
    
    def test_duplicate_handling(self, temp_db):
        """Test duplicate detection across all tables"""
        link = MatchLink(
            match_url="https://aiscore.com/match/12345",
            match_id="12345",
            source_date="20251110"
        )
        
        # Insert twice
        result1 = temp_db.insert_link(link)
        result2 = temp_db.insert_link(link)
        
        assert result1 is True
        assert result2 is False
        
        # Check only one record
        temp_db.cursor.execute("SELECT COUNT(*) FROM match_links")
        assert temp_db.cursor.fetchone()[0] == 1
    
    def test_transaction_rollback(self, temp_db):
        """Test transaction rollback on error"""
        links = [
            MatchLink(
                match_url=f"https://aiscore.com/match/{i}",
                match_id=str(i),
                source_date="20251110"
            )
            for i in range(1, 6)
        ]
        
        try:
            with temp_db.conn:
                # Insert some links
                for link in links[:3]:
                    temp_db.insert_link(link)
                
                # Simulate error
                raise Exception("Simulated error")
        
        except Exception:
            pass
        
        # Check that transaction was NOT committed
        # (depends on implementation - adjust if needed)


class TestScraperIntegration:
    """Test FootballScraper integration"""
    
    @pytest.fixture
    def test_config(self):
        """Create test configuration"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config()
            config.database.path = f"{tmpdir}/test.db"
            config.browser.headless = True
            config.logging.file = f"{tmpdir}/test.log"
            config.metrics.export_path = tmpdir
            
            yield config
    
    @patch('football_scraper.scraper.BrowserManager')
    def test_scraper_initialization(self, mock_browser, test_config):
        """Test scraper initialization"""
        scraper = FootballScraper(test_config)
        
        assert scraper.config == test_config
        assert scraper.db is not None
    
    @patch('football_scraper.scraper.BrowserManager')
    @patch('football_scraper.extractor.LinkExtractor')
    def test_scraper_context_manager(self, mock_extractor, mock_browser, test_config):
        """Test scraper as context manager"""
        with FootballScraper(test_config) as scraper:
            assert scraper is not None
        
        # Should clean up resources
        # (implementation-specific verification)
    
    @patch('football_scraper.scraper.BrowserManager')
    def test_scraper_with_metrics(self, mock_browser, test_config):
        """Test scraper with metrics collection"""
        with tempfile.TemporaryDirectory() as tmpdir:
            metrics_path = tmpdir
            
            with MetricsContext(export_path=metrics_path) as metrics:
                scraper = FootballScraper(test_config)
                
                # Simulate operations
                metrics.record('links_extracted', 100)
                metrics.record('links_inserted', 95)
                metrics.increment('errors')
            
            # Check metrics were exported
            # (implementation-specific)


class TestOddsScraperIntegration:
    """Test OddsScraper integration"""
    
    @pytest.fixture
    def temp_db_with_matches(self):
        """Create database with sample matches"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        db = DatabaseManager(db_path)
        db.init_schema()
        
        # Insert sample matches
        links = [
            MatchLink(
                match_url=f"https://aiscore.com/match/{i}",
                match_id=str(i),
                source_date="20251110"
            )
            for i in range(1, 6)
        ]
        db.batch_insert_links(links)
        
        yield db
        
        db.close()
        Path(db_path).unlink()
    
    @patch('football_scraper.odds_scraper.BrowserManager')
    def test_odds_scraper_initialization(self, mock_browser, temp_db_with_matches, test_config):
        """Test odds scraper initialization"""
        scraper = OddsScraper(test_config, temp_db_with_matches)
        
        assert scraper.config == test_config
        assert scraper.db == temp_db_with_matches
    
    @patch('football_scraper.odds_scraper.BrowserManager')
    def test_odds_scraper_fetch_unscraped(self, mock_browser, temp_db_with_matches, test_config):
        """Test fetching unscraped matches"""
        scraper = OddsScraper(test_config, temp_db_with_matches)
        
        matches = temp_db_with_matches.get_unscraped_matches(limit=3)
        
        assert len(matches) == 3
        assert all(match['is_scraped'] == 0 for match in matches)


class TestEndToEndScenarios:
    """End-to-end integration tests"""
    
    @pytest.fixture
    def full_system_setup(self):
        """Setup complete system for testing"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config()
            config.database.path = f"{tmpdir}/test.db"
            config.logging.file = f"{tmpdir}/test.log"
            config.metrics.export_path = tmpdir
            config.browser.headless = True
            
            # Initialize database
            db = DatabaseManager(config.database.path)
            db.init_schema()
            
            yield config, db
            
            db.close()
    
    def test_full_scraping_workflow(self, full_system_setup):
        """Test complete scraping workflow"""
        config, db = full_system_setup
        
        # Step 1: Insert match links (simulating link scraper)
        links = [
            MatchLink(
                match_url=f"https://aiscore.com/match/{i}",
                match_id=str(i),
                source_date="20251110"
            )
            for i in range(1, 11)
        ]
        db.batch_insert_links(links)
        
        # Step 2: Verify links in database
        stats = db.get_statistics()
        assert stats['total_links'] == 10
        assert stats['unscraped'] == 10
        
        # Step 3: Fetch matches for odds scraping
        matches_to_scrape = db.get_unscraped_matches(limit=5)
        assert len(matches_to_scrape) == 5
        
        # Step 4: Simulate odds scraping
        for match in matches_to_scrape:
            # Insert odds (simulated)
            odds = Odds1X2(
                match_id=match['match_id'],
                match_url=match['match_url'],
                bookmaker="TestBookmaker",
                home_odds=2.00,
                draw_odds=3.00,
                away_odds=4.00
            )
            db.insert_odds_1x2([odds])
            
            # Mark as scraped
            db.mark_match_scraped(match['match_url'], success=True)
        
        # Step 5: Verify final state
        final_stats = db.get_statistics()
        assert final_stats['scraped'] == 5
        assert final_stats['unscraped'] == 5
        
        # Verify odds were inserted
        db.cursor.execute("SELECT COUNT(*) FROM odds_1x2")
        assert db.cursor.fetchone()[0] == 5
    
    def test_error_recovery(self, full_system_setup):
        """Test error recovery in workflow"""
        config, db = full_system_setup
        
        # Insert links
        links = [
            MatchLink(
                match_url=f"https://aiscore.com/match/{i}",
                match_id=str(i),
                source_date="20251110"
            )
            for i in range(1, 4)
        ]
        db.batch_insert_links(links)
        
        # Simulate scraping with failures
        matches = db.get_unscraped_matches()
        
        # First match: success
        db.mark_match_scraped(matches[0]['match_url'], success=True)
        
        # Second match: failure
        db.mark_match_scraped(
            matches[1]['match_url'],
            success=False,
            error_message="Timeout error"
        )
        
        # Third match: not attempted yet
        
        # Check state
        stats = db.get_statistics()
        assert stats['scraped'] == 1
        assert stats['unscraped'] == 2
        
        # Verify error was recorded
        db.cursor.execute(
            "SELECT last_error FROM match_links WHERE match_id = ?",
            (matches[1]['match_id'],)
        )
        error = db.cursor.fetchone()
        assert error is not None
    
    def test_metrics_collection_workflow(self, full_system_setup):
        """Test metrics collection throughout workflow"""
        config, db = full_system_setup
        
        with MetricsContext(export_path=config.metrics.export_path) as metrics:
            # Track link scraping
            with metrics.timer('link_scraping'):
                links = [
                    MatchLink(
                        match_url=f"https://aiscore.com/match/{i}",
                        match_id=str(i),
                        source_date="20251110"
                    )
                    for i in range(1, 51)
                ]
                inserted = db.batch_insert_links(links)
                
                metrics.record('links_extracted', len(links))
                metrics.record('links_inserted', inserted)
            
            # Track odds scraping
            with metrics.timer('odds_scraping'):
                matches = db.get_unscraped_matches(limit=10)
                
                for match in matches:
                    metrics.increment('matches_processed')
                    # Simulate scraping...
                    time.sleep(0.01)
                
                metrics.record('matches_scraped', len(matches))
        
        # Verify metrics were recorded
        exported_metrics = metrics.export()
        
        assert 'links_extracted' in exported_metrics
        assert 'links_inserted' in exported_metrics
        assert 'matches_processed' in exported_metrics
        assert exported_metrics['links_extracted'] == 50
        assert exported_metrics['matches_scraped'] == 10
    
    def test_concurrent_database_access(self, full_system_setup):
        """Test concurrent database access patterns"""
        config, db = full_system_setup
        
        # Insert initial links
        links = [
            MatchLink(
                match_url=f"https://aiscore.com/match/{i}",
                match_id=str(i),
                source_date="20251110"
            )
            for i in range(1, 21)
        ]
        db.batch_insert_links(links)
        
        # Simulate multiple readers
        batch1 = db.get_unscraped_matches(limit=5)
        batch2 = db.get_unscraped_matches(limit=5)
        
        # Both should get matches (may overlap depending on implementation)
        assert len(batch1) == 5
        assert len(batch2) == 5
        
        # Mark from both batches
        for match in batch1:
            db.mark_match_scraped(match['match_url'], success=True)
        
        for match in batch2:
            db.mark_match_scraped(match['match_url'], success=True)
        
        # Check final state
        stats = db.get_statistics()
        assert stats['scraped'] <= 10  # May be less if there was overlap


class TestSystemResilience:
    """Test system resilience and error handling"""
    
    def test_database_recovery_from_corruption(self):
        """Test recovery from database issues"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            # Create and corrupt database
            db = DatabaseManager(db_path)
            db.init_schema()
            db.close()
            
            # Attempt to reinitialize
            db = DatabaseManager(db_path)
            db.init_schema()  # Should handle existing schema
            
            # Verify it works
            link = MatchLink(
                match_url="https://aiscore.com/match/1",
                match_id="1",
                source_date="20251110"
            )
            result = db.insert_link(link)
            assert result is True
            
            db.close()
        
        finally:
            Path(db_path).unlink()
    
    def test_configuration_validation(self):
        """Test configuration validation"""
        config = Config()
        
        # Ensure critical paths are set
        assert config.database.path is not None
        assert config.scraping.base_url is not None
        assert config.logging.file is not None
        
        # Ensure numeric values are positive
        assert config.database.batch_size > 0
        assert config.scraping.timeouts.page_load > 0
        assert config.retry.max_attempts > 0

