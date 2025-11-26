"""Tests for scraper modules."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from src.config import FotMobConfig
from src.scrapers import BaseScraper, DailyScraper, MatchScraper


class TestBaseScraper:
    """Tests for BaseScraper."""
    
    def test_initialization(self):
        """Test scraper initialization."""
        config = FotMobConfig()
        scraper = BaseScraper(config)
        
        assert scraper.config == config
        assert scraper.session is not None
        assert scraper.logger is not None
    
    @patch('requests.Session.get')
    def test_make_request_success(self, mock_get):
        """Test successful API request."""
        config = FotMobConfig()
        scraper = BaseScraper(config)
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_get.return_value = mock_response
        
        result = scraper.make_request("https://api.example.com/test")
        
        assert result == {"data": "test"}
        assert mock_get.called
    
    @patch('requests.Session.get')
    def test_make_request_404(self, mock_get):
        """Test request with 404 response."""
        config = FotMobConfig()
        scraper = BaseScraper(config)
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        result = scraper.make_request("https://api.example.com/test")
        
        assert result is None
    
    @patch('requests.Session.get')
    def test_make_request_timeout(self, mock_get):
        """Test request timeout handling."""
        config = FotMobConfig()
        scraper = BaseScraper(config)
        
        mock_get.side_effect = requests.exceptions.Timeout()
        
        result = scraper.make_request("https://api.example.com/test")
        
        assert result is None
    
    def test_context_manager(self):
        """Test context manager functionality."""
        config = FotMobConfig()
        
        with BaseScraper(config) as scraper:
            assert scraper.session is not None
        
        # Session should be closed after exiting context


class TestDailyScraper:
    """Tests for DailyScraper."""
    
    def test_extract_match_ids(self):
        """Test match ID extraction from API response."""
        config = FotMobConfig()
        scraper = DailyScraper(config)
        
        response_data = {
            "leagues": [
                {
                    "name": "Premier League",
                    "matches": [
                        {"id": 123, "name": "Team A vs Team B"},
                        {"id": 456, "name": "Team C vs Team D"}
                    ]
                },
                {
                    "name": "La Liga",
                    "matches": [
                        {"id": 789, "name": "Team E vs Team F"}
                    ]
                }
            ]
        }
        
        match_ids = scraper._extract_match_ids(response_data)
        
        assert len(match_ids) == 3
        assert 123 in match_ids
        assert 456 in match_ids
        assert 789 in match_ids
    
    def test_extract_match_ids_empty(self):
        """Test match ID extraction with empty response."""
        config = FotMobConfig()
        scraper = DailyScraper(config)
        
        response_data = {"leagues": []}
        match_ids = scraper._extract_match_ids(response_data)
        
        assert len(match_ids) == 0


class TestMatchScraper:
    """Tests for MatchScraper."""
    
    def test_validate_match_response_valid(self):
        """Test validation of valid match response."""
        config = FotMobConfig()
        scraper = MatchScraper(config)
        
        response_data = {
            "general": {
                "matchId": 12345,
                "homeTeam": {"name": "Team A"},
                "awayTeam": {"name": "Team B"}
            }
        }
        
        assert scraper._validate_match_response(response_data, "12345")
    
    def test_validate_match_response_missing_general(self):
        """Test validation with missing general section."""
        config = FotMobConfig()
        scraper = MatchScraper(config)
        
        response_data = {"header": {}}
        
        assert not scraper._validate_match_response(response_data, "12345")
    
    @patch.object(MatchScraper, 'make_request')
    def test_fetch_match_details_success(self, mock_request):
        """Test successful match details fetch."""
        config = FotMobConfig()
        scraper = MatchScraper(config)
        
        mock_request.return_value = {
            "general": {"matchId": 12345}
        }
        
        result = scraper.fetch_match_details("12345")
        
        assert result is not None
        assert result["general"]["matchId"] == 12345
    
    @patch.object(MatchScraper, 'make_request')
    def test_fetch_match_details_failure(self, mock_request):
        """Test failed match details fetch."""
        config = FotMobConfig()
        scraper = MatchScraper(config)
        
        mock_request.return_value = None
        
        result = scraper.fetch_match_details("12345")
        
        assert result is None

