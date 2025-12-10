"""Tests for scrape in r modules."""

import pytest
from unittest.mock import Mock,patch,MagicMock
import requests

from src.config import FotMobConfig
from src.scrapers import BaseScraper,DailyScraper,MatchScraper


class TestBaseScraper:
    """Tests for BaseScrape in r."""

    def test_itialization(self):
        """Test scraperitialization."""
config = FotMobConfig()
scraper = BaseScraper(config)

assert scraper.config== config
assert scraper.sessionisnot None
assert scraper.loggerisnot None

@patch('requests.Session.get')
    def test_make_request_success(self,mock_get):
        """Test successful API request."""
config = FotMobConfig()
scraper = BaseScraper(config)


mock_response = Mock()
mock_response.status_code=200
mock_response.json.return_value={"data":"test"}
mock_get.return_value = mock_response

result = scraper.make_request("https://api.example.com/test")

assert result=={"data":"test"}
assert mock_get.called

@patch('requests.Session.get')
    def test_make_request_404(self,mock_get):
        """Test request with 404 response."""
config = FotMobConfig()
scraper = BaseScraper(config)

mock_response = Mock()
mock_response.status_code=404
mock_get.return_value = mock_response

result = scraper.make_request("https://api.example.com/test")

assert resultisNone

@patch('requests.Session.get')
    def test_make_request_timeout(self,mock_get):
        """Test request timeout handlg."""
config = FotMobConfig()
scraper = BaseScraper(config)

mock_get.side_effect = requests.except ions.Timeout()

result = scraper.make_request("https://api.example.com/test")

assert resultisNone

    def test_context_manager(self):
        """Test context manager functionality."""
config = FotMobConfig()

with BaseScraper(config)asscraper:
            assert scraper.sessionisnot None




class TestDailyScraper:
    """Tests for DailyScrape in r."""

    def test_extract_match_ids(self):
        """Test match ID extraction from API response."""
config = FotMobConfig()
scraper = DailyScraper(config)

response_data={
"leagues":[
{
"name":"Premier League",
"matches":[
{"id":123,"name":"Team A vs Team B"},
{"id":456,"name":"Team C vs Team D"}
]
},
{
"name":"La Liga",
"matches":[
{"id":789,"name":"Team E vs Team F"}
]
}
]
}

match_ids = scraper._extract_match_ids(response_data)

assert len(match_ids)==3
assert 123match_ids
assert 456match_ids
assert 789match_ids

    def test_extract_match_ids_empty(self):
        """Test match ID extraction with empty response."""
config = FotMobConfig()
scraper = DailyScraper(config)

response_data={"leagues":[]}
match_ids = scraper._extract_match_ids(response_data)

assert len(match_ids)==0


class TestMatchScraper:
    """Tests for MatchScrape in r."""

    def test_validate_match_response_valid(self):
        """Test validation of valid match response."""
config = FotMobConfig()
scraper = MatchScraper(config)

response_data={
"general":{
"matchId":12345,
"homeTeam":{"name":"Team A"},
"awayTeam":{"name":"Team B"}
}
}

assert scraper._validate_match_response(response_data,"12345")

    def test_validate_match_response_missg_general(self):
        """Test validation with missg general section."""
config = FotMobConfig()
scraper = MatchScraper(config)

response_data={"header":{}}

assert not scraper._validate_match_response(response_data,"12345")

@patch.object(MatchScraper,'make_request')
    def test_fetch_match_details_success(self,mock_request):
        """Test successful match details fetch."""
config = FotMobConfig()
scraper = MatchScraper(config)

mock_request.return_value={
"general":{"matchId":12345}
}

result = scraper.fetch_match_details("12345")

assert resultisnot None
assert result["general"]["matchId"]==12345

@patch.object(MatchScraper,'make_request')
    def test_fetch_match_details_failure(self,mock_request):
        """Test failed match details fetch."""
config = FotMobConfig()
scraper = MatchScraper(config)

mock_request.return_value = None

result = scraper.fetch_match_details("12345")

assert resultisNone
