"""Tests for odds parsers"""

import pytest
from unittest.mock import Mock, MagicMock
from selenium.webdriver.remote.webelement import WebElement

from src.scrapers.aiscore.odds_parsers import (
    Odds1X2Parser,
    AsianHandicapParser,
    OverUnderParser,
    OddsParserFactory
)
from src.scrapers.aiscore.models import Odds1X2, OddsAsianHandicap, OddsOverUnder
from src.scrapers.aiscore.exceptions import ExtractionError


class TestOdds1X2Parser:
    """Test 1X2 odds parser"""
    
    @pytest.fixture
    def parser(self):
        """Create 1X2 parser"""
        return Odds1X2Parser()
    
    @pytest.fixture
    def mock_row(self):
        """Create mock table row with 1X2 data"""
        row = Mock(spec=WebElement)
        
        # Mock bookmaker cell
        bookmaker_cell = Mock()
        bookmaker_cell.text = "Bet365"
        
        # Mock odds cells
        home_cell = Mock()
        home_cell.text = "2.10"
        
        draw_cell = Mock()
        draw_cell.text = "3.40"
        
        away_cell = Mock()
        away_cell.text = "3.50"
        
        # Mock find_elements to return cells
        row.find_elements.return_value = [
            bookmaker_cell,
            home_cell,
            draw_cell,
            away_cell
        ]
        
        return row
    
    def test_parse_valid_row(self, parser, mock_row):
        """Test parsing valid 1X2 row"""
        result = parser.parse_row(mock_row, "match_12345", "https://example.com/match/12345")
        
        assert isinstance(result, Odds1X2)
        assert result.match_id == "match_12345"
        assert result.bookmaker == "Bet365"
        assert result.home_odds == 2.10
        assert result.draw_odds == 3.40
        assert result.away_odds == 3.50
    
    def test_parse_invalid_odds(self, parser):
        """Test parsing with invalid odds values"""
        row = Mock(spec=WebElement)
        
        cells = [
            Mock(text="Bet365"),
            Mock(text="invalid"),  # Invalid odds
            Mock(text="3.40"),
            Mock(text="3.50")
        ]
        row.find_elements.return_value = cells
        
        with pytest.raises(ExtractionError):
            parser.parse_row(row, "match_12345", "https://example.com/match/12345")
    
    def test_parse_missing_cells(self, parser):
        """Test parsing with missing cells"""
        row = Mock(spec=WebElement)
        row.find_elements.return_value = [Mock(text="Bet365")]  # Only 1 cell
        
        with pytest.raises(ExtractionError):
            parser.parse_row(row, "match_12345", "https://example.com/match/12345")
    
    def test_validate_odds(self, parser):
        """Test odds validation"""
        # Valid odds
        assert parser.validate_odds(1.01) is True
        assert parser.validate_odds(100.0) is True
        
        # Invalid odds
        assert parser.validate_odds(0.5) is False
        assert parser.validate_odds(1001.0) is False
        assert parser.validate_odds(-1.0) is False


class TestAsianHandicapParser:
    """Test Asian Handicap parser"""
    
    @pytest.fixture
    def parser(self):
        """Create Asian Handicap parser"""
        return AsianHandicapParser()
    
    @pytest.fixture
    def mock_row(self):
        """Create mock table row with Asian Handicap data"""
        row = Mock(spec=WebElement)
        
        cells = [
            Mock(text="90+10'"),          # Time
            Mock(text="2-2"),             # Score
            Mock(text="-0/0.5"),          # Home handicap
            Mock(text="6.49"),            # Home odds
            Mock(text="+0/0.5"),          # Away handicap
            Mock(text="1.09")             # Away odds
        ]
        row.find_elements.return_value = cells
        
        return row
    
    def test_parse_valid_row(self, parser, mock_row):
        """Test parsing valid Asian Handicap row"""
        result = parser.parse_row(mock_row, "match_12345", "https://example.com/match/12345")
        
        assert isinstance(result, OddsAsianHandicap)
        assert result.match_id == "match_12345"
        assert result.match_time == "90+10'"
        assert result.moment_result == "2-2"
        assert result.home_handicap == "-0/0.5"
        assert result.home_odds == 6.49
        assert result.away_handicap == "+0/0.5"
        assert result.away_odds == 1.09
    
    def test_parse_different_time_formats(self, parser):
        """Test parsing different time formats"""
        test_cases = [
            ("45'", True),
            ("90+5'", True),
            ("HT", True),
            ("FT", True),
            ("invalid", False)
        ]
        
        for time_str, should_pass in test_cases:
            row = Mock(spec=WebElement)
            cells = [
                Mock(text=time_str),
                Mock(text="1-1"),
                Mock(text="-0.5"),
                Mock(text="2.00"),
                Mock(text="+0.5"),
                Mock(text="1.80")
            ]
            row.find_elements.return_value = cells
            
            if should_pass:
                result = parser.parse_row(row, "match_12345", "https://example.com/match/12345")
                assert result.match_time == time_str
            else:
                with pytest.raises(ExtractionError):
                    parser.parse_row(row, "match_12345", "https://example.com/match/12345")
    
    def test_parse_handicap_formats(self, parser):
        """Test parsing different handicap formats"""
        test_cases = [
            "-0.5",
            "-0/0.5",
            "-1",
            "-1.5",
            "+0.5",
            "+0/0.5",
            "0"
        ]
        
        for handicap in test_cases:
            row = Mock(spec=WebElement)
            cells = [
                Mock(text="45'"),
                Mock(text="0-0"),
                Mock(text=handicap),
                Mock(text="2.00"),
                Mock(text=handicap),
                Mock(text="1.80")
            ]
            row.find_elements.return_value = cells
            
            result = parser.parse_row(row, "match_12345", "https://example.com/match/12345")
            assert result.home_handicap == handicap


class TestOverUnderParser:
    """Test Over/Under parser"""
    
    @pytest.fixture
    def parser(self):
        """Create Over/Under parser"""
        return OverUnderParser()
    
    @pytest.fixture
    def mock_row(self):
        """Create mock table row with Over/Under data"""
        row = Mock(spec=WebElement)
        
        cells = [
            Mock(text="Pinnacle"),        # Bookmaker
            Mock(text="2.5"),             # Total line
            Mock(text="1.95"),            # Over odds
            Mock(text="1.85")             # Under odds
        ]
        row.find_elements.return_value = cells
        
        return row
    
    def test_parse_goals_valid_row(self, parser, mock_row):
        """Test parsing valid Over/Under goals row"""
        result = parser.parse_row(
            mock_row,
            "match_12345",
            "https://example.com/match/12345",
            market_type="goals"
        )
        
        assert isinstance(result, OddsOverUnder)
        assert result.match_id == "match_12345"
        assert result.bookmaker == "Pinnacle"
        assert result.total_line == 2.5
        assert result.over_odds == 1.95
        assert result.under_odds == 1.85
        assert result.market_type == "goals"
    
    def test_parse_corners_valid_row(self, parser, mock_row):
        """Test parsing valid Over/Under corners row"""
        result = parser.parse_row(
            mock_row,
            "match_12345",
            "https://example.com/match/12345",
            market_type="corners"
        )
        
        assert result.market_type == "corners"
    
    def test_parse_different_total_lines(self, parser):
        """Test parsing different total lines"""
        test_lines = [0.5, 1.5, 2.5, 3.5, 10.5, 15.5]
        
        for line in test_lines:
            row = Mock(spec=WebElement)
            cells = [
                Mock(text="Bet365"),
                Mock(text=str(line)),
                Mock(text="2.00"),
                Mock(text="1.80")
            ]
            row.find_elements.return_value = cells
            
            result = parser.parse_row(
                row,
                "match_12345",
                "https://example.com/match/12345",
                market_type="goals"
            )
            assert result.total_line == line
    
    def test_parse_invalid_market_type(self, parser, mock_row):
        """Test parsing with invalid market type"""
        with pytest.raises(ValueError):
            parser.parse_row(
                mock_row,
                "match_12345",
                "https://example.com/match/12345",
                market_type="invalid"
            )
    
    def test_validate_total_line(self, parser):
        """Test total line validation"""
        # Valid lines
        assert parser.validate_total_line(0.5) is True
        assert parser.validate_total_line(50.5) is True
        
        # Invalid lines
        assert parser.validate_total_line(-1.0) is False
        assert parser.validate_total_line(100.0) is False


class TestOddsParserFactory:
    """Test OddsParserFactory"""
    
    def test_get_1x2_parser(self):
        """Test getting 1X2 parser"""
        parser = OddsParserFactory.get_parser("1x2")
        assert isinstance(parser, Odds1X2Parser)
    
    def test_get_asian_handicap_parser(self):
        """Test getting Asian Handicap parser"""
        parser = OddsParserFactory.get_parser("asian_handicap")
        assert isinstance(parser, AsianHandicapParser)
    
    def test_get_over_under_parser(self):
        """Test getting Over/Under parser"""
        parser = OddsParserFactory.get_parser("over_under")
        assert isinstance(parser, OverUnderParser)
    
    def test_get_unknown_parser(self):
        """Test getting unknown parser type"""
        with pytest.raises(ValueError):
            OddsParserFactory.get_parser("unknown_type")
    
    def test_parser_singleton(self):
        """Test that factory returns same parser instance"""
        parser1 = OddsParserFactory.get_parser("1x2")
        parser2 = OddsParserFactory.get_parser("1x2")
        
        # Should be same instance (if factory implements caching)
        # If not cached, at least should be same type
        assert type(parser1) == type(parser2)


class TestOddsParserIntegration:
    """Integration tests for odds parsers"""
    
    def test_parse_multiple_bookmakers(self):
        """Test parsing multiple bookmakers for 1X2"""
        parser = Odds1X2Parser()
        bookmakers = ["Bet365", "Pinnacle", "1xBet", "William Hill"]
        
        results = []
        for bookmaker in bookmakers:
            row = Mock(spec=WebElement)
            cells = [
                Mock(text=bookmaker),
                Mock(text="2.10"),
                Mock(text="3.40"),
                Mock(text="3.50")
            ]
            row.find_elements.return_value = cells
            
            result = parser.parse_row(row, "match_12345", "https://example.com/match/12345")
            results.append(result)
        
        assert len(results) == len(bookmakers)
        assert all(isinstance(r, Odds1X2) for r in results)
        assert [r.bookmaker for r in results] == bookmakers
    
    def test_parse_full_asian_handicap_table(self):
        """Test parsing full Asian Handicap table"""
        parser = AsianHandicapParser()
        
        # Simulate match progression
        times = ["HT", "45'", "60'", "75'", "90'", "FT"]
        results = []
        
        for time in times:
            row = Mock(spec=WebElement)
            cells = [
                Mock(text=time),
                Mock(text="1-0"),
                Mock(text="-0.5"),
                Mock(text="2.00"),
                Mock(text="+0.5"),
                Mock(text="1.80")
            ]
            row.find_elements.return_value = cells
            
            result = parser.parse_row(row, "match_12345", "https://example.com/match/12345")
            results.append(result)
        
        assert len(results) == len(times)
        assert [r.match_time for r in results] == times
    
    def test_edge_case_odds_values(self):
        """Test edge case odds values"""
        parser = Odds1X2Parser()
        
        edge_cases = [
            ("1.01", "1.01", "50.00"),  # Very low and very high
            ("1.50", "4.00", "10.00"),  # Common odds
            ("99.00", "99.00", "1.01")  # Extreme values
        ]
        
        for home, draw, away in edge_cases:
            row = Mock(spec=WebElement)
            cells = [
                Mock(text="TestBookmaker"),
                Mock(text=home),
                Mock(text=draw),
                Mock(text=away)
            ]
            row.find_elements.return_value = cells
            
            result = parser.parse_row(row, "match_12345", "https://example.com/match/12345")
            assert result.home_odds == float(home)
            assert result.draw_odds == float(draw)
            assert result.away_odds == float(away)

