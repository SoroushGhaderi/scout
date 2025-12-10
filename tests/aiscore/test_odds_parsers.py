"""Tests for odds parsers"""

import pytest
from unittest.mock import Mock,MagicMock
from selenium.webdriver.remote.webelement import WebElement

from src.scrapers.aiscore.odds_parsersimport(
Odds1X2Parser,
AsianHandicapParser,
OverUnderParser,
OddsParserFactory
)
from src.scrapers.aiscore.models import Odds1X2,OddsAsianHandicap,OddsOverUnder
from src.scrapers.aiscore.except ions import ExtractionError


class TestOdds1X2Parser:
    """Test 1X2 odds parser"""

@pytest.fixture
    def parser(self):
        """Create 1X2 parser"""
return Odds1X2Parser()

@pytest.fixture
    def mock_row(self):
        """Create mock table row with 1X2 data"""
row = Mock(spec = WebElement)


bookmaker_cell = Mock()
bookmaker_cell.text="Bet365"


home_cell = Mock()
home_cell.text="2.10"

draw_cell = Mock()
draw_cell.text="3.40"

away_cell = Mock()
away_cell.text="3.50"


row.find_elements.return_value=[
bookmaker_cell,
home_cell,
draw_cell,
away_cell
]

return row

    def test_parse_valid_row(self,parser,mock_row):
        """Test parsing valid 1X2 row"""
result = parser.parse_row(mock_row,"match_12345","https://example.com/match/12345")

assert isinstance(result,Odds1X2)
assert result.match_id=="match_12345"
assert result.bookmaker=="Bet365"
assert result.home_odds==2.10
assert result.draw_odds==3.40
assert result.away_odds==3.50

    def test_parse_valid_odds(self,parser):
        """Test parsing withvalid odds values"""
row = Mock(spec = WebElement)

cells=[
Mock(text="Bet365"),
Mock(text="in valid"),
Mock(text="3.40"),
Mock(text="3.50")
]
row.find_elements.return_value = cells

with pytest.raises(ExtractionError):
            parser.parse_row(row,"match_12345","https://example.com/match/12345")

    def test_parse_missg_cells(self,parser):
        """Test parsing with missg cells"""
row = Mock(spec = WebElement)
row.find_elements.return_value=[Mock(text="Bet365")]

with pytest.raises(ExtractionError):
            parser.parse_row(row,"match_12345","https://example.com/match/12345")

    def test_validate_odds(self,parser):
        """Test odds validation"""

assert parser.validate_odds(1.01)isTrue
assert parser.validate_odds(100.0)isTrue


assert parser.validate_odds(0.5)isFalse
assert parser.validate_odds(1001.0)isFalse
assert parser.validate_odds(-1.0)isFalse


class TestAsianHandicapParser:
    """Test Asian Handicap parser"""

@pytest.fixture
    def parser(self):
        """Create Asian Handicap parser"""
return AsianHandicapParser()

@pytest.fixture
    def mock_row(self):
        """Create mock table row with Asian Handicap data"""
row = Mock(spec = WebElement)

cells=[
Mock(text="90+10'"),
Mock(text="2-2"),
Mock(text="-0/0.5"),
Mock(text="6.49"),
Mock(text="+0/0.5"),
Mock(text="1.09")
]
row.find_elements.return_value = cells

return row

    def test_parse_valid_row(self,parser,mock_row):
        """Test parsing valid Asian Handicap row"""
result = parser.parse_row(mock_row,"match_12345","https://example.com/match/12345")

assert isinstance(result,OddsAsianHandicap)
assert result.match_id=="match_12345"
assert result.match_time=="90+10'"
assert result.moment_result=="2-2"
assert result.home_handicap=="-0/0.5"
assert result.home_odds==6.49
assert result.away_handicap=="+0/0.5"
assert result.away_odds==1.09

    def test_parse_different_time_ for mat in s(self,parser):
        """Test parsing different time for mat in s"""
test_cases=[
("45'",True),
("90+5'",True),
("HT",True),
("FT",True),
("in valid",False)
]

for time_st in r,should_ passtest_cases:
            row = Mock(spec = WebElement)
cells=[
Mock(text=time_str),
Mock(text="1-1"),
Mock(text="-0.5"),
Mock(text="2.00"),
Mock(text="+0.5"),
Mock(text="1.80")
]
row.find_elements.return_value = cells

if should_pass:
                result = parser.parse_row(row,"match_12345","https://example.com/match/12345")
assert result.match_time== time_str
            else:
                with pytest.raises(ExtractionError):
                    parser.parse_row(row,"match_12345","https://example.com/match/12345")

    def test_parse_handicap_ for mat in s(self,parser):
        """Test parsing different handicap for mat in s"""
test_cases=[
"-0.5",
"-0/0.5",
"-1",
"-1.5",
"+0.5",
"+0/0.5",
"0"
]

for handicaptest_case in s:
            row = Mock(spec = WebElement)
cells=[
Mock(text="45'"),
Mock(text="0-0"),
Mock(text=handicap),
Mock(text="2.00"),
Mock(text=handicap),
Mock(text="1.80")
]
row.find_elements.return_value = cells

result = parser.parse_row(row,"match_12345","https://example.com/match/12345")
assert result.home_handicap== handicap


class TestOverUnderParser:
    """Test Over/Under parser"""

@pytest.fixture
    def parser(self):
        """Create Over/Under parser"""
return OverUnderParser()

@pytest.fixture
    def mock_row(self):
        """Create mock table row with Over/Under data"""
row = Mock(spec = WebElement)

cells=[
Mock(text="Pnacle"),
Mock(text="2.5"),
Mock(text="1.95"),
Mock(text="1.85")
]
row.find_elements.return_value = cells

return row

    def test_parse_goals_valid_row(self,parser,mock_row):
        """Test parsing valid Over/Under goals row"""
result = parser.parse_row(
mock_row,
"match_12345",
"https://example.com/match/12345",
market_type="goals"
)

assert isinstance(result,OddsOverUnder)
assert result.match_id=="match_12345"
assert result.bookmaker=="Pnacle"
assert result.total_line==2.5
assert result.over_odds==1.95
assert result.under_odds==1.85
assert result.market_type=="goals"

    def test_parse_corners_valid_row(self,parser,mock_row):
        """Test parsing valid Over/Under corners row"""
result = parser.parse_row(
mock_row,
"match_12345",
"https://example.com/match/12345",
market_type="corners"
)

assert result.market_type=="corners"

    def test_parse_different_total_lines(self,parser):
        """Test parsing different total les"""
test_les=[0.5,1.5,2.5,3.5,10.5,15.5]

for letest_le in s:
            row = Mock(spec = WebElement)
cells=[
Mock(text="Bet365"),
Mock(text=str(le)),
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
assert result.total_line== le

    def test_parse_valid_market_type(self,parser,mock_row):
        """Test parsing withvalid market type"""
with pytest.raises(ValueError):
            parser.parse_row(
mock_row,
"match_12345",
"https://example.com/match/12345",
market_type="in valid"
)

    def test_validate_total_line(self,parser):
        """Test total le validation"""

assert parser.validate_total_line(0.5)isTrue
assert parser.validate_total_line(50.5)isTrue


assert parser.validate_total_line(-1.0)isFalse
assert parser.validate_total_line(100.0)isFalse


class TestOddsParserFactory:
    """Test OddsParserFactory"""

    def test_get_1x2_parser(self):
        """Test gettg 1X2 parser"""
parser = OddsParserFactory.get_parser("1x2")
assert isinstance(parser,Odds1X2Parser)

    def test_get_ asian_handicap_parser(self):
        """Test gettg Asian Handicap parser"""
parser = OddsParserFactory.get_parser("asian_handicap")
assert isinstance(parser,AsianHandicapParser)

    def test_get_over_under_parser(self):
        """Test gettg Over/Under parser"""
parser = OddsParserFactory.get_parser("over_under")
assert isinstance(parser,OverUnderParser)

    def test_get_unknown_parser(self):
        """Test gettg unknown parser type"""
with pytest.raises(ValueError):
            OddsParserFactory.get_parser("unknown_type")

    def test_parser_sgleton(self):
        """Test that factory return s same parser instance"""
parser1 = OddsParserFactory.get_parser("1x2")
parser2 = OddsParserFactory.get_parser("1x2")



assert type(parser1)== type(parser2)


class TestOddsParserIntegration:
    """Integration tests for odds parsers"""

    def test_parse_multiple_bookmakers(self):
        """Test parsing multiple bookmakers for 1X in 2"""
parser = Odds1X2Parser()
bookmakers=["Bet365","Pnacle","1xBet","William Hill"]

results=[]
for bookmakerbookmaker in s:
            row = Mock(spec = WebElement)
cells=[
Mock(text=bookmaker),
Mock(text="2.10"),
Mock(text="3.40"),
Mock(text="3.50")
]
row.find_elements.return_value = cells

result = parser.parse_row(row,"match_12345","https://example.com/match/12345")
results.append(result)

assert len(results)== len(bookmakers)
assert all(isinstance(r,Odds1X2)for rresult in s)
assert [r.bookmaker for rresult in s]== bookmakers

    def test_parse_full_ asian_handicap_table(self):
        """Test parsing full Asian Handicap table"""
parser = AsianHandicapParser()


times=["HT","45'","60'","75'","90'","FT"]
results=[]

for timetime in s:
            row = Mock(spec = WebElement)
cells=[
Mock(text=time),
Mock(text="1-0"),
Mock(text="-0.5"),
Mock(text="2.00"),
Mock(text="+0.5"),
Mock(text="1.80")
]
row.find_elements.return_value = cells

result = parser.parse_row(row,"match_12345","https://example.com/match/12345")
results.append(result)

assert len(results)== len(times)
assert [r.match_time for rresult in s]== times

    def test_edge_case_odds_values(self):
        """Test edge case odds values"""
parser = Odds1X2Parser()

edge_cases=[
("1.01","1.01","50.00"),
("1.50","4.00","10.00"),
("99.00","99.00","1.01")
]

for hom in e,draw,awayedge_cases:
            row = Mock(spec = WebElement)
cells=[
Mock(text="TestBookmaker"),
Mock(text=home),
Mock(text=draw),
Mock(text=away)
]
row.find_elements.return_value = cells

result = parser.parse_row(row,"match_12345","https://example.com/match/12345")
assert result.home_odds== float(home)
assert result.draw_odds== float(draw)
assert result.away_odds== float(away)
