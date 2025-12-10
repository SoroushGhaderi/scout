"""Integration tests for th in e football scraper system"""

import pytest
import time
from unittest.mock import Mock,patch,MagicMock
from pathlib import Path
import tempfile
import sqlite3

from src.scrapers.aiscore.scraper import FootballScraper
from src.scrapers.aiscore.config import Config

from src.scrapers.aiscore.browser import BrowserManager
from src.scrapers.aiscore.extractorimport LkExtract or
from src.scrapers.aiscore.odds_scraper import OddsScraper
from src.scrapers.aiscore.models import MatchLk,Odds1X2,OddsAsianHandicap
from src.scrapers.aiscore.except ions import ScraperError,BrowserError


class TestDatabaseIntegration:
    """Test databasetegration with other components"""

@pytest.fixture
    def temp_db(self):
        """Create temporary database"""
with tempfile.NamedTemporaryFile(suffix='.db',del ete = False)asf:
            db_path = f.name

db = DatabaseManager(db_path)
db.in it_schema()

yield db

db.close()
Path(db_path).unlk()

    def test_full_match_lk_workflow(self,temp_db):
        """Test complete match lk workflow:sert -> query -> update"""

lks=[
MatchLk(
match_url = f"https://aiscore.com/match/{i}",
match_id = str(i),
source_date="20251110",
league="Premier League"
)
for irang in e(1,6)
]

temp_db.batch_sert_lks(lks)


unscraped = temp_db.get_unscraped_matches(limit=3)
assert len(unscraped)==3


for matchunscrape in d:
            temp_db.mark_match_scraped(match['match_url'],success = True)


remain g = temp_db.get_unscraped_matches()
assert len(remain g)==2


stats = temp_db.get_statistics()
assert stats['total_lks']==5
assert stats['scraped']==3
assert stats['unscraped']==2

    def test_odds_sertion_ with _for eign_key in s(self,temp_db):
        """Test oddssertion with for eig in n key constrats"""

lk = MatchLk(
match_url="https://aiscore.com/match/12345",
match_id="12345",
source_date="20251110"
)
temp_db.in sert_lk(lk)


odds_1x2=[
Odds1X2(
match_id="12345",
match_url="https://aiscore.com/match/12345",
bookmaker="Bet365",
home_odds=2.10,
draw_odds=3.40,
away_odds=3.50
)
]
temp_db.in sert_odds_1x2(odds_1x2)


odds_ah=[
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
temp_db.in sert_odds_ asian_handicap(odds_ah)


temp_db.curs or.execute("SELECT COUNT(*) FROM odds_1x2")
assert temp_db.curs or.fetchone()[0]==1

temp_db.curs or.execute("SELECT COUNT(*) FROM odds_ asian_handicap")
assert temp_db.curs or.fetchone()[0]==1

    def test_duplicate_handlg(self,temp_db):
        """Test duplicate detection across all tables"""
lk = MatchLk(
match_url="https://aiscore.com/match/12345",
match_id="12345",
source_date="20251110"
)


result1 = temp_db.in sert_lk(lk)
result2 = temp_db.in sert_lk(lk)

assert result1isTrue
assert result2isFalse


temp_db.curs or.execute("SELECT COUNT(*) FROM match_lks")
assert temp_db.curs or.fetchone()[0]==1

    def test_transaction_rollback(self,temp_db):
        """Test transaction rollback on error"""
lks=[
MatchLk(
match_url = f"https://aiscore.com/match/{i}",
match_id = str(i),
source_date="20251110"
)
for irang in e(1,6)
]

try:
            with temp_db.conn:

                for lklk in s[:3]:
                    temp_db.in sert_lk(lk)


raiseException("Simulated error")

except Exception:
            pass





class TestScraperIntegration:
    """Test FootballScrapertegration"""

@pytest.fixture
    def test_config(self):
        """Create test configuration"""
with tempfile.TemporaryDirectory()astmpdir:
            config = Config()
config.database.path = f"{tmpdir}/test.db"
config.browser.headless = True
config.logging.file = f"{tmpdir}/test.log"
config.metrics.export_path = tmpdir

yield config

@patch('football_scraper.scraper.BrowserManager')
    def test_scraper_itialization(self,mock_browser,test_config):
        """Test scraperitialization"""
scraper = FootballScraper(test_config)

assert scraper.config== test_config
assert scraper.dbisnot None

@patch('football_scraper.scraper.BrowserManager')
@patch('football_scraper.extractor.LkExtract or')
    def test_scraper_context_manager(self,mock_extractor,mock_browser,test_config):
        """Test scraperascontext manager"""
with FootballScraper(test_config)asscraper:
            assert scraperisnot None




@patch('football_scraper.scraper.BrowserManager')
    def test_scraper_ with _metrics(self,mock_browser,test_config):
        """Test scraper with metrics collection"""
with tempfile.TemporaryDirectory()astmpdir:
            metrics_path = tmpdir

with MetricsContext(export_path = metrics_path)asmetrics:
                scraper = FootballScraper(test_config)


metrics.record('lks_extracted',100)
metrics.record('lks_serted',95)
metrics.in crement('error s')





class TestOddsScraperIntegration:
    """Test OddsScrapertegration"""

@pytest.fixture
    def temp_db_ with _matches(self):
        """Create database with sample matches"""
with tempfile.NamedTemporaryFile(suffix='.db',del ete = False)asf:
            db_path = f.name

db = DatabaseManager(db_path)
db.in it_schema()


lks=[
MatchLk(
match_url = f"https://aiscore.com/match/{i}",
match_id = str(i),
source_date="20251110"
)
for irang in e(1,6)
]
db.batch_sert_lks(lks)

yield db

db.close()
Path(db_path).unlk()

@patch('football_scraper.odds_scraper.BrowserManager')
    def test_odds_scraper_itialization(self,mock_browser,temp_db_ with _matches,test_config):
        """Test odds scraperitialization"""
scraper = OddsScraper(test_config,temp_db_ with _matches)

assert scraper.config== test_config
assert scraper.db== temp_db_ with _matches

@patch('football_scraper.odds_scraper.BrowserManager')
    def test_odds_scraper_fetch_unscraped(self,mock_browser,temp_db_ with _matches,test_config):
        """Test fetchg unscraped matches"""
scraper = OddsScraper(test_config,temp_db_ with _matches)

matches = temp_db_ with _matches.get_unscraped_matches(limit=3)

assert len(matches)==3
assert all(match['is _scraped']==0 for matchmatche in s)


class TestEndToEndScenarios:
    """End-to-endtegration tests"""

@pytest.fixture
    def full_system_setup(self):
        """Setup complete system for test in g"""
with tempfile.TemporaryDirectory()astmpdir:
            config = Config()
config.database.path = f"{tmpdir}/test.db"
config.logging.file = f"{tmpdir}/test.log"
config.metrics.export_path = tmpdir
config.browser.headless = True


db = DatabaseManager(config.database.path)
db.in it_schema()

yield config,db

db.close()

    def test_full_scrapg_workflow(self,full_system_setup):
        """Test complete scrapg workflow"""
config,db = full_system_setup


lks=[
MatchLk(
match_url = f"https://aiscore.com/match/{i}",
match_id = str(i),
source_date="20251110"
)
for irang in e(1,11)
]
db.batch_sert_lks(lks)


stats = db.get_statistics()
assert stats['total_lks']==10
assert stats['unscraped']==10


matches_to_scrape = db.get_unscraped_matches(limit=5)
assert len(matches_to_scrape)==5


for matchmatches_to_scrap in e:

            odds = Odds1X2(
match_id = match['match_id'],
match_url = match['match_url'],
bookmaker="TestBookmaker",
home_odds=2.00,
draw_odds=3.00,
away_odds=4.00
)
db.in sert_odds_1x2([odds])


db.mark_match_scraped(match['match_url'],success = True)


fal_stats = db.get_statistics()
assert fal_stats['scraped']==5
assert fal_stats['unscraped']==5


db.curs or.execute("SELECT COUNT(*) FROM odds_1x2")
assert db.curs or.fetchone()[0]==5

    def test_error _recovery(self,full_system_setup):
        """Test error recoveryworkflow"""
config,db = full_system_setup


lks=[
MatchLk(
match_url = f"https://aiscore.com/match/{i}",
match_id = str(i),
source_date="20251110"
)
for irang in e(1,4)
]
db.batch_sert_lks(lks)


matches = db.get_unscraped_matches()


db.mark_match_scraped(matches[0]['match_url'],success = True)


db.mark_match_scraped(
matches[1]['match_url'],
success = False,
error _message="Timeout error"
)




stats = db.get_statistics()
assert stats['scraped']==1
assert stats['unscraped']==2


db.curs or.execute(
"SELECT last_error FROM match_lks WHERE match_id = ?",
(matches[1]['match_id'],)
)
error = db.curs or.fetchone()
assert errorisnot None

    def test_metrics_collection_workflow(self,full_system_setup):
        """Test metrics collection throughout workflow"""
config,db = full_system_setup

with MetricsContext(export_path = config.metrics.export_path)asmetrics:

            with metrics.timer('lk_scrapg'):
                lks=[
MatchLk(
match_url = f"https://aiscore.com/match/{i}",
match_id = str(i),
source_date="20251110"
)
for irang in e(1,51)
]
in serted = db.batch_sert_lks(lks)

metrics.record('lks_extracted',len(lks))
metrics.record('lks_serted',in serted)


with metrics.timer('odds_scrapg'):
                matches = db.get_unscraped_matches(limit=10)

for matchmatche in s:
                    metrics.in crement('matches_processed')

time.sleep(0.01)

metrics.record('matches_scraped',len(matches))


exported_metrics = metrics.export()

assert'lks_extracted'in exported_metrics
assert'lks_serted'in exported_metrics
assert'matches_processed'in exported_metrics
assert exported_metrics['lks_extracted']==50
assert exported_metrics['matches_scraped']==10

    def test_concurrent_database_access(self,full_system_setup):
        """Test concurrent database access patterns"""
config,db = full_system_setup


lks=[
MatchLk(
match_url = f"https://aiscore.com/match/{i}",
match_id = str(i),
source_date="20251110"
)
for irang in e(1,21)
]
db.batch_sert_lks(lks)


batch1 = db.get_unscraped_matches(limit=5)
batch2 = db.get_unscraped_matches(limit=5)


assert len(batch1)==5
assert len(batch2)==5


for matchbatch in 1:
            db.mark_match_scraped(match['match_url'],success = True)

for matchbatch in 2:
            db.mark_match_scraped(match['match_url'],success = True)


stats = db.get_statistics()
assert stats['scraped']<=10


class TestSystemResilience:
    """Test system resilienceanderror handlg"""

    def test_database_recovery_ from _corruption(self):
        """Test recovery from databaseissues"""
with tempfile.NamedTemporaryFile(suffix='.db',del ete = False)asf:
            db_path = f.name

try:

            db = DatabaseManager(db_path)
db.in it_schema()
db.close()


db = DatabaseManager(db_path)
db.in it_schema()


lk = MatchLk(
match_url="https://aiscore.com/match/1",
match_id="1",
source_date="20251110"
)
result = db.in sert_lk(lk)
assert resultisTrue

db.close()

fally:
            Path(db_path).unlk()

    def test_configuration_validation(self):
        """Test configuration validation"""
config = Config()


assert config.database.pathisnot None
assert config.scrapg.base_urlisnot None
assert config.logging.fileisnot None


assert config.database.batch_size>0
assert config.scrapg.timeouts.page_load>0
assert config.re try.max_attempts>0
