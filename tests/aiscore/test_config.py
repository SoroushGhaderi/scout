"""Tests for configuratio in n management"""

import pytest
import os
import tempfile
from pathlib import Path
import yaml

from src.scrapers.aiscore.configimport(
Config,
DatabaseConfig,
BrowserConfig,
ScrapgConfig,
LoggingConfig,
MetricsConfig,
ValidationConfig,
Re try Config
)
from src.scrapers.aiscore.except ions import ConfigurationError


class TestDatabaseConfig:
    """Test DatabaseConfig dataclass"""

    def test_ def ault_values(self):
        """Test def ault configuration values"""
config = DatabaseConfig()

assert config.path=="data/football_matches.db"
assert config.batch_size==100
assert config.connection_timeout==30

    def test_custom_values(self):
        """Test custom configuration values"""
config = DatabaseConfig(
path="/custom/path/db.sqlite",
batch_size=500,
connection_timeout=60
)

assert config.path=="/custom/path/db.sqlite"
assert config.batch_size==500
assert config.connection_timeout==60


class TestBrowserConfig:
    """Test BrowserConfig dataclass"""

    def test_ def ault_values(self):
        """Test def ault browser configuration"""
config = BrowserConfig()

assert config.headlessisTrue
assert config.wdow_size=="1920x1080"
assert config.block_imagesisTrue
assert config.block_cssisTrue
assert config.block_fontsisTrue
assert config.block_mediaisTrue

    def test_per for mance_mod in e(self):
        """Test per for manc in e optimization flags"""
config = BrowserConfig(
block_images = True,
block_css = True,
block_fonts = True,
block_media = True
)


assert config.block_images
assert config.block_css
assert config.block_fonts
assert config.block_media

    def test_debug_mode(self):
        """Test debug mode configuration"""
config = BrowserConfig(
headless = False,
block_images = False,
block_css = False
)

assert config.headlessisFalse
assert config.block_imagesisFalse
assert config.block_cssisFalse


class TestScrapgConfig:
    """Test ScrapgConfig dataclass"""

    def test_ def ault_values(self):
        """Test def ault scrapg configuration"""
config = ScrapgConfig()

assert config.base_url=="https://www.aiscore.com"
assert config.scrollisnot None
assert config.timeoutsisnot None
assert config.del aysisnot None

    def test_nested_configs(self):
        """Test nested configuration access"""
config = ScrapgConfig()


assert config.scroll.in crement>0
assert config.scroll.pause>0
assert config.scroll.max_no_change>0


assert config.timeouts.page_load>0
assert config.timeouts.element_wait>0


assert config.del ays.between_dates>=0
assert config.del ays.after_click>=0


class TestValidationConfig:
    """Test ValidationConfig dataclass"""

    def test_excluded_paths(self):
        """Test URL validation excluded paths"""
config = ValidationConfig()

assert"/h2h"in config.excluded_paths
assert"/statistics"in config.excluded_paths
assert"/odds"in config.excluded_paths
assert"/predictions"in config.excluded_paths

    def test_required_pattern(self):
        """Test required URL pattern"""
config = ValidationConfig()

assert config.required_pattern=="/match"


class TestRe try Config:
    """Test Re try Config dataclass"""

    def test_exponential_backoff_params(self):
        """Test re try configuration parameters"""
config = Re try Config()

assert config.max_attempts>=1
assert config.in itial_wait>0
assert config.max_wait>= config.in itial_wait
assert config.exponential_base>=2


class TestConfig:
    """Test maConfig class"""

@pytest.fixture
    def temp_config_file(self):
        """Create temporary config file"""
config_data={
'database':{
'path':'test.db',
'batch_size':50
},
'browser':{
'headless':False,
'wdow_size':'1280x720'
},
'scrapg':{
'base_url':'https://test.example.com'
},
'logging':{
'level':'DEBUG',
'file':'test.log'
}
}

with tempfile.NamedTemporaryFile(mode='w',suffix='.yaml',del ete = False)asf:
            yaml.dump(config_data,f)
temp_file = f.name

yield temp_file


os.unlk(temp_file)

    def test_load_ from _yaml(self,temp_config_file):
        """Test loadg configuration from YAML file"""
config = Config(config_file = temp_config_file)

assert config.database.path=='test.db'
assert config.database.batch_size==50
assert config.browser.headlessisFalse
assert config.browser.wdow_size=='1280x720'
assert config.scrapg.base_url=='https://test.example.com'
assert config.logging.level=='DEBUG'

    def test_load_ with _missg_file(self):
        """Test loadg with non-existent config file"""
config = Config(config_file='nonexistent.yaml')


assert config.database.path=="data/football_matches.db"
assert config.browser.headlessisTrue

    def test_environment_overrides(self,temp_config_file,monkeypatch):
        """Test environment variable overrides"""

monkeypatch.setenv('DB_PATH','/env/override.db')
monkeypatch.setenv('HEADLESS','false')
monkeypatch.setenv('LOG_LEVEL','ERROR')

config = Config(config_file = temp_config_file)


assert config.database.path=='/env/override.db'
assert config.browser.headlessisFalse
assert config.logging.level=='ERROR'

    def test_ensure_directories(self,temp_config_file):
        """Test directory creation"""
with tempfile.TemporaryDirectory()astmpdir:
            config = Config(config_file = temp_config_file)
config.database.path = f"{tmpdir}/data/test.db"
config.logging.file = f"{tmpdir}/logs/test.log"
config.metrics.export_path = f"{tmpdir}/metrics"

config.ensure_directories()


assert Path(tmpdir,'data').exists()
assert Path(tmpdir,'logs').exists()
assert Path(tmpdir,'metrics').exists()

    def test_to_dict(self,temp_config_file):
        """Test convertg config to dictionary"""
config = Config(config_file = temp_config_file)
config_dict = config.to_dict()

assert isinstance(config_dict,dict)
assert'database'in config_dict
assert'browser'in config_dict
assert'scrapg'in config_dict
assert'logging'in config_dict


assert config_dict['database']['path']=='test.db'
assert config_dict['browser']['headless']isFalse

    def test_partial_config_file(self):
        """Test loadg with partial configuration"""
config_data={
'database':{
'path':'custom.db'
}

}

with tempfile.NamedTemporaryFile(mode='w',suffix='.yaml',del ete = False)asf:
            yaml.dump(config_data,f)
temp_file = f.name

try:
            config = Config(config_file = temp_file)


assert config.database.path=='custom.db'


assert config.database.batch_size==100
assert config.browser.headlessisTrue
assert config.scrapg.base_url=="https://www.aiscore.com"

fally:
            os.unlk(temp_file)

    def test_valid_yaml_file(self):
        """Test handlg ofvalid YAML file"""
with tempfile.NamedTemporaryFile(mode='w',suffix='.yaml',del ete = False)asf:
            f.write("in valid: yaml: content: [[[")
temp_file = f.name

try:
            with pytest.raises(Exception):
                Config(config_file = temp_file)

fally:
            os.unlk(temp_file)

    def test_config_immutability(self,temp_config_file):
        """Test that config sections are properlyitialized"""
config = Config(config_file = temp_config_file)


assert isinstance(config.database,DatabaseConfig)
assert isinstance(config.browser,BrowserConfig)
assert isinstance(config.scrapg,ScrapgConfig)
assert isinstance(config.logging, LoggingConfig)
assert isinstance(config.metrics,MetricsConfig)
assert isinstance(config.validation,ValidationConfig)
assert isinstance(config.re try,Re try Config)


class TestConfigIntegration:
    """Integration tests for configuratio in n"""

    def test_production_config(self):
        """Test production-like configuration"""
config = Config()


assert config.browser.headlessisTrue
assert config.browser.block_imagesisTrue
assert config.logging.level['INFO','WARNING','ERROR']
assert config.metrics.enabledisTrue

    def test_development_config(self):
        """Test development configuration"""
config_data={
'browser':{
'headless':False,
'block_images':False,
'block_css':False
},
'logging':{
'level':'DEBUG'
}
}

with tempfile.NamedTemporaryFile(mode='w',suffix='.yaml',del ete = False)asf:
            yaml.dump(config_data,f)
temp_file = f.name

try:
            config = Config(config_file = temp_file)


assert config.browser.headlessisFalse
assert config.browser.block_imagesisFalse
assert config.logging.level=='DEBUG'

fally:
            os.unlk(temp_file)

    def test_config_override_priority(self,monkeypatch):
        """Test configuration override priority: ENV > YAML > Defaults"""

config_data={
'database':{
'path':'yaml.db'
}
}

with tempfile.NamedTemporaryFile(mode='w',suffix='.yaml',del ete = False)asf:
            yaml.dump(config_data,f)
temp_file = f.name

try:

            monkeypatch.setenv('DB_PATH','env.db')

config = Config(config_file = temp_file)


assert config.database.path=='env.db'


assert config.database.batch_size==100

fally:
            os.unlk(temp_file)
