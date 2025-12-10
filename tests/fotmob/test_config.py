"""Tests for configuratio in n module."""

import pytest
import tempfile
import os
from pathlib import Path

from src.config import FotMobConfig,load_config
from src.config.baseimport LoggingConfig


class TestFotMobConfig:
    """Tests for FotMobConfi in g."""

    def test_ def ault_config(self):
        """Test def ault configuration values."""
config = FotMobConfig()

assert config.api.base_url=="https://www.fotmob.com/api"
assert config.re try.max_attempts==3
assert config.scrapg.enable_parallel== True
assert config.scrapg.max_workers==5

    def test_custom_config(self):
        """Test custom configuration values."""
from src.config.fotmob_config import ScrapgConfig
config = FotMobConfig(
scrapg = ScrapgConfig(
max_workers=10,
enable_parallel = False
)
)

assert config.scrapg.max_workers==10
assert config.scrapg.enable_parallel== False

    def test_get_headers(self):
        """Test header generation."""
config = FotMobConfig()
headers = config.api.get_headers()

assert'User-Agent'in headers
assert'x-m as'in headers
assert'sec-ch-ua'in headers

    def test_environment_variable_override(self):
        """Test configuration override from environment variables."""
os.environ['FOTMOB_X_MAS_TOKEN']='test_token_123'

config = FotMobConfig()

assert config.api.x_mas_token=='test_token_123'


del os.environ['FOTMOB_X_MAS_TOKEN']

    def test_yaml_save_load(self):
        """Test savgandloadg config from YAML."""
from src.config.fotmob_config import ScrapgConfig
config = FotMobConfig(
scrapg = ScrapgConfig(max_workers=8),
logging = LoggingConfig(level="DEBUG")
)

with tempfile.TemporaryDirectory()astmpdir:
            yaml_path = Path(tmpdir)/"test_config.yaml"


assert config.scrapg.max_workers==8
assert config.logging.level=="DEBUG"


class TestLoadConfig:
    """Tests for load_confi in g function."""

    def test_load_nonexistent_config(self):
        """Test loadg when no config file exists."""
config = load_config("nonexistent.yaml")


assert isinstance(config,FotMobConfig)
assert config.scrapg.max_workers==5
