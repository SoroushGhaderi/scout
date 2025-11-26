"""Tests for configuration management"""

import pytest
import os
import tempfile
from pathlib import Path
import yaml

from src.scrapers.aiscore.config import (
    Config,
    DatabaseConfig,
    BrowserConfig,
    ScrapingConfig,
    LoggingConfig,
    MetricsConfig,
    ValidationConfig,
    RetryConfig
)
from src.scrapers.aiscore.exceptions import ConfigurationError


class TestDatabaseConfig:
    """Test DatabaseConfig dataclass"""
    
    def test_default_values(self):
        """Test default configuration values"""
        config = DatabaseConfig()
        
        assert config.path == "data/football_matches.db"
        assert config.batch_size == 100
        assert config.connection_timeout == 30
    
    def test_custom_values(self):
        """Test custom configuration values"""
        config = DatabaseConfig(
            path="/custom/path/db.sqlite",
            batch_size=500,
            connection_timeout=60
        )
        
        assert config.path == "/custom/path/db.sqlite"
        assert config.batch_size == 500
        assert config.connection_timeout == 60


class TestBrowserConfig:
    """Test BrowserConfig dataclass"""
    
    def test_default_values(self):
        """Test default browser configuration"""
        config = BrowserConfig()
        
        assert config.headless is True
        assert config.window_size == "1920x1080"
        assert config.block_images is True
        assert config.block_css is True
        assert config.block_fonts is True
        assert config.block_media is True
    
    def test_performance_mode(self):
        """Test performance optimization flags"""
        config = BrowserConfig(
            block_images=True,
            block_css=True,
            block_fonts=True,
            block_media=True
        )
        
        # All blocking enabled for performance
        assert config.block_images
        assert config.block_css
        assert config.block_fonts
        assert config.block_media
    
    def test_debug_mode(self):
        """Test debug mode configuration"""
        config = BrowserConfig(
            headless=False,
            block_images=False,
            block_css=False
        )
        
        assert config.headless is False
        assert config.block_images is False
        assert config.block_css is False


class TestScrapingConfig:
    """Test ScrapingConfig dataclass"""
    
    def test_default_values(self):
        """Test default scraping configuration"""
        config = ScrapingConfig()
        
        assert config.base_url == "https://www.aiscore.com"
        assert config.scroll is not None
        assert config.timeouts is not None
        assert config.delays is not None
    
    def test_nested_configs(self):
        """Test nested configuration access"""
        config = ScrapingConfig()
        
        # Scroll config
        assert config.scroll.increment > 0
        assert config.scroll.pause > 0
        assert config.scroll.max_no_change > 0
        
        # Timeout config
        assert config.timeouts.page_load > 0
        assert config.timeouts.element_wait > 0
        
        # Delay config
        assert config.delays.between_dates >= 0
        assert config.delays.after_click >= 0


class TestValidationConfig:
    """Test ValidationConfig dataclass"""
    
    def test_excluded_paths(self):
        """Test URL validation excluded paths"""
        config = ValidationConfig()
        
        assert "/h2h" in config.excluded_paths
        assert "/statistics" in config.excluded_paths
        assert "/odds" in config.excluded_paths
        assert "/predictions" in config.excluded_paths
    
    def test_required_pattern(self):
        """Test required URL pattern"""
        config = ValidationConfig()
        
        assert config.required_pattern == "/match"


class TestRetryConfig:
    """Test RetryConfig dataclass"""
    
    def test_exponential_backoff_params(self):
        """Test retry configuration parameters"""
        config = RetryConfig()
        
        assert config.max_attempts >= 1
        assert config.initial_wait > 0
        assert config.max_wait >= config.initial_wait
        assert config.exponential_base >= 2


class TestConfig:
    """Test main Config class"""
    
    @pytest.fixture
    def temp_config_file(self):
        """Create temporary config file"""
        config_data = {
            'database': {
                'path': 'test.db',
                'batch_size': 50
            },
            'browser': {
                'headless': False,
                'window_size': '1280x720'
            },
            'scraping': {
                'base_url': 'https://test.example.com'
            },
            'logging': {
                'level': 'DEBUG',
                'file': 'test.log'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        yield temp_file
        
        # Cleanup
        os.unlink(temp_file)
    
    def test_load_from_yaml(self, temp_config_file):
        """Test loading configuration from YAML file"""
        config = Config(config_file=temp_config_file)
        
        assert config.database.path == 'test.db'
        assert config.database.batch_size == 50
        assert config.browser.headless is False
        assert config.browser.window_size == '1280x720'
        assert config.scraping.base_url == 'https://test.example.com'
        assert config.logging.level == 'DEBUG'
    
    def test_load_with_missing_file(self):
        """Test loading with non-existent config file"""
        config = Config(config_file='nonexistent.yaml')
        
        # Should use defaults
        assert config.database.path == "data/football_matches.db"
        assert config.browser.headless is True
    
    def test_environment_overrides(self, temp_config_file, monkeypatch):
        """Test environment variable overrides"""
        # Set environment variables
        monkeypatch.setenv('DB_PATH', '/env/override.db')
        monkeypatch.setenv('HEADLESS', 'false')
        monkeypatch.setenv('LOG_LEVEL', 'ERROR')
        
        config = Config(config_file=temp_config_file)
        
        # Environment should override YAML
        assert config.database.path == '/env/override.db'
        assert config.browser.headless is False
        assert config.logging.level == 'ERROR'
    
    def test_ensure_directories(self, temp_config_file):
        """Test directory creation"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(config_file=temp_config_file)
            config.database.path = f"{tmpdir}/data/test.db"
            config.logging.file = f"{tmpdir}/logs/test.log"
            config.metrics.export_path = f"{tmpdir}/metrics"
            
            config.ensure_directories()
            
            # Check directories were created
            assert Path(tmpdir, 'data').exists()
            assert Path(tmpdir, 'logs').exists()
            assert Path(tmpdir, 'metrics').exists()
    
    def test_to_dict(self, temp_config_file):
        """Test converting config to dictionary"""
        config = Config(config_file=temp_config_file)
        config_dict = config.to_dict()
        
        assert isinstance(config_dict, dict)
        assert 'database' in config_dict
        assert 'browser' in config_dict
        assert 'scraping' in config_dict
        assert 'logging' in config_dict
        
        # Check nested values
        assert config_dict['database']['path'] == 'test.db'
        assert config_dict['browser']['headless'] is False
    
    def test_partial_config_file(self):
        """Test loading with partial configuration"""
        config_data = {
            'database': {
                'path': 'custom.db'
            }
            # Other sections missing
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            config = Config(config_file=temp_file)
            
            # Custom value
            assert config.database.path == 'custom.db'
            
            # Defaults for missing sections
            assert config.database.batch_size == 100
            assert config.browser.headless is True
            assert config.scraping.base_url == "https://www.aiscore.com"
        
        finally:
            os.unlink(temp_file)
    
    def test_invalid_yaml_file(self):
        """Test handling of invalid YAML file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [[[")
            temp_file = f.name
        
        try:
            with pytest.raises(Exception):  # Should raise YAML parsing error
                Config(config_file=temp_file)
        
        finally:
            os.unlink(temp_file)
    
    def test_config_immutability(self, temp_config_file):
        """Test that config sections are properly initialized"""
        config = Config(config_file=temp_config_file)
        
        # All sections should be properly initialized
        assert isinstance(config.database, DatabaseConfig)
        assert isinstance(config.browser, BrowserConfig)
        assert isinstance(config.scraping, ScrapingConfig)
        assert isinstance(config.logging, LoggingConfig)
        assert isinstance(config.metrics, MetricsConfig)
        assert isinstance(config.validation, ValidationConfig)
        assert isinstance(config.retry, RetryConfig)


class TestConfigIntegration:
    """Integration tests for configuration"""
    
    def test_production_config(self):
        """Test production-like configuration"""
        config = Config()
        
        # Production settings
        assert config.browser.headless is True
        assert config.browser.block_images is True
        assert config.logging.level in ['INFO', 'WARNING', 'ERROR']
        assert config.metrics.enabled is True
    
    def test_development_config(self):
        """Test development configuration"""
        config_data = {
            'browser': {
                'headless': False,
                'block_images': False,
                'block_css': False
            },
            'logging': {
                'level': 'DEBUG'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            config = Config(config_file=temp_file)
            
            # Development settings
            assert config.browser.headless is False
            assert config.browser.block_images is False
            assert config.logging.level == 'DEBUG'
        
        finally:
            os.unlink(temp_file)
    
    def test_config_override_priority(self, monkeypatch):
        """Test configuration override priority: ENV > YAML > Defaults"""
        # Create YAML with custom values
        config_data = {
            'database': {
                'path': 'yaml.db'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            # Set environment variable
            monkeypatch.setenv('DB_PATH', 'env.db')
            
            config = Config(config_file=temp_file)
            
            # ENV should win
            assert config.database.path == 'env.db'
            
            # Values not in ENV or YAML should use defaults
            assert config.database.batch_size == 100
        
        finally:
            os.unlink(temp_file)

