"""Environment-based settings management for Scout project.

This module provides centralized environment configuration management.
All environment variables are loaded from .env file using python-dotenv.

Usage:
    from config.settings import settings, Environment
    
    if settings.environment == Environment.PROD:
        print("Running in production")
    
    print(f"Log level: {settings.log_level}")
"""

import os
from enum import Enum
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    # Load .env file from project root
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)
except ImportError:
    pass


class Environment(str, Enum):
    """Application environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"
    
    @classmethod
    def from_string(cls, value: str) -> 'Environment':
        """Convert string to Environment enum."""
        value = value.lower()
        for env in cls:
            if env.value == value:
                return env
        return cls.DEVELOPMENT


@dataclass
class Settings:
    """Global application settings loaded from environment variables.
    
    This provides a centralized place for application-wide configuration
    that isn't specific to a particular scraper.
    
    Environment Variables:
        ENVIRONMENT: Application environment (development/staging/production)
        LOG_LEVEL: Global log level (DEBUG/INFO/WARNING/ERROR)
        DATA_DIR: Base directory for all data storage
        CLICKHOUSE_HOST: ClickHouse database host
        CLICKHOUSE_PORT: ClickHouse database port
        CLICKHOUSE_USER: ClickHouse database user
        CLICKHOUSE_PASSWORD: ClickHouse database password
    """
    
    # Application environment
    environment: Environment = Environment.DEVELOPMENT
    
    # Logging
    log_level: str = "INFO"
    log_dir: str = "logs"
    
    # Data storage
    data_dir: str = "data"
    
    # ClickHouse database
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"
    
    # Feature flags
    enable_metrics: bool = True
    enable_health_checks: bool = True
    
    def __init__(self):
        """Initialize settings from environment variables."""
        self._load_from_env()
    
    def _load_from_env(self):
        """Load configuration from environment variables."""
        # Environment
        env_str = os.getenv('ENVIRONMENT', 'development')
        self.environment = Environment.from_string(env_str)
        
        # Logging
        self.log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        self.log_dir = os.getenv('LOG_DIR', 'logs')
        
        # Data storage
        self.data_dir = os.getenv('DATA_DIR', 'data')
        
        # ClickHouse
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', '')
        self.clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        
        # Feature flags
        self.enable_metrics = os.getenv('ENABLE_METRICS', 'true').lower() == 'true'
        self.enable_health_checks = os.getenv('ENABLE_HEALTH_CHECKS', 'true').lower() == 'true'
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == Environment.DEVELOPMENT
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PRODUCTION
    
    @property
    def is_testing(self) -> bool:
        """Check if running in testing environment."""
        return self.environment == Environment.TESTING
    
    def ensure_directories(self):
        """Create required directories if they don't exist."""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
    
    def to_dict(self) -> dict:
        """Convert settings to dictionary."""
        return {
            'environment': self.environment.value,
            'log_level': self.log_level,
            'log_dir': self.log_dir,
            'data_dir': self.data_dir,
            'clickhouse_host': self.clickhouse_host,
            'clickhouse_port': self.clickhouse_port,
            'clickhouse_database': self.clickhouse_database,
            'enable_metrics': self.enable_metrics,
            'enable_health_checks': self.enable_health_checks,
        }


# Global settings instance
settings = Settings()

# Ensure directories exist on import
settings.ensure_directories()


__all__ = ['settings', 'Settings', 'Environment']
