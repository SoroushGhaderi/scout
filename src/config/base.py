"""
Base configuration classes for unified scraper configuration system.

Configuration is read ONLY from environment variables (.env file).

This follows industry best practices for configuration management.

Features:
- Type-safe data classes
- Environment variable based configuration
- Hierarchical configuration structure
- Validation and defaults
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from pathlib import Path
from abc import ABC

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class StorageConfig:
    """Base storage configuration for data lake architecture."""
    bronze_path: str = ""
    enabled: bool = True

    def ensure_directories(self):
        """Create storage directories if they don't exist."""
        if self.enabled and self.bronze_path:
            Path(self.bronze_path).mkdir(parents=True, exist_ok=True)


@dataclass
class LoggingConfig:
    """Standardized logging configuration."""
    level: str = "INFO"
    format: str = (
        "%(asctime)s - %(name)s - %(levelname)s - "
        "%(funcName)s:%(lineno)d - %(message)s"
    )
    file: str = "logs/scraper.log"
    max_bytes: int = 10485760
    backup_count: int = 5
    dir: str = "logs"

    def ensure_directories(self):
        """Create log directory if it doesn't exist."""
        Path(self.dir).mkdir(parents=True, exist_ok=True)
        Path(self.file).parent.mkdir(parents=True, exist_ok=True)


@dataclass
class MetricsConfig:
    """Standardized metrics configuration."""
    enabled: bool = True
    export_path: str = "metrics"
    export_format: str = "json"

    def ensure_directories(self):
        """Create metrics directory if it doesn't exist."""
        if self.enabled:
            Path(self.export_path).mkdir(parents=True, exist_ok=True)


@dataclass
class RetryConfig:
    """Standardized retry configuration."""
    max_attempts: int = 3
    initial_wait: float = 2.0
    max_wait: float = 10.0
    exponential_base: float = 2.0
    backoff_factor: float = 2.0
    status_codes: tuple = field(
        default_factory=lambda: (429, 500, 502, 503, 504)
    )


class BaseConfig(ABC):
    """
    Base configuration class with common functionality.

    All scraper configs should inherit from this class to ensure consistency.

    Configuration is read ONLY from environment variables (.env file).
    """

    def __init__(self):
        """Initialize configuration from environment variables."""
        self._load_config()
        self._apply_env_overrides()
        self._ensure_directories()

    def _load_config(self):
        """Initialize configuration with defaults. Overridden by subclasses."""
        pass

    def _apply_env_overrides(self):
        """
        Apply environment variable overrides.

        Subclasses should override this to add scraper-specific env vars.

        Pattern: {SCRAPER_NAME}_{CONFIG_KEY} (e.g., FOTMOB_X_MAS_TOKEN)
        """
        if hasattr(self, 'logging') and isinstance(self.logging, LoggingConfig):
            if os.getenv('LOG_LEVEL'):
                self.logging.level = os.getenv('LOG_LEVEL')
            if os.getenv('LOG_FILE'):
                self.logging.file = os.getenv('LOG_FILE')

        if hasattr(self, 'metrics') and isinstance(self.metrics, MetricsConfig):
            if os.getenv('METRICS_ENABLED'):
                self.metrics.enabled = (
                    os.getenv('METRICS_ENABLED').lower() == 'true'
                )

    def _ensure_directories(self):
        """Ensure all required directories exist."""
        data_path = Path("data")

        if data_path.exists():
            if data_path.is_dir():
                pass
            elif data_path.is_file():
                raise OSError(
                    f"Cannot create directory 'data': A file with that name "
                    f"already exists. Path: {data_path.absolute()}. "
                    f"Please remove or rename the file."
                )
            else:
                raise OSError(
                    f"Cannot create directory 'data': A non-directory with "
                    f"that name already exists. Path: {data_path.absolute()}. "
                    f"Please remove or rename it."
                )
        else:
            try:
                data_path.mkdir(parents=True, exist_ok=True)
            except FileExistsError as e:
                if data_path.exists() and data_path.is_dir():
                    pass
                else:
                    raise OSError(
                        f"Cannot create directory 'data': A file or "
                        f"non-directory with that name exists. "
                        f"Path: {data_path.absolute()}. "
                        f"Please remove or rename it."
                    ) from e
            except OSError as e:
                if e.errno == 17:
                    if data_path.exists() and data_path.is_dir():
                        pass
                    else:
                        raise OSError(
                            f"Cannot create directory 'data': A file or "
                            f"non-directory with that name exists. "
                            f"Path: {data_path.absolute()}. "
                            f"Please remove or rename it."
                        ) from e
                else:
                    raise

        for field_name, field_value in self.__dict__.items():
            if isinstance(
                field_value, (StorageConfig, LoggingConfig, MetricsConfig)
            ):
                field_value.ensure_directories()

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.

        Returns:
            Dictionary representation of configuration
        """
        result = {}
        for field_name, field_value in self.__dict__.items():
            if field_name.startswith('_'):
                continue
            if isinstance(
                field_value,
                (StorageConfig, LoggingConfig, MetricsConfig, RetryConfig)
            ):
                result[field_name] = field_value.__dict__
            elif isinstance(
                field_value, (list, dict, str, int, float, bool, type(None))
            ):
                result[field_name] = field_value
            else:
                result[field_name] = (
                    field_value.__dict__
                    if hasattr(field_value, '__dict__')
                    else str(field_value)
                )
        return result

    def validate(self) -> List[str]:
        """
        Validate configuration values.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if hasattr(self, 'logging') and isinstance(self.logging, LoggingConfig):
            valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if self.logging.level not in valid_levels:
                errors.append(
                    f"Invalid log level: {self.logging.level}. "
                    f"Must be one of {valid_levels}"
                )

        if hasattr(self, 'storage') and isinstance(self.storage, StorageConfig):
            if not self.storage.bronze_path:
                errors.append("bronze_path cannot be empty")

        return errors
