"""

FotMob scraper configuration.

Configuration is loaded from:
1. config.yaml - Application settings (primary source)
2. .env file - Environment-specific & sensitive data (overrides)

"""





import os
import json
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path

from .base import BaseConfig, StorageConfig, LoggingConfig, MetricsConfig, RetryConfig


@dataclass
class ApiConfig:
    """FotMob API configuration."""
    base_url: str = "https://www.fotmob.com/api/data"
    user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    x_mas_token: str = ""
    cookies: str = ""
    user_agents: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    ])

    def get_headers(self, referer: str = "https://www.fotmob.com/") -> Dict[str, str]:
        """Get HTTP headers for API requests with random User-Agent."""
        user_agent = random.choice(self.user_agents)
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,fa;q=0.8",
            "priority": "u=1, i",
            "sec-ch-ua-platform": '"macOS"',
            "Referer": referer,
            "User-Agent": user_agent,
            "x-mas": self.x_mas_token,
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }
        if self.cookies:
            headers["Cookie"] = self._format_cookies(self.cookies)
        return headers

    def _format_cookies(self, cookies_input: str) -> str:
        """Convert JSON cookies to cookie header format."""
        try:
            cookies_dict = json.loads(cookies_input)
            return "; ".join(f"{k}={v}" for k, v in cookies_dict.items())
        except (json.JSONDecodeError, AttributeError):
            return cookies_input


@dataclass
class RequestConfig:
    """HTTP request configuration."""
    timeout: int = 30
    delay_min: float = 2.0
    delay_max: float = 4.0


@dataclass
class ScrapingConfig:
    """Scraping behavior configuration."""
    max_workers: int = 1
    enable_parallel: bool = False
    metrics_update_interval: int = 20
    filter_by_status: bool = True
    allowed_match_statuses: tuple = field(default_factory=lambda: (
        "Finished", "FullTime", "FT",
        "After Extra Time", "AET",
        "After Penalties", "AP"
    ))


@dataclass
class DataQualityConfig:
    """Data quality checking configuration."""
    enabled: bool = True
    fail_on_issues: bool = False


@dataclass
class ProxyConfig:
    """Proxy configuration."""
    enabled: bool = False
    http: str = ""
    https: str = ""


class FotMobConfig(BaseConfig):
    """

    FotMob scraper configuration.

    Configuration is read ONLY from environment variables (.env file).

    Usage:

        config = FotMobConfig()

        print(config.api.base_url)

        print(config.storage.bronze_path)

    Required Environment Variables:

        FOTMOB_X_MAS_TOKEN: API authentication token

    See .env.example for all available configuration options.

    """















    def __init__(self):
        """Initialize FotMob configuration from environment variables."""
        self._load_config()
        self._apply_env_overrides()
        self._ensure_directories()

    def _load_config(self):
        """Initialize configuration from config.yaml with defaults as fallback."""
        # Get FOTMOB config from YAML or use defaults
        yaml_fotmob = self._yaml_config.get('fotmob', {}) if hasattr(self, '_yaml_config') else {}
        
        # Initialize with defaults first
        default_user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        ]

        api_config = yaml_fotmob.get('api', {})
        self.api = ApiConfig(
            base_url=api_config.get('base_url', "https://www.fotmob.com/api/data"),
            user_agent=api_config.get('user_agent', ApiConfig.user_agent),
            x_mas_token="",  # Will be loaded from .env
            user_agents=api_config.get('user_agents', default_user_agents),
        )

        request_config = yaml_fotmob.get('request', {})
        self.request = RequestConfig(
            timeout=request_config.get('timeout', 30),
            delay_min=request_config.get('delay_min', 2.0),
            delay_max=request_config.get('delay_max', 4.0),
        )

        scraping_config = yaml_fotmob.get('scraping', {})
        self.scraping = ScrapingConfig(
            max_workers=scraping_config.get('max_workers', 1),
            enable_parallel=scraping_config.get('enable_parallel', False),
            metrics_update_interval=scraping_config.get('metrics_update_interval', 20),
            filter_by_status=scraping_config.get('filter_by_status', True),
            allowed_match_statuses=tuple(scraping_config.get('allowed_match_statuses', [
                "Finished", "FullTime", "FT",
                "After Extra Time", "AET",
                "After Penalties", "AP"
            ])),
        )

        storage_config = yaml_fotmob.get('storage', {})
        self.storage = StorageConfig(
            bronze_path=storage_config.get('bronze_path', "data/fotmob"),
            enabled=storage_config.get('enabled', True),
        )

        retry_config = yaml_fotmob.get('retry', {})
        self.retry = RetryConfig(
            max_attempts=retry_config.get('max_attempts', 3),
            initial_wait=retry_config.get('initial_wait', 2.0),
            max_wait=retry_config.get('max_wait', 10.0),
            exponential_base=retry_config.get('exponential_base', 2.0),
            backoff_factor=retry_config.get('backoff_factor', 2.0),
            status_codes=tuple(retry_config.get('status_codes', [429, 500, 502, 503, 504])),
        )

        fotmob_logging = yaml_fotmob.get('logging', {})
        self.logging = LoggingConfig(
            level=fotmob_logging.get('level', "INFO"),
            format=fotmob_logging.get('format', "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            file=fotmob_logging.get('file', "logs/fotmob_scraper.log"),
            max_bytes=fotmob_logging.get('max_bytes', 10485760),
            backup_count=fotmob_logging.get('backup_count', 5),
            dir=fotmob_logging.get('dir', "logs"),
        )

        fotmob_metrics = yaml_fotmob.get('metrics', {})
        self.metrics = MetricsConfig(
            enabled=fotmob_metrics.get('enabled', True),
            export_path=fotmob_metrics.get('export_path', "metrics"),
            export_format=fotmob_metrics.get('export_format', "json"),
        )

        data_quality_config = yaml_fotmob.get('data_quality', {})
        self.data_quality = DataQualityConfig(
            enabled=data_quality_config.get('enabled', True),
            fail_on_issues=data_quality_config.get('fail_on_issues', False),
        )

        proxy_config = yaml_fotmob.get('proxy', {})
        self.proxy = ProxyConfig(
            enabled=proxy_config.get('enabled', False),
        )

    def _apply_env_overrides(self):
        """Load configuration from environment variables (.env file)."""
        super()._apply_env_overrides()

        if os.getenv('FOTMOB_X_MAS_TOKEN'):
            self.api.x_mas_token = os.getenv('FOTMOB_X_MAS_TOKEN')
        if os.getenv('FOTMOB_USER_AGENT'):
            self.api.user_agent = os.getenv('FOTMOB_USER_AGENT')
        if os.getenv('FOTMOB_API_BASE_URL'):
            self.api.base_url = os.getenv('FOTMOB_API_BASE_URL')
        if os.getenv('FOTMOB_COOKIES'):
            self.api.cookies = os.getenv('FOTMOB_COOKIES')

        if os.getenv('FOTMOB_REQUEST_TIMEOUT'):
            self.request.timeout = int(os.getenv('FOTMOB_REQUEST_TIMEOUT'))
        if os.getenv('FOTMOB_DELAY_MIN'):
            self.request.delay_min = float(os.getenv('FOTMOB_DELAY_MIN'))
        if os.getenv('FOTMOB_DELAY_MAX'):
            self.request.delay_max = float(os.getenv('FOTMOB_DELAY_MAX'))

        if os.getenv('FOTMOB_MAX_WORKERS'):
            self.scraping.max_workers = int(os.getenv('FOTMOB_MAX_WORKERS'))
        if os.getenv('FOTMOB_ENABLE_PARALLEL'):
            self.scraping.enable_parallel = os.getenv('FOTMOB_ENABLE_PARALLEL').lower() == 'true'
        if os.getenv('FOTMOB_ENABLE_CACHING'):
            self.scraping.enable_caching = os.getenv('FOTMOB_ENABLE_CACHING').lower() == 'true'
        if os.getenv('FOTMOB_CACHE_TTL_HOURS'):
            self.scraping.cache_ttl_hours = int(os.getenv('FOTMOB_CACHE_TTL_HOURS'))
        if os.getenv('FOTMOB_METRICS_UPDATE_INTERVAL'):
            self.scraping.metrics_update_interval = int(os.getenv('FOTMOB_METRICS_UPDATE_INTERVAL'))
        if os.getenv('FOTMOB_FILTER_BY_STATUS'):
            self.scraping.filter_by_status = os.getenv('FOTMOB_FILTER_BY_STATUS').lower() == 'true'
        if os.getenv('FOTMOB_ALLOWED_MATCH_STATUSES'):
            statuses = [s.strip() for s in os.getenv('FOTMOB_ALLOWED_MATCH_STATUSES').split(',')]
            self.scraping.allowed_match_statuses = tuple(statuses)

        if os.getenv('FOTMOB_BRONZE_PATH'):
            self.storage.bronze_path = os.getenv('FOTMOB_BRONZE_PATH')
        if os.getenv('FOTMOB_STORAGE_ENABLED'):
            self.storage.enabled = os.getenv('FOTMOB_STORAGE_ENABLED').lower() == 'true'

        if os.getenv('FOTMOB_RETRY_MAX_ATTEMPTS'):
            self.retry.max_attempts = int(os.getenv('FOTMOB_RETRY_MAX_ATTEMPTS'))
        if os.getenv('FOTMOB_RETRY_INITIAL_WAIT'):
            self.retry.initial_wait = float(os.getenv('FOTMOB_RETRY_INITIAL_WAIT'))
        if os.getenv('FOTMOB_RETRY_MAX_WAIT'):
            self.retry.max_wait = float(os.getenv('FOTMOB_RETRY_MAX_WAIT'))

        if os.getenv('FOTMOB_DATA_QUALITY_ENABLED'):
            self.data_quality.enabled = os.getenv('FOTMOB_DATA_QUALITY_ENABLED').lower() == 'true'
        if os.getenv('FOTMOB_DATA_QUALITY_FAIL_ON_ISSUES'):
            self.data_quality.fail_on_issues = os.getenv('FOTMOB_DATA_QUALITY_FAIL_ON_ISSUES').lower() == 'true'

        if os.getenv('FOTMOB_PROXY_ENABLED'):
            self.proxy.enabled = os.getenv('FOTMOB_PROXY_ENABLED').lower() == 'true'
        if os.getenv('FOTMOB_PROXY_HTTP'):
            self.proxy.http = os.getenv('FOTMOB_PROXY_HTTP')
        if os.getenv('FOTMOB_PROXY_HTTPS'):
            self.proxy.https = os.getenv('FOTMOB_PROXY_HTTPS')


    @property
    def api_base_url(self) -> str:
        """Backward compatibility: api.base_url"""
        return self.api.base_url

    @property
    def user_agent(self) -> str:
        """Backward compatibility: api.user_agent"""
        return self.api.user_agent

    @property
    def x_mas_token(self) -> str:
        """Backward compatibility: api.x_mas_token"""
        return self.api.x_mas_token

    @property
    def user_agents(self) -> List[str]:
        """Backward compatibility: api.user_agents"""
        return self.api.user_agents

    @property
    def request_timeout(self) -> int:
        """Backward compatibility: request.timeout"""
        return self.request.timeout

    @property
    def request_delay_min(self) -> float:
        """Backward compatibility: request.delay_min"""
        return self.request.delay_min

    @property
    def request_delay_max(self) -> float:
        """Backward compatibility: request.delay_max"""
        return self.request.delay_max

    @property
    def max_workers(self) -> int:
        """Backward compatibility: scraping.max_workers"""
        return self.scraping.max_workers

    @property
    def enable_parallel(self) -> bool:
        """Backward compatibility: scraping.enable_parallel"""
        return self.scraping.enable_parallel

    @property
    def enable_caching(self) -> bool:
        """Backward compatibility: scraping.enable_caching"""
        return self.scraping.enable_caching

    @property
    def cache_ttl_hours(self) -> int:
        """Backward compatibility: scraping.cache_ttl_hours"""
        return self.scraping.cache_ttl_hours

    @property
    def metrics_update_interval(self) -> int:
        """Backward compatibility: scraping.metrics_update_interval"""
        return self.scraping.metrics_update_interval

    @property
    def filter_by_status(self) -> bool:
        """Backward compatibility: scraping.filter_by_status"""
        return self.scraping.filter_by_status

    @property
    def allowed_match_statuses(self) -> tuple:
        """Backward compatibility: scraping.allowed_match_statuses"""
        return self.scraping.allowed_match_statuses

    @property
    def bronze_base_dir(self) -> str:
        """Backward compatibility: storage.bronze_path"""
        return self.storage.bronze_path

    @property
    def parquet_base_dir(self) -> str:
        """DEPRECATED: Parquet storage has been removed. Use ClickHouse instead."""
        raise DeprecationWarning(
            "Parquet storage has been removed. Use load_clickhouse.py to load data to ClickHouse."
        )

    @property
    def enable_bronze_storage(self) -> bool:
        """Backward compatibility: storage.enabled"""
        return self.storage.enabled

    @property
    def log_level(self) -> str:
        """Backward compatibility: logging.level"""
        return self.logging.level

    @property
    def log_dir(self) -> str:
        """Backward compatibility: logging.dir"""
        return self.logging.dir

    @property
    def log_format(self) -> str:
        """Backward compatibility: logging.format"""
        return self.logging.format

    @property
    def metrics_dir(self) -> str:
        """Backward compatibility: metrics.export_path"""
        return self.metrics.export_path

    @property
    def enable_metrics(self) -> bool:
        """Backward compatibility: metrics.enabled"""
        return self.metrics.enabled

    @property
    def enable_data_quality_checks(self) -> bool:
        """Backward compatibility: data_quality.enabled"""
        return self.data_quality.enabled

    @property
    def fail_on_quality_issues(self) -> bool:
        """Backward compatibility: data_quality.fail_on_issues"""
        return self.data_quality.fail_on_issues

    @property
    def max_retries(self) -> int:
        """Backward compatibility: retry.max_attempts"""
        return self.retry.max_attempts

    @property
    def retry_backoff_factor(self) -> float:
        """Backward compatibility: retry.backoff_factor"""
        return self.retry.backoff_factor

    @property
    def retry_status_codes(self) -> tuple:
        """Backward compatibility: retry.status_codes"""
        return self.retry.status_codes

    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        return self.api.get_headers()

