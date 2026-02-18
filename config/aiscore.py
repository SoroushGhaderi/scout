"""

AIScore scraper configuration.

Configuration is loaded from:
1. config.yaml - Application settings (required)
2. .env file - Sensitive data overrides (tokens, secrets)

All non-sensitive settings must be defined in config.yaml.

"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path

from .base import BaseConfig, StorageConfig, LoggingConfig, MetricsConfig, RetryConfig


@dataclass
class ScrollConfig:
    """Scrolling configuration for link scraping."""
    increment: int
    pause: float
    max_no_change: int
    smart_wait_interval: float
    smart_wait_timeout: float


@dataclass
class TimeoutConfig:
    """Timeout configuration."""
    page_load: int
    element_wait: int
    cloudflare_max: int
    script_timeout: int


@dataclass
class NavigationConfig:
    """Navigation delay configuration."""
    homepage_load: float
    date_page_load: float
    tab_click: float


@dataclass
class DelayConfig:
    """Delay configuration for odds scraping."""
    between_dates: float
    between_matches: float
    initial_load: float
    after_click: float
    tab_scroll: float
    content_check_interval: float
    content_fallback: float


@dataclass
class ScrapingConfig:
    """Scraping behavior configuration."""
    base_url: str
    filter_by_importance: bool
    filter_by_countries: bool
    filter_by_leagues: bool
    allowed_countries: List[str]
    allowed_leagues: List[str]
    extract_team_names_during_link_scraping: bool
    scroll: ScrollConfig
    timeouts: TimeoutConfig
    navigation: NavigationConfig
    delays: DelayConfig


@dataclass
class BrowserConfig:
    """Browser configuration for Selenium."""
    headless: bool
    window_size: str
    block_images: bool
    block_css: bool
    block_fonts: bool
    block_media: bool
    user_agent: str


@dataclass
class SelectorsConfig:
    """CSS selectors configuration."""
    match_container: str
    all_tab: str
    match_link: str


@dataclass
class ValidationConfig:
    """URL validation configuration."""
    excluded_paths: List[str]
    required_pattern: str


class AIScoreConfig(BaseConfig):
    """

    AIScore scraper configuration.

    Configuration is loaded from config.yaml (required) with .env overrides.

    Usage:

        config = AIScoreConfig()

        print(config.scraping.base_url)

        print(config.storage.bronze_path)

    See config.yaml for all available configuration options.

    """

    def __init__(self):
        """Initialize AIScore configuration from config.yaml."""
        self._yaml_config = self._load_yaml_config(required_keys=['aiscore'])
        self._load_config()
        self._apply_env_overrides()
        self._ensure_directories()

    def _load_config(self):
        """Initialize configuration from config.yaml (no defaults)."""
        yaml_aiscore = self._yaml_config.get('aiscore', {})
        
        storage_config = yaml_aiscore.get('storage', {})
        if not storage_config.get('bronze_path'):
            raise ValueError("aiscore.storage.bronze_path is required in config.yaml")
            
        self.storage = StorageConfig(
            bronze_path=storage_config['bronze_path'],
            enabled=storage_config.get('enabled', True),
        )

        scraping_config = yaml_aiscore.get('scraping', {})
        if not scraping_config.get('base_url'):
            raise ValueError("aiscore.scraping.base_url is required in config.yaml")
            
        scroll_config = scraping_config.get('scroll', {})
        self.scraping = ScrapingConfig(
            base_url=scraping_config['base_url'],
            filter_by_importance=scraping_config.get('filter_by_importance', False),
            filter_by_countries=scraping_config.get('filter_by_countries', False),
            filter_by_leagues=scraping_config.get('filter_by_leagues', False),
            allowed_countries=scraping_config.get('allowed_countries', []),
            allowed_leagues=scraping_config.get('allowed_leagues', []),
            extract_team_names_during_link_scraping=scraping_config.get('extract_team_names_during_link_scraping', False),
            scroll=ScrollConfig(
                increment=scroll_config.get('increment', 500),
                pause=scroll_config.get('pause', 0.3),
                max_no_change=scroll_config.get('max_no_change', 8),
                smart_wait_interval=scroll_config.get('smart_wait_interval', 0.2),
                smart_wait_timeout=scroll_config.get('smart_wait_timeout', 3.0),
            ),
            timeouts=TimeoutConfig(
                page_load=scraping_config.get('page_load', 30),
                element_wait=scraping_config.get('element_wait', 10),
                cloudflare_max=scraping_config.get('cloudflare_max', 15),
                script_timeout=scraping_config.get('script_timeout', 30),
            ),
            navigation=NavigationConfig(
                homepage_load=scraping_config.get('homepage_load', 0.5),
                date_page_load=scraping_config.get('date_page_load', 0.5),
                tab_click=scraping_config.get('tab_click', 0.5),
            ),
            delays=DelayConfig(
                between_dates=scraping_config.get('between_dates', 1.0),
                between_matches=scraping_config.get('between_matches', 0.5),
                initial_load=scraping_config.get('initial_load', 0.5),
                after_click=scraping_config.get('after_click', 0.3),
                tab_scroll=scraping_config.get('tab_scroll', 0.3),
                content_check_interval=scraping_config.get('content_check_interval', 0.1),
                content_fallback=scraping_config.get('content_fallback', 0.5),
            ),
        )

        browser_config = yaml_aiscore.get('browser', {})
        self.browser = BrowserConfig(
            headless=browser_config.get('headless', True),
            window_size=browser_config.get('window_size', '1920x1080'),
            block_images=browser_config.get('block_images', True),
            block_css=browser_config.get('block_css', True),
            block_fonts=browser_config.get('block_fonts', True),
            block_media=browser_config.get('block_media', True),
            user_agent=browser_config.get('user_agent', ''),
        )

        selectors_config = yaml_aiscore.get('selectors', {})
        self.selectors = SelectorsConfig(
            match_container=selectors_config.get('match_container', '.match-container'),
            all_tab=selectors_config.get('all_tab', '.changeTabBox .changeItem'),
            match_link=selectors_config.get('match_link', "a[href*='/match']"),
        )

        validation_config = yaml_aiscore.get('validation', {})
        self.validation = ValidationConfig(
            excluded_paths=validation_config.get('excluded_paths', ['/h2h', '/statistics', '/odds', '/predictions', '/lineups']),
            required_pattern=validation_config.get('required_pattern', '/match'),
        )

        retry_config = yaml_aiscore.get('retry', {})
        self.retry = RetryConfig(
            max_attempts=retry_config.get('max_attempts', 3),
            initial_wait=retry_config.get('initial_wait', 2.0),
            max_wait=retry_config.get('max_wait', 10.0),
            exponential_base=retry_config.get('exponential_base', 2.0),
            backoff_factor=retry_config.get('backoff_factor', 2.0),
            status_codes=tuple(retry_config.get('status_codes', [429, 500, 502, 503, 504])),
        )

        aiscore_logging = yaml_aiscore.get('logging', {})
        self.logging = LoggingConfig(
            level=aiscore_logging.get('level', 'INFO'),
            format=aiscore_logging.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            file=aiscore_logging.get('file', 'logs/aiscore_scraper.log'),
            max_bytes=aiscore_logging.get('max_bytes', 10485760),
            backup_count=aiscore_logging.get('backup_count', 5),
            dir=aiscore_logging.get('dir', 'logs'),
        )

        aiscore_metrics = yaml_aiscore.get('metrics', {})
        self.metrics = MetricsConfig(
            enabled=aiscore_metrics.get('enabled', True),
            export_path=aiscore_metrics.get('export_path', 'metrics'),
            export_format=aiscore_metrics.get('export_format', 'json'),
        )

    def _apply_env_overrides(self):
        """Apply environment variable overrides for sensitive data."""
        super()._apply_env_overrides()

        if os.getenv('AISCORE_BRONZE_PATH'):
            self.storage.bronze_path = os.getenv('AISCORE_BRONZE_PATH')
        if os.getenv('AISCORE_STORAGE_ENABLED'):
            self.storage.enabled = os.getenv('AISCORE_STORAGE_ENABLED').lower() == 'true'

        if os.getenv('AISCORE_BASE_URL'):
            self.scraping.base_url = os.getenv('AISCORE_BASE_URL')
        if os.getenv('AISCORE_FILTER_BY_IMPORTANCE'):
            self.scraping.filter_by_importance = os.getenv('AISCORE_FILTER_BY_IMPORTANCE').lower() == 'true'
        if os.getenv('AISCORE_FILTER_BY_COUNTRIES'):
            self.scraping.filter_by_countries = os.getenv('AISCORE_FILTER_BY_COUNTRIES').lower() == 'true'
        if os.getenv('AISCORE_FILTER_BY_LEAGUES'):
            self.scraping.filter_by_leagues = os.getenv('AISCORE_FILTER_BY_LEAGUES').lower() == 'true'
        if os.getenv('AISCORE_ALLOWED_COUNTRIES'):
            countries = [c.strip() for c in os.getenv('AISCORE_ALLOWED_COUNTRIES').split(',')]
            self.scraping.allowed_countries = countries
        if os.getenv('AISCORE_ALLOWED_LEAGUES'):
            leagues = [
                league.strip()
                for league in os.getenv('AISCORE_ALLOWED_LEAGUES').split(',')
                if league.strip()
            ]
            self.scraping.allowed_leagues = leagues
        if os.getenv('AISCORE_EXTRACT_TEAM_NAMES_DURING_LINK_SCRAPING'):
            env_val = os.getenv('AISCORE_EXTRACT_TEAM_NAMES_DURING_LINK_SCRAPING')
            self.scraping.extract_team_names_during_link_scraping = (
                env_val.lower() == 'true'
            )

        if os.getenv('AISCORE_SCROLL_INCREMENT'):
            self.scraping.scroll.increment = int(os.getenv('AISCORE_SCROLL_INCREMENT'))
        if os.getenv('AISCORE_SCROLL_PAUSE'):
            self.scraping.scroll.pause = float(os.getenv('AISCORE_SCROLL_PAUSE'))
        if os.getenv('AISCORE_SCROLL_MAX_NO_CHANGE'):
            self.scraping.scroll.max_no_change = int(os.getenv('AISCORE_SCROLL_MAX_NO_CHANGE'))

        if os.getenv('AISCORE_TIMEOUT_PAGE_LOAD'):
            self.scraping.timeouts.page_load = int(os.getenv('AISCORE_TIMEOUT_PAGE_LOAD'))
        if os.getenv('AISCORE_TIMEOUT_ELEMENT_WAIT'):
            self.scraping.timeouts.element_wait = int(os.getenv('AISCORE_TIMEOUT_ELEMENT_WAIT'))
        if os.getenv('AISCORE_TIMEOUT_CLOUDFLARE_MAX'):
            self.scraping.timeouts.cloudflare_max = int(os.getenv('AISCORE_TIMEOUT_CLOUDFLARE_MAX'))
        if os.getenv('AISCORE_TIMEOUT_SCRIPT'):
            self.scraping.timeouts.script_timeout = int(os.getenv('AISCORE_TIMEOUT_SCRIPT'))

        if os.getenv('AISCORE_NAV_HOMEPAGE_LOAD'):
            self.scraping.navigation.homepage_load = float(os.getenv('AISCORE_NAV_HOMEPAGE_LOAD'))
        if os.getenv('AISCORE_NAV_DATE_PAGE_LOAD'):
            self.scraping.navigation.date_page_load = float(os.getenv('AISCORE_NAV_DATE_PAGE_LOAD'))
        if os.getenv('AISCORE_NAV_TAB_CLICK'):
            self.scraping.navigation.tab_click = float(os.getenv('AISCORE_NAV_TAB_CLICK'))

        if os.getenv('AISCORE_DELAY_BETWEEN_DATES'):
            self.scraping.delays.between_dates = float(os.getenv('AISCORE_DELAY_BETWEEN_DATES'))
        if os.getenv('AISCORE_DELAY_BETWEEN_MATCHES'):
            self.scraping.delays.between_matches = float(os.getenv('AISCORE_DELAY_BETWEEN_MATCHES'))
        if os.getenv('AISCORE_DELAY_INITIAL_LOAD'):
            self.scraping.delays.initial_load = float(os.getenv('AISCORE_DELAY_INITIAL_LOAD'))
        if os.getenv('AISCORE_DELAY_AFTER_CLICK'):
            self.scraping.delays.after_click = float(os.getenv('AISCORE_DELAY_AFTER_CLICK'))

        if os.getenv('AISCORE_HEADLESS'):
            self.browser.headless = os.getenv('AISCORE_HEADLESS').lower() == 'true'
        if os.getenv('AISCORE_BROWSER_WINDOW_SIZE'):
            self.browser.window_size = os.getenv('AISCORE_BROWSER_WINDOW_SIZE')
        if os.getenv('AISCORE_BROWSER_BLOCK_IMAGES'):
            self.browser.block_images = os.getenv('AISCORE_BROWSER_BLOCK_IMAGES').lower() == 'true'
        if os.getenv('AISCORE_BROWSER_BLOCK_CSS'):
            self.browser.block_css = os.getenv('AISCORE_BROWSER_BLOCK_CSS').lower() == 'true'
        if os.getenv('AISCORE_BROWSER_BLOCK_FONTS'):
            self.browser.block_fonts = os.getenv('AISCORE_BROWSER_BLOCK_FONTS').lower() == 'true'
        if os.getenv('AISCORE_BROWSER_BLOCK_MEDIA'):
            self.browser.block_media = os.getenv('AISCORE_BROWSER_BLOCK_MEDIA').lower() == 'true'
        if os.getenv('AISCORE_BROWSER_USER_AGENT'):
            self.browser.user_agent = os.getenv('AISCORE_BROWSER_USER_AGENT')

        if os.getenv('AISCORE_SELECTOR_MATCH_CONTAINER'):
            self.selectors.match_container = os.getenv('AISCORE_SELECTOR_MATCH_CONTAINER')
        if os.getenv('AISCORE_SELECTOR_ALL_TAB'):
            self.selectors.all_tab = os.getenv('AISCORE_SELECTOR_ALL_TAB')
        if os.getenv('AISCORE_SELECTOR_MATCH_LINK'):
            self.selectors.match_link = os.getenv('AISCORE_SELECTOR_MATCH_LINK')

        if os.getenv('AISCORE_VALIDATION_EXCLUDED_PATHS'):
            paths = [p.strip() for p in os.getenv('AISCORE_VALIDATION_EXCLUDED_PATHS').split(',')]
            self.validation.excluded_paths = paths
        if os.getenv('AISCORE_VALIDATION_REQUIRED_PATTERN'):
            self.validation.required_pattern = os.getenv('AISCORE_VALIDATION_REQUIRED_PATTERN')

        if os.getenv('AISCORE_RETRY_MAX_ATTEMPTS'):
            self.retry.max_attempts = int(os.getenv('AISCORE_RETRY_MAX_ATTEMPTS'))
        if os.getenv('AISCORE_RETRY_INITIAL_WAIT'):
            self.retry.initial_wait = float(os.getenv('AISCORE_RETRY_INITIAL_WAIT'))
        if os.getenv('AISCORE_RETRY_MAX_WAIT'):
            self.retry.max_wait = float(os.getenv('AISCORE_RETRY_MAX_WAIT'))

    @property
    def bronze_layer(self):
        """Backward compatibility: access storage as bronze_layer."""
        class BronzeLayerCompat:
            def __init__(self, storage):
                self.enabled = storage.enabled
                self.path = storage.bronze_path
        return BronzeLayerCompat(self.storage)

    @property
    def database(self):
        """Backward compatibility: database config (not used but kept for compatibility)."""
        class DatabaseCompat:
            path = "data/football_matches.db"
            batch_size = 100
            connection_timeout = 30
        return DatabaseCompat()

    def ensure_directories(self):
        """Ensure all required directories exist."""
        super().ensure_directories()
