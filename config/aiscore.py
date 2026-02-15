"""

AIScore scraper configuration.

Configuration is loaded from:
1. config.yaml - Application settings (primary source)
2. .env file - Environment-specific & sensitive data (overrides)

"""





import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path

from .base import BaseConfig, StorageConfig, LoggingConfig, MetricsConfig, RetryConfig


@dataclass
class ScrollConfig:
    """Scrolling configuration for link scraping."""
    increment: int = 500
    pause: float = 0.3
    max_no_change: int = 8
    smart_wait_interval: float = 0.2
    smart_wait_timeout: float = 3.0


@dataclass
class TimeoutConfig:
    """Timeout configuration."""
    page_load: int = 30
    element_wait: int = 10
    cloudflare_max: int = 15
    script_timeout: int = 30


@dataclass
class NavigationConfig:
    """Navigation delay configuration."""
    homepage_load: float = 0.5
    date_page_load: float = 0.5
    tab_click: float = 0.5


@dataclass
class DelayConfig:
    """Delay configuration for odds scraping."""
    between_dates: float = 1.0
    between_matches: float = 0.5
    initial_load: float = 0.5
    after_click: float = 0.3
    tab_scroll: float = 0.3
    content_check_interval: float = 0.1
    content_fallback: float = 0.5


@dataclass
class ScrapingConfig:
    """Scraping behavior configuration."""
    base_url: str = "https://www.aiscore.com"
    filter_by_importance: bool = False
    filter_by_countries: bool = True
    filter_by_leagues: bool = False
    allowed_countries: List[str] = field(
        default_factory=lambda: [
            "England",
            "Spain",
            "Germany",
            "Italy",
            "France",
            "Portugal",
            "Netherlands",
            "Belgium",
            "Turkey",
            "Poland",
            "Austria",
            "Switzerland",
            "Scotland",
            "Denmark",
            "Sweden",
            "Norway",
            "Brazil",
            "Argentina",
            "Japan",
            "Saudi Arabia",
            "International",
            "World Cup",
            "Euro",
            "UEFA Champions League",
            "UEFA Europa League",
            "UEFA Europa Conference League",
            "Europe",
            "Africa",
            "Asia",
            "North America",
            "South America",
            "Oceania",
        ]
    )
    allowed_leagues: List[str] = field(default_factory=list)
    extract_team_names_during_link_scraping: bool = False
    scroll: ScrollConfig = field(default_factory=ScrollConfig)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    navigation: NavigationConfig = field(default_factory=NavigationConfig)
    delays: DelayConfig = field(default_factory=DelayConfig)


@dataclass
class BrowserConfig:
    """Browser configuration for Selenium."""
    headless: bool = True
    window_size: str = "1920x1080"
    block_images: bool = True
    block_css: bool = True
    block_fonts: bool = True
    block_media: bool = True
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


@dataclass
class SelectorsConfig:
    """CSS selectors configuration."""
    match_container: str = ".match-container"
    all_tab: str = ".changeTabBox .changeItem"
    match_link: str = "a[href*='/match']"


@dataclass
class ValidationConfig:
    """URL validation configuration."""
    excluded_paths: List[str] = field(default_factory=lambda: [
        "/h2h", "/statistics", "/odds", "/predictions", "/lineups"
    ])
    required_pattern: str = "/match"


class AIScoreConfig(BaseConfig):
    """
    AIScore scraper configuration.

    Configuration is loaded from:
    1. config.yaml - Application settings (primary source)
    2. .env file - Environment-specific & sensitive data (overrides)

    Usage:

        config = AIScoreConfig()

        print(config.scraping.base_url)

        print(config.storage.bronze_path)

    See config.yaml for all available configuration options.
    """








    def __init__(self):
        """Initialize AIScore configuration from YAML and environment variables."""
        self._load_config()
        self._apply_env_overrides()
        self._ensure_directories()

    def _load_config(self):
        """Initialize configuration from config.yaml with defaults as fallback."""
        # Get AISCORE config from YAML or use defaults
        yaml_aiscore = self._yaml_config.get('aiscore', {}) if hasattr(self, '_yaml_config') else {}
        
        storage_config = yaml_aiscore.get('storage', {})
        self.storage = StorageConfig(
            bronze_path=storage_config.get('bronze_path', "data/aiscore"),
            enabled=storage_config.get('enabled', True),
        )

        scraping_config = yaml_aiscore.get('scraping', {})
        scroll_config = scraping_config.get('scroll', {})
        timeouts_config = scraping_config.get('timeouts', {})
        navigation_config = scraping_config.get('navigation', {})
        delays_config = scraping_config.get('delays', {})
        
        self.scraping = ScrapingConfig(
            base_url=scraping_config.get('base_url', "https://www.aiscore.com"),
            filter_by_importance=scraping_config.get('filter_by_importance', False),
            filter_by_countries=scraping_config.get('filter_by_countries', False),
            filter_by_leagues=scraping_config.get('filter_by_leagues', True),
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
                page_load=timeouts_config.get('page_load', 30),
                element_wait=timeouts_config.get('element_wait', 10),
                cloudflare_max=timeouts_config.get('cloudflare_max', 15),
                script_timeout=timeouts_config.get('script_timeout', 30),
            ),
            navigation=NavigationConfig(
                homepage_load=navigation_config.get('homepage_load', 0.5),
                date_page_load=navigation_config.get('date_page_load', 0.5),
                tab_click=navigation_config.get('tab_click', 0.5),
            ),
            delays=DelayConfig(
                between_dates=delays_config.get('between_dates', 1.0),
                between_matches=delays_config.get('between_matches', 0.5),
                initial_load=delays_config.get('initial_load', 0.5),
                after_click=delays_config.get('after_click', 0.3),
                tab_scroll=delays_config.get('tab_scroll', 0.3),
                content_check_interval=delays_config.get('content_check_interval', 0.1),
                content_fallback=delays_config.get('content_fallback', 0.5),
            ),
        )

        browser_config = yaml_aiscore.get('browser', {})
        self.browser = BrowserConfig(
            headless=browser_config.get('headless', True),
            window_size=browser_config.get('window_size', "1920x1080"),
            block_images=browser_config.get('block_images', True),
            block_css=browser_config.get('block_css', True),
            block_fonts=browser_config.get('block_fonts', True),
            block_media=browser_config.get('block_media', True),
            user_agent=browser_config.get('user_agent', BrowserConfig.user_agent),
        )

        selectors_config = yaml_aiscore.get('selectors', {})
        self.selectors = SelectorsConfig(
            match_container=selectors_config.get('match_container', ".match-container"),
            all_tab=selectors_config.get('all_tab', ".changeTabBox .changeItem"),
            match_link=selectors_config.get('match_link', "a[href*='/match']"),
        )

        validation_config = yaml_aiscore.get('validation', {})
        self.validation = ValidationConfig(
            excluded_paths=validation_config.get('excluded_paths', ["/h2h", "/statistics", "/odds", "/predictions", "/lineups"]),
            required_pattern=validation_config.get('required_pattern', "/match"),
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
            level=aiscore_logging.get('level', "INFO"),
            format=aiscore_logging.get('format', "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            file=aiscore_logging.get('file', "logs/aiscore_scraper.log"),
            max_bytes=aiscore_logging.get('max_bytes', 10485760),
            backup_count=aiscore_logging.get('backup_count', 5),
            dir=aiscore_logging.get('dir', "logs"),
        )

        aiscore_metrics = yaml_aiscore.get('metrics', {})
        self.metrics = MetricsConfig(
            enabled=aiscore_metrics.get('enabled', True),
            export_path=aiscore_metrics.get('export_path', "metrics"),
            export_format=aiscore_metrics.get('export_format', "json"),
        )

    def _apply_env_overrides(self):
        """Load configuration from environment variables (.env file)."""
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

