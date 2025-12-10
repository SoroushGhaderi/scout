"""Browser/WebDriver management."""

import logging
import os
import platform
import time
import subprocess
import re
import tempfile
from typing import Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from .config import Config
from .exceptions import BrowserError

logger = logging.getLogger(__name__)


class BrowserManager:
    """Manages browser/WebDriver lifecycle and configuration."""

    def __init__(self, config: Config):
        self.config = config
        self.driver: Optional[webdriver.Chrome] = None

    def create_driver(self) -> webdriver.Chrome:
        """Create and configure Chrome WebDriver.

        Returns:
            Configured WebDriver instance

        Raises:
            BrowserError: If driver creation fails
        """
        try:
            chrome_options = self._configure_options()

            chrome_bin = os.getenv('CHROME_BIN')

            if not chrome_bin:
                if platform.system() == 'Windows':
                    possible_paths = [
                        os.path.expandvars(r'%ProgramFiles%\Google\Chrome\Application\chrome.exe'),
                        os.path.expandvars(r'%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe'),
                        os.path.expandvars(r'%LocalAppData%\Google\Chrome\Application\chrome.exe'),
                        r'C:\Program Files\Google\Chrome\Application\chrome.exe',
                        r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe',
                    ]
                    for path in possible_paths:
                        if os.path.exists(path):
                            chrome_bin = path
                            logger.info(f"Found Chrome at: {chrome_bin}")
                            break
                elif platform.system() == 'Linux':
                    possible_paths = [
                        '/usr/bin/chromium',
                        '/usr/bin/chromium-browser',
                        '/usr/bin/google-chrome',
                        '/usr/bin/google-chrome-stable',
                    ]
                    for path in possible_paths:
                        if os.path.exists(path):
                            chrome_bin = path
                            logger.info(f"Found Chrome at: {chrome_bin}")
                            break
                elif platform.system() == 'Darwin':
                    chrome_bin = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
                    if not os.path.exists(chrome_bin):
                        chrome_bin = None

            if chrome_bin and os.path.exists(chrome_bin):
                chrome_options.binary_location = chrome_bin
                logger.info(f"Using Chrome binary: {chrome_bin}")
            else:
                logger.info("Chrome binary not specified or not found, using system default")

            # Check for pre-installed ChromeDriver first (avoid runtime downloads)
            chromedriver_path = os.getenv('CHROMEDRIVER_PATH')
            if chromedriver_path and os.path.exists(chromedriver_path):
                logger.info(f"Using pre-installed ChromeDriver at: {chromedriver_path}")
                driver_path = chromedriver_path
            else:
                # Fall back to webdriver_manager (downloads at runtime)
                logger.info("CHROMEDRIVER_PATH not set or not found, using webdriver_manager")
                try:
                    if chrome_bin and os.path.exists(chrome_bin):
                        result = subprocess.run(
                            [chrome_bin, '--version'],
                            capture_output=True,
                            text=True,
                            timeout=5
                        )
                    else:
                        for cmd in ['chrome', 'google-chrome', 'chromium', 'chromium-browser']:
                            try:
                                result = subprocess.run(
                                    [cmd, '--version'],
                                    capture_output=True,
                                    text=True,
                                    timeout=5
                                )
                                break
                            except FileNotFoundError:
                                continue
                        else:
                            raise FileNotFoundError("Chrome command not found")

                    version_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', result.stdout or result.stderr)
                    if version_match:
                        chrome_full_version = version_match.group(1)
                        chrome_major = int(chrome_full_version.split('.')[0])
                        logger.info(f"Detected Chrome version: {chrome_full_version} (major: {chrome_major})")

                        if chrome_major >= 115:
                            driver_manager = ChromeDriverManager(driver_version=chrome_full_version)
                        else:
                            driver_manager = ChromeDriverManager()
                    else:
                        logger.warning("Could not detect Chrome version, using default ChromeDriverManager")
                        driver_manager = ChromeDriverManager()
                except Exception as e:
                    logger.warning(f"Version detection failed: {e}, using default ChromeDriverManager")
                    driver_manager = ChromeDriverManager()

                driver_path = driver_manager.install()
            
            service = Service(driver_path)

            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            self.driver.set_page_load_timeout(self.config.scraping.timeouts.page_load)
            self.driver.implicitly_wait(self.config.scraping.timeouts.element_wait)

            script_timeout = getattr(self.config.scraping.timeouts, 'script_timeout', 30)
            self.driver.set_script_timeout(script_timeout)

            self._inject_anti_detection()

            logger.info("Browser created successfully")
            return self.driver

        except Exception as e:
            raise BrowserError(f"Failed to create browser: {e}")

    def _configure_options(self) -> Options:
        """Configure Chrome options for optimal performance."""
        chrome_options = Options()

        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        width, height = self.config.browser.window_size.split('x')
        chrome_options.add_argument(f"--window-size={width},{height}")

        chrome_options.page_load_strategy = 'eager'

        cache_dir = os.path.join(tempfile.gettempdir(), 'chrome-cache')

        prefs = {
            "profile.managed_default_content_settings.images": 2 if self.config.browser.block_images else 1,
            "profile.default_content_setting_values.notifications": 2,
            "profile.managed_default_content_settings.stylesheets": 2 if self.config.browser.block_css else 1,
            "profile.managed_default_content_settings.cookies": 1,
            "profile.managed_default_content_settings.javascript": 1,
            "profile.managed_default_content_settings.plugins": 2,
            "profile.managed_default_content_settings.popups": 2,
            "profile.managed_default_content_settings.geolocation": 2,
            "profile.managed_default_content_settings.media_stream": 2,
            "disk-cache-size": 4096,
            "disk-cache-dir": cache_dir
        }
        chrome_options.add_experimental_option("prefs", prefs)

        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-dev-tools")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-breakpad")
        chrome_options.add_argument("--disable-component-extensions-with-background-pages")
        chrome_options.add_argument("--disable-features=TranslateUI,BlinkGenPropertyTrees")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--enable-features=NetworkService,NetworkServiceInProcess")
        chrome_options.add_argument("--force-color-profile=srgb")
        chrome_options.add_argument("--metrics-recording-only")
        chrome_options.add_argument("--mute-audio")

        if self.config.browser.block_images:
            chrome_options.add_argument("--blink-settings=imagesEnabled=false")

        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        chrome_options.add_argument(f"user-agent={self.config.browser.user_agent}")

        if self.config.browser.headless:
            chrome_options.add_argument("--headless=new")

        return chrome_options

    def _inject_anti_detection(self):
        """Inject anti-detection JavaScript."""
        try:
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })
            logger.debug("Anti-detection script injected")
        except Exception as e:
            logger.warning(f"Could not inject anti-detection script: {e}")

    def wait_for_element(
        self,
        selector: str,
        by: By = By.CSS_SELECTOR,
        timeout: Optional[int] = None
    ) -> bool:
        """Wait for element to be present.

        Args:
            selector: Element selector
            by: Selector type (default: CSS_SELECTOR)
            timeout: Optional timeout override

        Returns:
            True if element found, False otherwise
        """
        if timeout is None:
            timeout = self.config.scraping.timeouts.element_wait

        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return True
        except Exception as e:
            logger.debug(f"Element {selector} not found: {e}")
            return False

    def scroll_page(self, pixels: int):
        """Scroll page by specified pixels."""
        try:
            self.driver.execute_script(f"window.scrollBy(0, {pixels});")
        except Exception as e:
            logger.warning(f"Failed to scroll: {e}")

    def get_scroll_position(self) -> int:
        """Get current scroll position."""
        try:
            return self.driver.execute_script("return window.pageYOffset;")
        except:
            return 0

    def is_at_bottom(self) -> bool:
        """Check if page is scrolled to bottom."""
        try:
            return self.driver.execute_script(
                "return (window.innerHeight + window.pageYOffset) >= document.body.scrollHeight;"
            )
        except:
            return False

    def close(self):
        """Close browser and cleanup."""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser closed")
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
            finally:
                self.driver = None

    def is_healthy(self) -> bool:
        """Check if browser is still responsive.

        Returns:
            True if browser responds to commands, False otherwise
        """
        if not self.driver:
            return False

        try:
            _ = self.driver.title
            return True
        except Exception as e:
            logger.warning(f"Browser health check failed: {e}")
            return False

    def restart(self):
        """Restart the browser (useful after timeouts/crashes)."""
        logger.info("Restarting browser...")
        self.close()
        time.sleep(2)
        self.create_driver()
        logger.info("Browser restarted successfully")

    def __enter__(self):
        """Context manager enter."""
        self.create_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
