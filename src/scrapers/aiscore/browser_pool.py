"""
Browser Pool - Reusable browser instances for faster scraping

Maintains a pool of warm browser instances to eliminate startup overhead.
"""

import logging
import time
from queue import Queue, Empty
from threading import Lock
from typing import Optional

from .browser import BrowserManager
from .config import Config

logger = logging.getLogger(__name__)


class BrowserPool:
    """Pool of reusable browser instances"""
    
    def __init__(self, config: Config, size: int = 1):
        """Initialize browser pool.
        
        Args:
            config: Configuration instance
            size: Number of browsers in pool (default: 1 for single-threaded)
        """
        self.config = config
        self.size = size
        self.available = Queue(maxsize=size)
        self.all_browsers = []
        self.lock = Lock()
        self._is_initialized = False
    
    def initialize(self):
        """Pre-create all browsers in the pool (warm start)"""
        if self._is_initialized:
            return
        
        start_time = time.time()
        
        for i in range(self.size):
            try:
                browser = BrowserManager(self.config)
                browser.create_driver()
                self.all_browsers.append(browser)
                self.available.put(browser)
            except Exception as e:
                logger.error(f"Browser {i+1} failed: {e}")
        
        duration = time.time() - start_time
        logger.info(f"[POOL] Ready: {len(self.all_browsers)} browser(s) in {duration:.1f}s")
        self._is_initialized = True
    
    def acquire(self, timeout: int = 30) -> Optional[BrowserManager]:
        """Get a browser from the pool.
        
        Args:
            timeout: Max seconds to wait for available browser
            
        Returns:
            BrowserManager instance or None if timeout
        """
        if not self._is_initialized:
            self.initialize()
        
        try:
            browser = self.available.get(timeout=timeout)
            return browser
        except Empty:
            logger.error(f"No browser available (timeout: {timeout}s)")
            return None
    
    def release(self, browser: BrowserManager):
        """Return browser to pool (makes it available for reuse).
        
        Args:
            browser: Browser to return to pool
        """
        if browser not in self.all_browsers:
            return
        
        try:
            self._reset_browser(browser)
            self.available.put(browser)
        except Exception as e:
            logger.error(f"Release failed: {e}")
    
    def _reset_browser(self, browser: BrowserManager):
        """Reset browser state between uses.
        
        Args:
            browser: Browser to reset
        """
        try:
            browser.driver.delete_all_cookies()
            browser.driver.execute_script("window.localStorage.clear();")
            browser.driver.execute_script("window.sessionStorage.clear();")
        except Exception as e:
            # Non-critical, continue anyway
            pass
    
    def cleanup(self):
        """Close all browsers in the pool"""
        for browser in self.all_browsers:
            try:
                browser.close()
            except Exception as e:
                logger.error(f"Close error: {e}")
        
        self.all_browsers.clear()
        
        # Clear queue
        while not self.available.empty():
            try:
                self.available.get_nowait()
            except Empty:
                break
        
        self._is_initialized = False
    
    def __enter__(self):
        """Context manager entry"""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.cleanup()
    
    def get_stats(self) -> dict:
        """Get pool statistics.
        
        Returns:
            Dictionary with pool stats
        """
        return {
            'size': self.size,
            'initialized': self._is_initialized,
            'total_browsers': len(self.all_browsers),
            'available': self.available.qsize(),
            'in_use': len(self.all_browsers) - self.available.qsize()
        }

