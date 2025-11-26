"""
Shared utilities for scraping scripts to reduce code duplication and improve performance.

This module centralizes common operations used across scraping scripts.
"""

import sys
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime, timedelta
from calendar import monthrange

# Cache for project root to avoid repeated path operations
_PROJECT_ROOT = None


def get_project_root() -> Path:
    """
    Get project root directory (cached for performance).

    Returns:
        Path to project root directory
    """
    global _PROJECT_ROOT
    if _PROJECT_ROOT is None:
        # Assuming script is in scripts/ or scripts/*/
        current_file = Path(__file__).resolve()
        # Navigate up from scripts/utils/ to project root
        _PROJECT_ROOT = current_file.parent.parent.parent
    return _PROJECT_ROOT


def add_project_to_path():
    """Add project root to Python path if not already present."""
    project_root = get_project_root()
    project_root_str = str(project_root)

    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)


def validate_date_format(date_str: str, format_type: str = "YYYYMMDD") -> Tuple[bool, Optional[str]]:
    """
    Validate date string format.

    Args:
        date_str: Date string to validate
        format_type: Expected format ("YYYYMMDD" or "YYYYMM")

    Returns:
        Tuple of (is_valid, error_message)
    """
    if format_type == "YYYYMMDD":
        if len(date_str) != 8 or not date_str.isdigit():
            return False, f"Invalid date format: {date_str}. Expected YYYYMMDD (e.g., 20251113)"

        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])

            if not (1 <= month <= 12):
                return False, f"Invalid month: {month}. Must be between 01 and 12"

            if not (1 <= day <= 31):
                return False, f"Invalid day: {day}. Must be between 01 and 31"

            if not (2000 <= year <= 2100):
                return False, f"Invalid year: {year}. Must be between 2000 and 2100"

            # Validate actual date
            datetime(year, month, day)
            return True, None

        except ValueError as e:
            return False, f"Invalid date: {date_str}. {str(e)}"

    elif format_type == "YYYYMM":
        if len(date_str) != 6 or not date_str.isdigit():
            return False, f"Invalid month format: {date_str}. Expected YYYYMM (e.g., 202511)"

        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])

            if not (1 <= month <= 12):
                return False, f"Invalid month: {month}. Must be between 01 and 12"

            if not (2000 <= year <= 2100):
                return False, f"Invalid year: {year}. Must be between 2000 and 2100"

            return True, None

        except ValueError as e:
            return False, f"Invalid month: {date_str}. {str(e)}"

    return False, f"Unknown format type: {format_type}"


def generate_date_range(start_date: str, end_date: str) -> list[str]:
    """
    Generate list of dates between start and end (inclusive).

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Returns:
        List of date strings in YYYYMMDD format
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return dates


def generate_month_dates(month_str: str) -> list[str]:
    """
    Generate all dates in a month.

    Args:
        month_str: Month in YYYYMM format

    Returns:
        List of date strings in YYYYMMDD format
    """
    year = int(month_str[:4])
    month = int(month_str[4:6])

    _, last_day = monthrange(year, month)

    dates = []
    for day in range(1, last_day + 1):
        date_str = f"{year}{month:02d}{day:02d}"
        dates.append(date_str)

    return dates


class ImplicitWaitContext:
    """
    Context manager for temporarily changing Selenium implicit wait.

    More efficient than manually saving/restoring wait times.

    Usage:
        with ImplicitWaitContext(driver, 0):
            # Fast lookups with no wait
            elements = driver.find_elements(By.CSS_SELECTOR, ".fast")
        # Original wait time restored automatically
    """

    def __init__(self, driver, wait_seconds: float):
        """
        Initialize context manager.

        Args:
            driver: Selenium WebDriver instance
            wait_seconds: Temporary wait time in seconds
        """
        self.driver = driver
        self.new_wait = wait_seconds
        self.original_wait = None

    def __enter__(self):
        """Enter context: save current wait and set new wait."""
        # Note: Selenium doesn't provide a way to get current implicit wait
        # We assume it's set in config, but we'll try to restore it
        try:
            self.driver.implicitly_wait(self.new_wait)
        except Exception:
            pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context: restore original wait (assume default of 10s)."""
        try:
            # Restore to reasonable default (scripts should set explicit wait in config)
            self.driver.implicitly_wait(10)
        except Exception:
            pass
        return False


class PerformanceTimer:
    """
    Simple timer for performance measurement.

    Usage:
        timer = PerformanceTimer("Operation name")
        # ... do work ...
        timer.log_elapsed()  # Logs: "Operation name took 1.23s"
    """

    def __init__(self, operation_name: str, logger=None):
        """
        Initialize timer.

        Args:
            operation_name: Description of operation being timed
            logger: Optional logger (uses print if None)
        """
        self.operation_name = operation_name
        self.logger = logger
        self.start_time = datetime.now()

    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        return (datetime.now() - self.start_time).total_seconds()

    def log_elapsed(self, level="info"):
        """
        Log elapsed time.

        Args:
            level: Log level (info, debug, warning, error)
        """
        elapsed_time = self.elapsed()
        message = f"{self.operation_name} took {elapsed_time:.2f}s"

        if self.logger:
            log_func = getattr(self.logger, level, self.logger.info)
            log_func(message)
        else:
            print(message)

    def __enter__(self):
        """Support context manager usage."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-log on context exit."""
        self.log_elapsed()
        return False
