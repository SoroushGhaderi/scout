"""Logging utilities for FotMob scraper."""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


def setup_logging(
    name: str = "fotmob_scraper",
    log_dir: str = "logs",
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    date_suffix: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging to both file and console with date-specific log file.
    
    Args:
        name: Logger name
        log_dir: Directory to store log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        date_suffix: Optional date suffix for log filename (e.g., '20250101')
    
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Ensure logger propagates to root logger (for console output)
    logger.propagate = False  # We handle handlers ourselves
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Create formatters
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    formatter = logging.Formatter(log_format)
    
    # Create file handler
    if date_suffix:
        log_file = Path(log_dir) / f"{name}_{date_suffix}.log"
    else:
        timestamp = datetime.now().strftime('%Y%m%d')
        log_file = Path(log_dir) / f"{name}_{timestamp}.log"
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Create console handler - use sys.stdout directly for immediate output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Also ensure root logger has a console handler if it doesn't
    root_logger = logging.getLogger()
    root_has_console = any(
        isinstance(h, logging.StreamHandler) and h.stream == sys.stdout
        for h in root_logger.handlers
    )
    if not root_has_console:
        root_console = logging.StreamHandler(sys.stdout)
        root_console.setLevel(getattr(logging, log_level.upper()))
        root_console.setFormatter(formatter)
        root_logger.addHandler(root_console)
        root_logger.setLevel(getattr(logging, log_level.upper()))
    
    logger.info(f"Logging initialized. Log file: {log_file}")
    
    return logger


def get_logger(name: str = "fotmob_scraper") -> logging.Logger:
    """
    Get an existing logger or create a new one.
    Ensures console handler is always present for terminal output.
    
    Args:
        name: Logger name
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # If logger has no handlers, set it up with defaults
    if not logger.handlers:
        return setup_logging(name=name)
    
    # Check if console handler exists - if not, add one
    has_console_handler = any(
        isinstance(h, logging.StreamHandler) and h.stream == sys.stdout
        for h in logger.handlers
    )
    
    if not has_console_handler:
        # Add console handler if missing
        console_handler = logging.StreamHandler(sys.stdout)
        # Use effective level (checks parent loggers if level is NOTSET)
        effective_level = logger.getEffectiveLevel()
        if effective_level == logging.NOTSET:
            effective_level = logging.INFO  # Default to INFO if not set
        console_handler.setLevel(effective_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Ensure logger level is set if it was NOTSET
        if logger.level == logging.NOTSET:
            logger.setLevel(effective_level)
    
    return logger


class LoggerAdapter(logging.LoggerAdapter):
    """
    Custom logger adapter that adds context to log messages.
    
    Usage:
        logger = LoggerAdapter(base_logger, {'match_id': '123456'})
        logger.info("Processing match")  # Will include match_id in the log
    """
    
    def process(self, msg, kwargs):
        """Add extra context to log messages."""
        if self.extra:
            context = ' | '.join(f"{k}={v}" for k, v in self.extra.items())
            msg = f"[{context}] {msg}"
        return msg, kwargs

