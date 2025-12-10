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
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.propagate = False
    logger.handlers = []
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    formatter = logging.Formatter(log_format)
    if date_suffix:
        log_file = Path(log_dir) / f"{name}_{date_suffix}.log"
    else:
        timestamp = datetime.now().strftime('%Y%m%d')
        log_file = Path(log_dir) / f"{name}_{timestamp}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
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
    if not logger.handlers:
        return setup_logging(name=name)
    has_console_handler = any(
        isinstance(h, logging.StreamHandler) and h.stream == sys.stdout
        for h in logger.handlers
    )
    if not has_console_handler:
        console_handler = logging.StreamHandler(sys.stdout)
        effective_level = logger.getEffectiveLevel()
        if effective_level == logging.NOTSET:
            effective_level = logging.INFO
        console_handler.setLevel(effective_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        if logger.level == logging.NOTSET:
            logger.setLevel(effective_level)
    return logger
class LoggerAdapter(logging.LoggerAdapter):
    """
    Custom logger adapter that adds context to log messages.
    Usage:
        logger = LoggerAdapter(base_logger, {'match_id': '123456'})
        logger.info("Processing match")
    """
    def process(self, msg, kwargs):
        """Add extra context to log messages."""
        if self.extra:
            context = ' | '.join(f"{k}={v}" for k, v in self.extra.items())
            msg = f"[{context}] {msg}"
        return msg, kwargs
class JsonFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    Outputs log records as JSON for easy parsing by log aggregation tools
    (Elasticsearch, Splunk, CloudWatch, etc.).
    Example output:
        {
            "timestamp": "2025-11-26T10:30:00.123Z",
            "level": "INFO",
            "logger": "fotmob_scraper",
            "message": "Match processed successfully",
            "function": "process_match",
            "line": 45,
            "match_id": "12345",
            "duration_ms": 1234.5
        }
    """
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON string.
        Args:
            record: Log record to format
        Returns:
            JSON string representation of log record
        """
        import json
        from datetime import datetime
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat() + 'Z',
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "function": record.funcName,
            "line": record.lineno,
            "module": record.module,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            log_data["stack_info"] = self.formatStack(record.stack_info)
        for key, value in record.__dict__.items():
            if key not in ('name', 'msg', 'args', 'created', 'filename', 'funcName',
                          'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'message', 'pathname', 'process', 'processName',
                          'relativeCreated', 'thread', 'threadName', 'exc_info',
                          'exc_text', 'stack_info', 'taskName'):
                log_data[key] = value
        return json.dumps(log_data, default=str)

def setup_json_logging(
    name: str = "fotmob_scraper",
    log_dir: str = "logs",
    log_level: str = "INFO",
    date_suffix: Optional[str] = None
) -> logging.Logger:
    """
    Configure JSON structured logging for production environments.
    Creates a logger that outputs JSON-formatted logs for easy parsing
    by log aggregation and analysis tools.
    Args:
        name: Logger name
        log_dir: Directory to store log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        date_suffix: Optional date suffix for log filename
    Returns:
        Configured logger with JSON formatter
    Example:
        >>> logger = setup_json_logging("my_app", log_level="DEBUG")
        >>> logger.info("User logged in", extra={"user_id": "123", "ip": "192.168.1.1"})
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.propagate = False
    logger.handlers = []
    json_formatter = JsonFormatter()
    if date_suffix:
        log_file = Path(log_dir) / f"{name}_{date_suffix}.json"
    else:
        timestamp = datetime.now().strftime('%Y%m%d')
        log_file = Path(log_dir) / f"{name}_{timestamp}.json"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(json_formatter)
    file_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info(f"JSON logging initialized. Log file: {log_file}")
    return logger
