"""Logging utilities for Scout project with structlog as the global backend."""

import logging
import sys
from pathlib import Path
from typing import Optional

import structlog


_STRUCTLOG_CONFIGURED = False
_LOGGING_BOOTSTRAPPED = False
_DEFAULT_LOGGER_NAME = "scout"


def _normalize_log_level(log_level: str) -> int:
    """Convert string level to stdlib logging constant."""
    return getattr(logging, log_level.upper(), logging.INFO)


def _configure_structlog() -> None:
    """Configure structlog once for stdlib integration."""
    global _STRUCTLOG_CONFIGURED
    if _STRUCTLOG_CONFIGURED:
        return

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.ExtraAdder(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    _STRUCTLOG_CONFIGURED = True


def _shared_processors() -> list:
    """Shared pre-chain processors for non-structlog log records."""
    return [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.ExtraAdder(),
        structlog.processors.TimeStamper(fmt="iso"),
    ]


def _build_console_formatter() -> structlog.stdlib.ProcessorFormatter:
    """Create structlog-backed console formatter."""
    return structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=False),
        foreign_pre_chain=_shared_processors(),
    )


def _build_json_formatter() -> structlog.stdlib.ProcessorFormatter:
    """Create structlog-backed JSON formatter."""
    return structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=_shared_processors(),
    )


def initialize_logging(
    log_level: str = "INFO",
    json_output: bool = True,
    force: bool = False,
) -> logging.Logger:
    """Initialize root logging so all project loggers use structlog."""
    global _LOGGING_BOOTSTRAPPED

    _configure_structlog()
    root_logger = logging.getLogger()
    level = _normalize_log_level(log_level)

    if _LOGGING_BOOTSTRAPPED and not force:
        root_logger.setLevel(level)
        return root_logger

    if force:
        root_logger.handlers.clear()

    if not root_logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        formatter = _build_json_formatter() if json_output else _build_console_formatter()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    root_logger.setLevel(level)
    _LOGGING_BOOTSTRAPPED = True
    return root_logger


def setup_logging(
    name: str = _DEFAULT_LOGGER_NAME,
    log_dir: str = "logs",
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    date_suffix: Optional[str] = None,
) -> logging.Logger:
    """Configure logger with minimal, consistent JSON structure for all handlers."""
    initialize_logging(log_level=log_level)

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    level = _normalize_log_level(log_level)
    logger.setLevel(level)
    logger.propagate = True

    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)

    # Keep log file path fixed/predictable for each logger.
    # date_suffix/log_format are kept for backwards-compatible function signature.
    _ = date_suffix
    _ = log_format
    log_file = Path(log_dir) / f"{name}.log"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(_build_json_formatter())
    logger.addHandler(file_handler)

    logger.info("Logging initialized. Log file: %s", log_file)
    return logger


def get_logger(name: str = _DEFAULT_LOGGER_NAME) -> logging.Logger:
    """Get a logger after ensuring global structlog bootstrap is active."""
    initialize_logging()
    return logging.getLogger(name)


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
            context = " | ".join(f"{k}={v}" for k, v in self.extra.items())
            msg = f"[{context}] {msg}"
        return msg, kwargs


def setup_json_logging(
    name: str = _DEFAULT_LOGGER_NAME,
    log_dir: str = "logs",
    log_level: str = "INFO",
    date_suffix: Optional[str] = None,
) -> logging.Logger:
    """
    Configure JSON structured logging for production environments.
    Creates a logger that outputs JSON-formatted logs for easy parsing
    by log aggregation and analysis tools.
    Args:
        name: Logger name
        log_dir: Directory to store log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        date_suffix: Deprecated. Kept only for backwards compatibility.
    Returns:
        Configured logger with JSON formatter
    Example:
        >>> logger = setup_json_logging("my_app", log_level="DEBUG")
        >>> logger.info("User logged in", extra={"user_id": "123", "ip": "192.168.1.1"})
    """
    initialize_logging(log_level=log_level, json_output=True)
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(_normalize_log_level(log_level))
    logger.propagate = True

    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)

    json_formatter = _build_json_formatter()
    _ = date_suffix
    log_file = Path(log_dir) / f"{name}.json"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(json_formatter)
    file_handler.setLevel(_normalize_log_level(log_level))
    logger.addHandler(file_handler)
    logger.info("JSON logging initialized. Log file: %s", log_file)
    return logger
