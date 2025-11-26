"""Custom exceptions for the scraper"""


class ScraperError(Exception):
    """Base exception for all scraper errors"""
    pass


class DatabaseError(ScraperError):
    """Database-related errors"""
    pass


class NetworkError(ScraperError):
    """Network-related errors"""
    pass


class ParsingError(ScraperError):
    """Page parsing errors"""
    pass


class ConfigError(ScraperError):
    """Configuration errors"""
    pass


class BrowserError(ScraperError):
    """Browser/driver errors"""
    pass


class ElementNotFoundError(ScraperError):
    """Element not found on page"""
    pass


class ValidationError(ScraperError):
    """Data validation errors"""
    pass


class CloudflareError(NetworkError):
    """Cloudflare protection error"""
    pass


class RateLimitError(NetworkError):
    """Rate limit exceeded"""
    pass

