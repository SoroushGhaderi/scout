"""
Daily metrics reporting via Telegram with emoji indicators.

Sends end-of-day summaries for FotMob and AIScore scraping with metrics and status.

Usage:
    from src.utils.metrics_alerts import send_daily_report
    
    send_daily_report(
        scraper='fotmob',
        matches_scraped=150,
        errors=2,
        skipped=5,
        duration_seconds=3600,
        start_time=datetime.now()
    )
"""

import os
from datetime import datetime
from typing import Optional, Dict, Any

try:
    import requests
except ImportError:
    requests = None

from .logging_utils import get_logger


# Emoji indicators for different metrics
EMOJI_MAP = {
    # Status indicators
    'success': '✅',
    'error': '❌',
    'warning': '⚠️',
    'info': 'ℹ️',
    
    # Scraping metrics
    'matches': '⚽',
    'matches_scraped': '✨',
    'errors': '❌',
    'skipped': '⏭️',
    'data_quality_issues': '📊',
    'duration': '⏱️',
    'start_time': '🕐',
    'end_time': '🕐',
    'success_rate': '📈',
    
    # FotMob specific
    'teams': '🏆',
    'players': '👥',
    'shots': '🎯',
    'events': '📝',
    'stats': '📊',
    
    # AIScore specific
    'bookmarks': '🔖',
    'odds': '💰',
    'betting_data': '🎲',
    'links_scraped': '🔗',
    
    # General
    'database': '🗄️',
    'storage': '💾',
    'network': '🌐',
    'retry': '🔄',
    'timeout': '⏲️',
    'progress': '📊',
    'cache': '🚀',
    'cache_hit': '💨',
}


class TelegramMetricsReporter:
    """Reports daily metrics to Telegram."""

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None
    ):
        """
        Initialize Telegram metrics reporter.
        
        Args:
            bot_token: Telegram bot token (from TELEGRAM_BOT_TOKEN env var if not provided)
            chat_id: Telegram chat ID (from TELEGRAM_CHAT_ID env var if not provided)
        """
        self.bot_token = bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = chat_id or os.getenv('TELEGRAM_CHAT_ID')
        self.logger = get_logger()
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}" if self.bot_token else None

    def _send_message(self, text: str) -> bool:
        """Send a message to Telegram."""
        if not requests:
            self.logger.warning("requests library not available for Telegram")
            return False

        if not self.bot_token or not self.chat_id:
            self.logger.warning("Telegram not configured (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing)")
            return False

        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML"
            }
            
            response = requests.post(f"{self.api_url}/sendMessage", json=payload, timeout=10)
            
            if response.status_code == 200:
                self.logger.info("Telegram message sent successfully")
                return True
            else:
                self.logger.error(f"Telegram API error ({response.status_code}): {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to send Telegram message: {e}")
            return False

    def report_fotmob_daily(
        self,
        date: str,
        matches_scraped: int,
        errors: int = 0,
        skipped: int = 0,
        duration_seconds: float = 0,
        cache_hits: int = 0,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send FotMob daily scraping report.
        
        Args:
            date: Date being scraped (YYYYMMDD format)
            matches_scraped: Number of matches successfully scraped
            errors: Number of errors during scraping
            skipped: Number of matches skipped
            duration_seconds: Total duration in seconds
            cache_hits: Number of cache hits
            context: Additional context data
            
        Returns:
            True if message sent successfully
        """
        # Calculate metrics
        total_attempted = matches_scraped + errors + skipped
        success_rate = (matches_scraped / total_attempted * 100) if total_attempted > 0 else 0
        duration_str = self._format_duration(duration_seconds)

        # Build message
        message = f"<b>{EMOJI_MAP['matches']} FotMob Daily Report - {date}</b>\n\n"
        message += f"{EMOJI_MAP['matches_scraped']} <b>Matches Scraped:</b> {matches_scraped}\n"
        
        if success_rate >= 95:
            message += f"{EMOJI_MAP['success_rate']} <b>Success Rate:</b> {success_rate:.1f}% {EMOJI_MAP['success']}\n"
        else:
            message += f"{EMOJI_MAP['success_rate']} <b>Success Rate:</b> {success_rate:.1f}% {EMOJI_MAP['warning']}\n"
        
        if errors > 0:
            message += f"{EMOJI_MAP['errors']} <b>Errors:</b> {errors}\n"
        
        if skipped > 0:
            message += f"{EMOJI_MAP['skipped']} <b>Skipped:</b> {skipped}\n"
        
        message += f"{EMOJI_MAP['duration']} <b>Duration:</b> {duration_str}\n"
        
        if cache_hits > 0:
            message += f"{EMOJI_MAP['cache_hit']} <b>Cache Hits:</b> {cache_hits}\n"

        # Add context if provided
        if context:
            message += f"\n<b>Additional Details:</b>\n"
            for key, value in context.items():
                emoji = EMOJI_MAP.get(key, '•')
                message += f"{emoji} <b>{key}:</b> {value}\n"

        # Final status
        if errors == 0 and skipped == 0:
            message += f"\n{EMOJI_MAP['success']}<b> All matches scraped successfully!</b>"
        else:
            message += f"\n{EMOJI_MAP['info']} Status: Completed with issues"

        return self._send_message(message)

    def report_aiscore_daily(
        self,
        date: str,
        matches_scraped: int,
        odds_scraped: int = 0,
        errors: int = 0,
        skipped: int = 0,
        duration_seconds: float = 0,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send AIScore daily scraping report.
        
        Args:
            date: Date being scraped (YYYYMMDD format)
            matches_scraped: Number of matches found
            odds_scraped: Number of odds successfully scraped
            errors: Number of errors during scraping
            skipped: Number of matches skipped
            duration_seconds: Total duration in seconds
            context: Additional context data
            
        Returns:
            True if message sent successfully
        """
        # Calculate metrics
        total_attempted = matches_scraped + errors + skipped
        success_rate = (odds_scraped / matches_scraped * 100) if matches_scraped > 0 else 0
        duration_str = self._format_duration(duration_seconds)

        # Build message
        message = f"<b>{EMOJI_MAP['matches']} AIScore Daily Report - {date}</b>\n\n"
        message += f"{EMOJI_MAP['matches_scraped']} <b>Matches Found:</b> {matches_scraped}\n"
        message += f"{EMOJI_MAP['odds']} <b>Odds Scraped:</b> {odds_scraped}\n"
        
        if success_rate >= 95:
            message += f"{EMOJI_MAP['success_rate']} <b>Success Rate:</b> {success_rate:.1f}% {EMOJI_MAP['success']}\n"
        else:
            message += f"{EMOJI_MAP['success_rate']} <b>Success Rate:</b> {success_rate:.1f}% {EMOJI_MAP['warning']}\n"
        
        if errors > 0:
            message += f"{EMOJI_MAP['errors']} <b>Errors:</b> {errors}\n"
        
        if skipped > 0:
            message += f"{EMOJI_MAP['skipped']} <b>Skipped:</b> {skipped}\n"
        
        message += f"{EMOJI_MAP['duration']} <b>Duration:</b> {duration_str}\n"

        # Add context if provided
        if context:
            message += f"\n<b>Additional Details:</b>\n"
            for key, value in context.items():
                emoji = EMOJI_MAP.get(key, '•')
                message += f"{emoji} <b>{key}:</b> {value}\n"

        # Final status
        if errors == 0 and skipped == 0:
            message += f"\n{EMOJI_MAP['success']}<b> All odds scraped successfully!</b>"
        else:
            message += f"\n{EMOJI_MAP['info']} Status: Completed with issues"

        return self._send_message(message)

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format duration in seconds to human-readable format."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"


# Global reporter instance
_global_reporter: Optional[TelegramMetricsReporter] = None


def get_metrics_reporter() -> TelegramMetricsReporter:
    """Get or create the global metrics reporter."""
    global _global_reporter
    if _global_reporter is None:
        _global_reporter = TelegramMetricsReporter()
    return _global_reporter


def send_daily_report(
    scraper: str,
    date: Optional[str] = None,
    matches_scraped: int = 0,
    errors: int = 0,
    skipped: int = 0,
    odds_scraped: int = 0,
    duration_seconds: float = 0,
    cache_hits: int = 0,
    context: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Send a daily scraping report via Telegram.
    
    Args:
        scraper: 'fotmob' or 'aiscore'
        date: Date in YYYYMMDD format (defaults to today)
        matches_scraped: Number of matches scraped
        errors: Number of errors
        skipped: Number of skipped items
        odds_scraped: For AIScore - number of odds scraped
        duration_seconds: Total scraping duration
        cache_hits: For FotMob - cache hits
        context: Additional context dictionary
        
    Returns:
        True if successfully sent
    """
    if not date:
        date = datetime.now().strftime('%Y%m%d')
    
    reporter = get_metrics_reporter()
    
    if scraper.lower() == 'fotmob':
        return reporter.report_fotmob_daily(
            date=date,
            matches_scraped=matches_scraped,
            errors=errors,
            skipped=skipped,
            duration_seconds=duration_seconds,
            cache_hits=cache_hits,
            context=context
        )
    elif scraper.lower() == 'aiscore':
        return reporter.report_aiscore_daily(
            date=date,
            matches_scraped=matches_scraped,
            odds_scraped=odds_scraped,
            errors=errors,
            skipped=skipped,
            duration_seconds=duration_seconds,
            context=context
        )
    else:
        reporter.logger.warning(f"Unknown scraper: {scraper}")
        return False
