"""
Daily metrics reporting via Telegram with enriched emoji indicators.

Sends end-of-day summaries for FotMob and AIScore scraping with comprehensive metrics and status.

Usage:
    from src.utils.metrics_alerts import send_daily_report
    
    send_daily_report(
        scraper='fotmob',
        date='20260218',
        matches_scraped=150,
        matches_found=156,
        errors=8,
        skipped=5,
        empty_responses=3,
        rate_limited=5,
        cache_hits=42,
        retries=12,
        avg_response_time=1.2,
        max_response_time=8.5,
        duration_seconds=2723,
        teams=28,
        players_new=892,
        players_total=8234,
        shots=4821,
        events=892,
        bronze_files=148,
        bronze_size_mb=24,
        s3_backup=True,
        clickhouse_rows=148,
    )
"""

import os
from datetime import datetime
from typing import Optional, Dict, Any, List

try:
    import requests
except ImportError:
    requests = None

from .logging_utils import get_logger
from src.storage import get_s3_uploader


EMOJI_MAP = {
    'success': '✅',
    'error': '❌',
    'warning': '⚠️',
    'info': 'ℹ️',
    
    'matches': '⚽',
    'matches_scraped': '✨',
    'matches_found': '🔍',
    'errors': '❌',
    'skipped': '⏭️',
    'empty': '⬜',
    'rate_limited': '🚫',
    'data_quality_issues': '📊',
    'duration': '⏱️',
    'start_time': '🕐',
    'end_time': '🕥',
    'success_rate': '📈',
    
    'teams': '🏆',
    'leagues': '🌍',
    'players': '👥',
    'players_new': '🆕',
    'shots': '🎯',
    'events': '📝',
    'stats': '📊',
    
    'odds': '💰',
    'betting_data': '🎲',
    'links_scraped': '🔗',
    'bookmarks': '🔖',
    'odds_sources': '📡',
    
    'database': '🗄️',
    'storage': '💾',
    'bronze': '🏠',
    's3': '☁️',
    'clickhouse': '⚡',
    'network': '🌐',
    'retry': '🔄',
    'timeout': '⏲️',
    'progress': '📊',
    'cache': '🚀',
    'cache_hit': '💨',
    
    'performance': '⚡',
    'avg_time': '⏱️',
    'max_time': '📉',
    'retries': '🔁',
    
    'data': '📦',
    'new': '🆕',
    'total': '📊',
    'issues': '🔴',
}


class TelegramMetricsReporter:
    """Reports daily metrics to Telegram with enriched formatting."""

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
        environment: Optional[str] = None
    ):
        self.bot_token = bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = chat_id or os.getenv('TELEGRAM_CHAT_ID')
        self.environment = environment or os.getenv('ENVIRONMENT', 'production')
        self.server_name = os.getenv('SERVER_NAME', 'scout')
        self.logger = get_logger()
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}" if self.bot_token else None

    def _send_message(self, text: str) -> bool:
        """Send a message to Telegram."""
        if not requests:
            self.logger.warning("requests library not available for Telegram")
            return False

        if not self.bot_token or not self.chat_id:
            self.logger.warning("Telegram not configured")
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

    def _format_duration(self, seconds: float) -> str:
        """Format duration in seconds to human-readable format."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"

    def _format_size(self, size_mb: float) -> str:
        """Format size in MB to human-readable format."""
        if size_mb >= 1024:
            return f"{size_mb / 1024:.1f} GB"
        return f"{size_mb:.1f} MB"

    def _check_s3_backup(self, scraper: str, date: str) -> str:
        """Check if backup exists in S3 and return meaningful status message."""
        s3_uploader = get_s3_uploader()
        
        if not s3_uploader:
            return "S3: ⚠️ Not configured"
        
        year_month = date[:6] if len(date) >= 6 else date
        s3_key = f"bronze/{scraper}/{year_month}/{date}.tar.gz"
        
        if s3_uploader.object_exists(s3_key):
            size_bytes = s3_uploader.get_object_size(s3_key)
            if size_bytes:
                size_mb = size_bytes / (1024 * 1024)
                size_str = f" ({size_mb:.1f} MB)" if size_mb < 1024 else f" ({size_mb / 1024:.2f} GB)"
                return f"S3: ✅ {date}.tar.gz{size_str}"
            return f"S3: ✅ {date}.tar.gz"
        
        return f"S3: ⚠️ Missing ({date}.tar.gz)"

    def _build_progress_bar(self, value: int, total: int, width: int = 10) -> str:
        """Build a text-based progress bar."""
        if total == 0:
            return "[" + "░" * width + "] 0%"
        percentage = (value / total) * 100
        filled = int((value / total) * width) if total > 0 else 0
        bar = "█" * filled + "░" * (width - filled)
        return f"[{bar}] {percentage:.1f}%"

    def _format_issue(self, issue: str, count: int) -> str:
        """Format a single issue line."""
        emoji = EMOJI_MAP['error'] if count > 0 else EMOJI_MAP['success']
        return f"  {emoji} {issue}: <b>{count}</b>"

    def report_fotmob_daily(self, date: str, **kwargs) -> bool:
        """Send enriched FotMob daily scraping report."""
        
        matches_scraped = kwargs.get('matches_scraped', 0)
        matches_found = kwargs.get('matches_found', matches_scraped)
        errors = kwargs.get('errors', 0)
        skipped = kwargs.get('skipped', 0)
        empty_responses = kwargs.get('empty_responses', 0)
        rate_limited = kwargs.get('rate_limited', 0)
        cache_hits = kwargs.get('cache_hits', 0)
        retries = kwargs.get('retries', 0)
        avg_response_time = kwargs.get('avg_response_time', 0)
        max_response_time = kwargs.get('max_response_time', 0)
        duration_seconds = kwargs.get('duration_seconds', 0)
        
        teams = kwargs.get('teams', 0)
        leagues = kwargs.get('leagues', 0)
        players_new = kwargs.get('players_new', 0)
        players_total = kwargs.get('players_total', 0)
        shots = kwargs.get('shots', 0)
        events = kwargs.get('events', 0)
        
        bronze_files = kwargs.get('bronze_files', 0)
        bronze_size_mb = kwargs.get('bronze_size_mb', 0)
        s3_backup = kwargs.get('s3_backup', False)
        clickhouse_rows = kwargs.get('clickhouse_rows', 0)
        
        context = kwargs.get('context', {})

        success_rate = (matches_scraped / matches_found * 100) if matches_found > 0 else 0
        cache_rate = (cache_hits / matches_found * 100) if matches_found > 0 else 0
        failed_permanent = errors - retries if errors > retries else 0
        
        no_matches = matches_found == 0 and skipped > 0

        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"

        message = f"<b>{EMOJI_MAP['matches']} FotMob Daily Report - {formatted_date}</b>\n"
        message += f"<i>Environment: {self.environment} | Server: {self.server_name}</i>\n\n"

        message += f"<b>📊 SCAN SUMMARY</b>\n"
        message += f"{'─' * 40}\n"
        
        if no_matches:
            message += f"{EMOJI_MAP['info']} Matches: No matches scheduled for this date\n"
            message += f"{EMOJI_MAP['skipped']} Checked: <b>{skipped}</b> dates (already scraped)\n"
        else:
            message += f"{EMOJI_MAP['matches_found']} Matches: <b>{matches_scraped}/{matches_found}</b> scraped {self._build_progress_bar(matches_scraped, matches_found)}\n"
        
        if cache_hits > 0:
            message += f"{EMOJI_MAP['cache_hit']} Cache: <b>{cache_hits}</b> hits ({cache_rate:.1f}%)\n"
        
        error_details = []
        if errors > 0:
            error_details.append(f"{errors} errors")
        if empty_responses > 0:
            error_details.append(f"{empty_responses} empty")
        if rate_limited > 0:
            error_details.append(f"{rate_limited} rate limited")
        if skipped > 0:
            error_details.append(f"{skipped} skipped")
        
        if error_details:
            message += f"{EMOJI_MAP['warning']} Issues: {', '.join(error_details)}\n"
        
        message += f"{EMOJI_MAP['duration']} Duration: <b>{self._format_duration(duration_seconds)}</b>\n\n"

        if avg_response_time > 0 or max_response_time > 0:
            message += f"<b>⚡ PERFORMANCE</b>\n"
            message += f"{'─' * 40}\n"
            if avg_response_time > 0:
                message += f"{EMOJI_MAP['avg_time']} Avg Response: <b>{avg_response_time:.2f}s</b>\n"
            if max_response_time > 0:
                message += f"{EMOJI_MAP['max_time']} Max Response: <b>{max_response_time:.2f}s</b>\n"
            if retries > 0:
                message += f"{EMOJI_MAP['retries']} Retries: <b>{retries}</b>"
                if failed_permanent > 0:
                    message += f" ({failed_permanent} failed)"
                message += "\n"
            message += "\n"

        data_parts = []
        if teams > 0:
            data_parts.append(f"{EMOJI_MAP['leagues']} {teams} leagues")
        if players_total > 0:
            data_parts.append(f"{EMOJI_MAP['players']} {players_total:,}")
        if shots > 0:
            data_parts.append(f"{EMOJI_MAP['shots']} {shots:,}")
        if events > 0:
            data_parts.append(f"{EMOJI_MAP['events']} {events:,}")
        
        if data_parts:
            message += f"<b>📦 DATA COLLECTED</b>\n"
            message += f"{'─' * 40}\n"
            message += " | ".join(data_parts) + "\n"
            if players_new > 0:
                message += f"{EMOJI_MAP['new']} +{players_new:,} new players\n"
            message += "\n"

        message += f"<b>💾 STORAGE</b>\n"
        message += f"{'─' * 40}\n"
        
        storage_parts = []
        if bronze_files > 0:
            size_str = self._format_size(bronze_size_mb) if bronze_size_mb > 0 else ""
            storage_parts.append(f"{EMOJI_MAP['bronze']} Bronze: <b>{bronze_files}</b> files")
            if bronze_size_mb > 0:
                storage_parts[-1] += f" ({self._format_size(bronze_size_mb)})"
        
        storage_parts.append(f"{EMOJI_MAP['s3']} {self._check_s3_backup('fotmob', date)}")
        
        if clickhouse_rows > 0:
            storage_parts.append(f"{EMOJI_MAP['clickhouse']} ClickHouse: <b>{clickhouse_rows}</b> rows")
        
        message += "\n".join(storage_parts) + "\n\n"

        issues_list = []
        if empty_responses > 0:
            issues_list.append(f"Empty response data")
        if rate_limited > 0:
            issues_list.append(f"API rate limited ({rate_limited} times)")
        if failed_permanent > 0:
            issues_list.append(f"Permanent failures ({failed_permanent})")
        
        if issues_list:
            message += f"<b>🔴 ISSUES</b>\n"
            message += f"{'─' * 40}\n"
            for issue in issues_list:
                message += f"  • {issue}\n"
            message += "\n"

        if no_matches:
            message += f"{EMOJI_MAP['info']} No matches to scrape - already processed previously"
        elif success_rate >= 95 and errors == 0:
            message += f"{EMOJI_MAP['success']} <b>All matches scraped successfully!</b>"
        elif success_rate >= 90:
            message += f"{EMOJI_MAP['info']} Completed with minor issues"
        else:
            message += f"{EMOJI_MAP['warning']} <b>Review required</b> - Success rate: {success_rate:.1f}%"

        if context:
            message += f"\n\n<b>📋 CONTEXT</b>\n"
            message += f"{'─' * 40}\n"
            for key, value in context.items():
                emoji = EMOJI_MAP.get(key, '•')
                message += f"{emoji} <b>{key}:</b> {value}\n"

        return self._send_message(message)

    def report_aiscore_daily(self, date: str, **kwargs) -> bool:
        """Send enriched AIScore daily scraping report."""
        
        matches_found = kwargs.get('matches_found', 0)
        matches_scraped = kwargs.get('matches_scraped', 0)
        odds_scraped = kwargs.get('odds_scraped', 0)
        odds_sources = kwargs.get('odds_sources', 0)
        odds_sources_total = kwargs.get('odds_sources_total', 0)
        errors = kwargs.get('errors', 0)
        skipped = kwargs.get('skipped', 0)
        rate_limited = kwargs.get('rate_limited', 0)
        links_scraped = kwargs.get('links_scraped', 0)
        bookmarks = kwargs.get('bookmarks', 0)
        duration_seconds = kwargs.get('duration_seconds', 0)
        
        bronze_files = kwargs.get('bronze_files', 0)
        bronze_size_mb = kwargs.get('bronze_size_mb', 0)
        s3_backup = kwargs.get('s3_backup', False)
        clickhouse_rows = kwargs.get('clickhouse_rows', 0)
        
        context = kwargs.get('context', {})

        success_rate = (odds_scraped / matches_found * 100) if matches_found > 0 else 0
        odds_coverage = (odds_sources / odds_sources_total * 100) if odds_sources_total > 0 else 0
        no_matches = matches_found == 0

        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"

        message = f"<b>{EMOJI_MAP['odds']} AIScore Daily Report - {formatted_date}</b>\n"
        message += f"<i>Environment: {self.environment} | Server: {self.server_name}</i>\n\n"

        message += f"<b>📊 SCAN SUMMARY</b>\n"
        message += f"{'─' * 40}\n"
        
        if no_matches:
            message += f"{EMOJI_MAP['info']} Matches: No matches scheduled for this date\n"
            message += f"{EMOJI_MAP['odds']} Odds: <b>{odds_scraped}</b> scraped\n"
        else:
            message += f"{EMOJI_MAP['matches_found']} Matches: <b>{matches_found}</b> found\n"
            message += f"{EMOJI_MAP['odds']} Odds: <b>{odds_scraped}</b> scraped\n"
        
        if odds_sources_total > 0:
            message += f"{EMOJI_MAP['odds_sources']} Sources: <b>{odds_sources}/{odds_sources_total}</b> ({odds_coverage:.0f}%)\n"
        
        error_details = []
        if errors > 0:
            error_details.append(f"{errors} errors")
        if rate_limited > 0:
            error_details.append(f"{rate_limited} rate limited")
        if skipped > 0:
            error_details.append(f"{skipped} skipped")
        
        if error_details:
            message += f"{EMOJI_MAP['warning']} Issues: {', '.join(error_details)}\n"
        
        message += f"{EMOJI_MAP['duration']} Duration: <b>{self._format_duration(duration_seconds)}</b>\n\n"

        data_parts = []
        if links_scraped > 0:
            data_parts.append(f"{EMOJI_MAP['links_scraped']} {links_scraped:,} links")
        if bookmarks > 0:
            data_parts.append(f"{EMOJI_MAP['bookmarks']} {bookmarks:,} bookmarks")
        
        if data_parts:
            message += f"<b>📦 DATA COLLECTED</b>\n"
            message += f"{'─' * 40}\n"
            message += " | ".join(data_parts) + "\n\n"

        message += f"<b>💾 STORAGE</b>\n"
        message += f"{'─' * 40}\n"
        
        storage_parts = []
        if bronze_files > 0:
            size_str = self._format_size(bronze_size_mb) if bronze_size_mb > 0 else ""
            storage_parts.append(f"{EMOJI_MAP['bronze']} Bronze: <b>{bronze_files}</b> files")
            if bronze_size_mb > 0:
                storage_parts[-1] += f" ({self._format_size(bronze_size_mb)})"
        
        storage_parts.append(f"{EMOJI_MAP['s3']} {self._check_s3_backup('aiscore', date)}")
        
        if clickhouse_rows > 0:
            storage_parts.append(f"{EMOJI_MAP['clickhouse']} ClickHouse: <b>{clickhouse_rows}</b> rows")
        
        message += "\n".join(storage_parts) + "\n\n"

        issues_list = []
        if rate_limited > 0:
            issues_list.append(f"API rate limited ({rate_limited} times)")
        if errors > 0:
            issues_list.append(f"{errors} scraping errors")
        if odds_coverage < 50:
            issues_list.append(f"Low odds coverage ({odds_coverage:.0f}%)")
        
        if issues_list:
            message += f"<b>🔴 ISSUES</b>\n"
            message += f"{'─' * 40}\n"
            for issue in issues_list:
                message += f"  • {issue}\n"
            message += "\n"

        if no_matches:
            message += f"{EMOJI_MAP['info']} No matches to scrape - already processed previously"
        elif success_rate >= 95 and errors == 0:
            message += f"{EMOJI_MAP['success']} <b>All odds scraped successfully!</b>"
        elif success_rate >= 80:
            message += f"{EMOJI_MAP['info']} Completed with minor issues"
        else:
            message += f"{EMOJI_MAP['warning']} <b>Review required</b> - Success rate: {success_rate:.1f}%"

        if context:
            message += f"\n\n<b>📋 CONTEXT</b>\n"
            message += f"{'─' * 40}\n"
            for key, value in context.items():
                emoji = EMOJI_MAP.get(key, '•')
                message += f"{emoji} <b>{key}:</b> {value}\n"

        return self._send_message(message)


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
    matches_found: int = 0,
    errors: int = 0,
    skipped: int = 0,
    empty_responses: int = 0,
    rate_limited: int = 0,
    odds_scraped: int = 0,
    odds_sources: int = 0,
    odds_sources_total: int = 0,
    duration_seconds: float = 0,
    cache_hits: int = 0,
    retries: int = 0,
    avg_response_time: float = 0,
    max_response_time: float = 0,
    teams: int = 0,
    leagues: int = 0,
    players_new: int = 0,
    players_total: int = 0,
    shots: int = 0,
    events: int = 0,
    links_scraped: int = 0,
    bookmarks: int = 0,
    bronze_files: int = 0,
    bronze_size_mb: float = 0,
    s3_backup: bool = False,
    clickhouse_rows: int = 0,
    context: Optional[Dict[str, Any]] = None
) -> bool:
    """Send an enriched daily scraping report via Telegram."""
    
    if not date:
        date = datetime.now().strftime('%Y%m%d')
    
    reporter = get_metrics_reporter()
    
    kwargs = {
        'matches_scraped': matches_scraped,
        'matches_found': matches_found or matches_scraped,
        'errors': errors,
        'skipped': skipped,
        'empty_responses': empty_responses,
        'rate_limited': rate_limited,
        'odds_scraped': odds_scraped,
        'odds_sources': odds_sources,
        'odds_sources_total': odds_sources_total,
        'duration_seconds': duration_seconds,
        'cache_hits': cache_hits,
        'retries': retries,
        'avg_response_time': avg_response_time,
        'max_response_time': max_response_time,
        'teams': teams,
        'leagues': leagues,
        'players_new': players_new,
        'players_total': players_total,
        'shots': shots,
        'events': events,
        'links_scraped': links_scraped,
        'bookmarks': bookmarks,
        'bronze_files': bronze_files,
        'bronze_size_mb': bronze_size_mb,
        's3_backup': s3_backup,
        'clickhouse_rows': clickhouse_rows,
        'context': context or {},
    }
    
    if scraper.lower() == 'fotmob':
        return reporter.report_fotmob_daily(date=date, **kwargs)
    elif scraper.lower() == 'aiscore':
        return reporter.report_aiscore_daily(date=date, **kwargs)
    else:
        reporter.logger.warning(f"Unknown scraper: {scraper}")
        return False
