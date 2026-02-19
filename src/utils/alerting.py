"""
Alerting system for monitoring and notifying about system issues and daily metrics.

Supports alerts via:
- Email (SMTP)
- Telegram Bot
- Logging

Usage:
    from src.utils.alerting import AlertManager, AlertLevel
    
    alert_manager = AlertManager()
    alert_manager.send_alert(
        level=AlertLevel.ERROR,
        title="Scrape Failed",
        message="Failed to scrape match 12345",
        context={"match_id": "12345", "error": "Connection timeout"}
    )
    
    # For daily metrics via Telegram:
    from src.utils.metrics_alerts import send_daily_report
    send_daily_report(
        scraper='fotmob',
        matches_scraped=150,
        errors=2,
        skipped=5
    )
"""
















import json
import os
import smtplib
from dataclasses import dataclass
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from enum import Enum
from typing import Dict, Any, Optional, List

try:
    import requests
except ImportError:
    requests = None

from .logging_utils import get_logger


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Represents an alert message."""
    level: AlertLevel
    title: str
    message: str
    context: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "level": self.level.value,
            "title": self.title,
            "message": self.message,
            "context": self.context or {},
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }

    def to_string(self) -> str:
        """Convert alert to human-readable string."""
        level_str = self.level.value.upper()
        context_str = ""
        if self.context:
            context_str = f"\nContext: {json.dumps(self.context, indent=2)}"

        return f"[{level_str}] {self.title}\n{self.message}{context_str}"


class AlertChannel:
    """Base class for alert channels."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.logger = get_logger()

    def send(self, alert: Alert) -> bool:
        """
        Send an alert through this channel.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False

        try:
            return self._send_impl(alert)
        except Exception as e:
            self.logger.error(f"Failed to send alert through {self.__class__.__name__}: {e}")
            return False

    def _send_impl(self, alert: Alert) -> bool:
        """Implementation-specific send logic. Override in subclasses."""
        raise NotImplementedError


class LoggingChannel(AlertChannel):
    """Alert channel that logs to the application logger."""

    def _send_impl(self, alert: Alert) -> bool:
        """Log the alert."""
        level_map = {
            AlertLevel.INFO: self.logger.info,
            AlertLevel.WARNING: self.logger.warning,
            AlertLevel.ERROR: self.logger.error,
            AlertLevel.CRITICAL: self.logger.critical
        }

        log_func = level_map.get(alert.level, self.logger.info)
        log_func(f"ALERT: {alert.title} - {alert.message}")

        if alert.context:
            self.logger.debug(f"Alert context: {json.dumps(alert.context, indent=2)}")

        return True


class EmailChannel(AlertChannel):
    """Alert channel that sends emails via SMTP."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        from_email: str = "",
        to_emails: List[str] = None,
        enabled: bool = True
    ):
        super().__init__(enabled)
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_email = from_email
        self.to_emails = to_emails or []

    def _send_impl(self, alert: Alert) -> bool:
        """Send alert via email."""
        if not self.to_emails:
            self.logger.warning("No email recipients configured")
            return False

        try:
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ", ".join(self.to_emails)
            msg['Subject'] = f"[{alert.level.value.upper()}] {alert.title}"

            body = alert.to_string()
            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.smtp_user and self.smtp_password:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            self.logger.info(f"Email alert sent successfully to {len(self.to_emails)} recipient(s): {', '.join(self.to_emails)}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send email alert: {e}")
            return False


class TelegramChannel(AlertChannel):
    """Alert channel that sends messages via Telegram Bot."""

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        enabled: bool = True
    ):
        super().__init__(enabled)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"

    def _send_impl(self, alert: Alert) -> bool:
        """Send alert via Telegram."""
        if not requests:
            self.logger.warning("requests library not available for Telegram alerts")
            return False

        if not self.bot_token or not self.chat_id:
            self.logger.warning("Telegram bot token or chat ID not configured")
            return False

        try:
            # Format the message with level indicator
            level_emoji = {
                AlertLevel.INFO: "ℹ️",
                AlertLevel.WARNING: "⚠️",
                AlertLevel.ERROR: "❌",
                AlertLevel.CRITICAL: "🚨"
            }
            emoji = level_emoji.get(alert.level, "📢")
            
            message = f"{emoji} <b>[{alert.level.value.upper()}] {alert.title}</b>\n\n{alert.message}"
            
            if alert.context:
                message += f"\n\n<b>Details:</b>\n"
                for key, value in alert.context.items():
                    message += f"  • <b>{key}:</b> {value}\n"

            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }

            response = requests.post(f"{self.api_url}/sendMessage", json=payload, timeout=10)
            
            if response.status_code == 200:
                self.logger.info(f"Telegram alert sent successfully to chat {self.chat_id}")
                return True
            else:
                self.logger.error(f"Telegram API error ({response.status_code}): {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to send Telegram alert: {e}")
            return False


class AlertManager:
    """Manages alert channels and sends alerts."""

    def __init__(
        self,
        channels: Optional[List[AlertChannel]] = None,
        min_level: AlertLevel = AlertLevel.WARNING
    ):
        """
        Initialize alert manager.
        
        Args:
            channels: List of alert channels. If None, uses default (logging only).
            min_level: Minimum alert level to send (alerts below this are ignored).
        """
        self.logger = get_logger()
        self.min_level = min_level

        if channels is None:
            channels = [LoggingChannel()]

        self.channels = channels
        self._load_config()

    def _load_config(self):
        """Load alert configuration from environment variables."""
        # Email configuration
        smtp_host = os.getenv('ALERT_SMTP_HOST')
        if smtp_host:
            smtp_port = int(os.getenv('ALERT_SMTP_PORT', '587'))
            smtp_user = os.getenv('ALERT_SMTP_USER')
            smtp_password = os.getenv('ALERT_SMTP_PASSWORD')
            from_email = os.getenv('ALERT_FROM_EMAIL', '')
            to_emails = [e.strip() for e in os.getenv('ALERT_TO_EMAILS', '').split(',') if e.strip()]

            if to_emails:
                email_channel = EmailChannel(
                    smtp_host=smtp_host,
                    smtp_port=smtp_port,
                    smtp_user=smtp_user,
                    smtp_password=smtp_password,
                    from_email=from_email,
                    to_emails=to_emails,
                    enabled=True
                )
                self.channels.append(email_channel)
                self.logger.info(f"Email alerts enabled: {len(to_emails)} recipients")
            else:
                self.logger.warning("ALERT_SMTP_HOST is set but ALERT_TO_EMAILS is empty. Email alerts disabled.")

        # Telegram configuration
        telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if telegram_bot_token and telegram_chat_id:
            telegram_channel = TelegramChannel(
                bot_token=telegram_bot_token,
                chat_id=telegram_chat_id,
                enabled=True
            )
            self.channels.append(telegram_channel)
            self.logger.info(f"Telegram alerts enabled for chat {telegram_chat_id}")
        elif telegram_bot_token or telegram_chat_id:
            self.logger.warning("Partial Telegram configuration: both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required")

    def send_alert(
        self,
        level: AlertLevel,
        title: str,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send an alert through all configured channels.
        
        Args:
            level: Alert severity level
            title: Alert title
            message: Alert message
            context: Additional context data
            
        Returns:
            True if at least one channel succeeded, False otherwise
        """
        level_priority = {
            AlertLevel.INFO: 1,
            AlertLevel.WARNING: 2,
            AlertLevel.ERROR: 3,
            AlertLevel.CRITICAL: 4
        }

        if level_priority.get(level, 0) < level_priority.get(self.min_level, 0):
            return False

        alert = Alert(
            level=level,
            title=title,
            message=message,
            context=context
        )

        results = []
        channel_names = []
        for channel in self.channels:
            try:
                result = channel.send(alert)
                results.append(result)
                channel_names.append(f"{channel.__class__.__name__}: {'success' if result else 'failed'}")
            except Exception as e:
                self.logger.error(f"Error sending alert through {channel.__class__.__name__}: {e}")
                results.append(False)
                channel_names.append(f"{channel.__class__.__name__}: error")

        success = any(results)
        if success:
            self.logger.debug(f"Alert sent through channels: {', '.join(channel_names)}")
        else:
            self.logger.warning(f"Alert failed to send through all channels: {', '.join(channel_names)}")

        return success

    def alert_failed_scrape(
        self,
        match_id: str,
        error: str,
        error_type: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """Send alert for a failed scrape."""
        full_context = {
            "match_id": match_id,
            "error": error,
            "error_type": error_type or "Unknown",
            "alert_type": "failed_scrape"
        }
        if context:
            full_context.update(context)

        return self.send_alert(
            level=AlertLevel.ERROR,
            title=f"Scrape Failed: Match {match_id}",
            message=f"Failed to scrape match {match_id}: {error}",
            context=full_context
        )

    def alert_data_quality_issue(
        self,
        match_id: str,
        issues: List[str],
        context: Optional[Dict[str, Any]] = None
    ):
        """Send alert for data quality issues."""
        full_context = {
            "match_id": match_id,
            "issues": issues,
            "issue_count": len(issues),
            "alert_type": "data_quality"
        }
        if context:
            full_context.update(context)

        return self.send_alert(
            level=AlertLevel.WARNING,
            title=f"Data Quality Issue: Match {match_id}",
            message=f"Data quality issues detected for match {match_id}: {', '.join(issues[:3])}" +
                    (f" (and {len(issues) - 3} more)" if len(issues) > 3 else ""),
            context=full_context
        )

    def alert_system_failure(
        self,
        component: str,
        error: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """Send alert for system failure."""
        full_context = {
            "component": component,
            "error": error,
            "alert_type": "system_failure"
        }
        if context:
            full_context.update(context)

        return self.send_alert(
            level=AlertLevel.CRITICAL,
            title=f"System Failure: {component}",
            message=f"System failure in {component}: {error}",
            context=full_context
        )

    def alert_health_check_failure(
        self,
        component: str,
        status: str,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """Send alert for health check failure."""
        level = AlertLevel.CRITICAL if status in ["error", "critical"] else AlertLevel.WARNING

        full_context = {
            "component": component,
            "status": status,
            "alert_type": "health_check"
        }
        if context:
            full_context.update(context)

        return self.send_alert(
            level=level,
            title=f"Health Check Failed: {component}",
            message=f"Health check failed for {component}: {message}",
            context=full_context
        )



_global_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get or create the global alert manager instance."""
    global _global_alert_manager
    if _global_alert_manager is None:
        _global_alert_manager = AlertManager()
    return _global_alert_manager


def set_alert_manager(manager: AlertManager):
    """Set the global alert manager instance."""
    global _global_alert_manager
    _global_alert_manager = manager
