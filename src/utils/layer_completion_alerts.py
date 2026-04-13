"""Telegram completion alerts for bronze/silver/gold layer scripts."""

from datetime import datetime, timezone
from typing import Sequence

from .metrics_alerts import send_raw_telegram_message


LAYER_EMOJI = {
    "bronze": "🥉",
    "silver": "🥈",
    "gold": "🥇",
    "quality": "🔎",
}


def _format_duration(duration_seconds: float) -> str:
    if duration_seconds < 60:
        return f"{duration_seconds:.0f}s"
    if duration_seconds < 3600:
        minutes = int(duration_seconds // 60)
        seconds = int(duration_seconds % 60)
        return f"{minutes}m {seconds}s"
    hours = int(duration_seconds // 3600)
    minutes = int((duration_seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def send_layer_completion_alert(
    *,
    layer: str,
    summary_message: str,
    scope: str,
    success: bool,
    duration_seconds: float,
    detail_lines: Sequence[str],
    insight_lines: Sequence[str] = (),
    action_lines: Sequence[str] = (),
) -> bool:
    """Send a standardized completion message for a pipeline layer."""
    layer_key = layer.lower()
    status = "SUCCESS" if success else "FAILED"
    status_emoji = "✅" if success else "❌"
    layer_emoji = LAYER_EMOJI.get(layer_key, "📦")
    layer_title = layer_key.capitalize()

    details = "".join(f"• {line}\n" for line in detail_lines)
    insights = "".join(f"• {line}\n" for line in insight_lines)
    actions = "".join(f"• {line}\n" for line in action_lines)
    completed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    message = (
        f"<b>{status_emoji} {layer_emoji} {layer_title} Layer Completed</b>\n"
        f"<i>{summary_message}</i>\n\n"
        f"<b>Execution Snapshot</b>\n"
        f"• Status: <b>{status}</b>\n"
        f"• Scope: <b>{scope}</b>\n"
        f"• Duration: <b>{_format_duration(duration_seconds)}</b>\n\n"
        f"• Completed At: <b>{completed_at}</b>\n\n"
        f"<b>Layer Details</b>\n"
        f"{details}"
    )
    if insights:
        message += f"\n<b>Operational Insights</b>\n{insights}"
    if actions:
        message += f"\n<b>Suggested Next Actions</b>\n{actions}"
    return send_raw_telegram_message(message)
