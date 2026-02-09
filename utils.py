from __future__ import annotations

from datetime import datetime


def format_duration(started_at: str | None, ended_at: str | None) -> str:
    if not started_at or not ended_at:
        return ""
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        end = datetime.fromisoformat(ended_at.replace("Z", "+00:00"))
    except ValueError:
        return ""
    duration = end - start
    if duration.total_seconds() < 0:
        return ""
    total_seconds = int(duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def format_timestamp(value: object) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    return parsed.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
