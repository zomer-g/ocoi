"""Israel timezone utilities.

The container runs with TZ=Asia/Jerusalem, so datetime.now() already
returns Israel local time. This module provides explicit helpers for
clarity and for columns that need timezone-aware datetimes.
"""

from datetime import datetime, timezone, timedelta, tzinfo

try:
    from zoneinfo import ZoneInfo
    ISR_TZ: tzinfo = ZoneInfo("Asia/Jerusalem")
except ImportError:
    # Fallback for environments without zoneinfo (shouldn't happen on Python 3.9+)
    ISR_TZ = timezone(timedelta(hours=2))  # IST (doesn't handle DST)


def now_israel() -> datetime:
    """Return the current datetime in Israel timezone (aware)."""
    return datetime.now(ISR_TZ)


def now_israel_naive() -> datetime:
    """Return the current datetime in Israel timezone (naive, for TIMESTAMP WITHOUT TIME ZONE columns)."""
    return datetime.now(ISR_TZ).replace(tzinfo=None)
