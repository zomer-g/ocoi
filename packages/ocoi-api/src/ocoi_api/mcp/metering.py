"""Per-tool-call metering decorator + monthly-quota check.

Every MCP tool is wrapped so we get exactly one ``usage_events`` row per
invocation — including failures and quota rejections. Quota enforcement
sits in front of the tool body so blocked calls are cheap.

The writes are async-fire-and-forget against a fresh session: the tool's
own latency must not depend on the metering write succeeding.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Awaitable, Callable

from sqlalchemy import func, select

from ocoi_api.mcp.auth import MCPCaller, get_current_caller
from ocoi_db.engine import async_session_factory
from ocoi_db.models import UsageEvent

_log = logging.getLogger("ocoi.mcp.metering")


class QuotaExceeded(Exception):
    """Raised by ``check_quota`` when the caller is over their monthly cap."""


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def _count_calls_this_month(user_id: str) -> int:
    start_of_month = _now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    async with async_session_factory() as session:
        row = await session.execute(
            select(func.count(UsageEvent.id)).where(
                UsageEvent.user_id == user_id,
                UsageEvent.started_at >= start_of_month,
                UsageEvent.status == "ok",
            )
        )
        return int(row.scalar() or 0)


async def check_quota(caller: MCPCaller) -> None:
    """Raise QuotaExceeded if the caller would exceed their monthly cap.
    ``None`` quota = unlimited. ``0`` = effectively blocked."""
    if caller.monthly_quota is None:
        return
    used = await _count_calls_this_month(caller.user_id)
    if used >= caller.monthly_quota:
        raise QuotaExceeded(
            f"Monthly quota of {caller.monthly_quota} tool calls reached"
        )


async def _record_event(
    *,
    user_id: str,
    client_id: str | None,
    tool: str,
    started_at: datetime,
    duration_ms: int,
    bytes_in: int,
    bytes_out: int,
    status: str,
    error_message: str | None,
) -> None:
    try:
        async with async_session_factory() as session:
            session.add(UsageEvent(
                user_id=user_id,
                client_id=client_id,
                tool=tool,
                started_at=started_at,
                duration_ms=duration_ms,
                bytes_in=bytes_in,
                bytes_out=bytes_out,
                status=status,
                error_message=error_message,
            ))
            await session.commit()
    except Exception:
        # Never let a metering failure surface to the caller. Log and move on.
        _log.exception("usage_event write failed for tool=%s user=%s", tool, user_id)


def metered(tool_name: str) -> Callable:
    """Decorate an async tool function with quota + metering logic."""

    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            caller = get_current_caller()
            started_at = _now()
            t0 = time.perf_counter()

            try:
                bytes_in = len(json.dumps(kwargs, default=str, ensure_ascii=False).encode("utf-8"))
            except Exception:
                bytes_in = 0

            # ── Quota gate ──
            try:
                await check_quota(caller)
            except QuotaExceeded as q:
                asyncio.create_task(_record_event(
                    user_id=caller.user_id, client_id=caller.client_id, tool=tool_name,
                    started_at=started_at, duration_ms=0,
                    bytes_in=bytes_in, bytes_out=0,
                    status="quota_exceeded", error_message=str(q),
                ))
                # Re-raise so the MCP layer returns an isError tool result.
                raise

            # ── Actual call ──
            try:
                result = await fn(*args, **kwargs)
                status = "ok"
                error_message = None
            except Exception as e:
                status = "error"
                error_message = f"{type(e).__name__}: {e}"[:500]
                duration_ms = int((time.perf_counter() - t0) * 1000)
                asyncio.create_task(_record_event(
                    user_id=caller.user_id, client_id=caller.client_id, tool=tool_name,
                    started_at=started_at, duration_ms=duration_ms,
                    bytes_in=bytes_in, bytes_out=0,
                    status=status, error_message=error_message,
                ))
                raise

            duration_ms = int((time.perf_counter() - t0) * 1000)
            try:
                bytes_out = len(json.dumps(result, default=str, ensure_ascii=False).encode("utf-8"))
            except Exception:
                bytes_out = 0
            asyncio.create_task(_record_event(
                user_id=caller.user_id, client_id=caller.client_id, tool=tool_name,
                started_at=started_at, duration_ms=duration_ms,
                bytes_in=bytes_in, bytes_out=bytes_out,
                status=status, error_message=error_message,
            ))
            return result

        return wrapper

    return decorator
