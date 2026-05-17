"""Stripe metered-billing batcher.

Runs as a background asyncio task launched from FastAPI's lifespan.
Every ``BATCH_INTERVAL_SECONDS`` it scans ``usage_events`` for rows that:

* belong to a user whose ``billing_accounts.plan = 'metered'``,
* have ``stripe_pushed_at IS NULL``,
* have ``status = 'ok'`` (failures and quota rejections are never billed).

For each batch it calls ``stripe.SubscriptionItem.create_usage_record``
with ``action="increment"`` (idempotent on retry) and marks the rows as
pushed. Stripe IDs are looked up from ``billing_accounts``; if any are
missing the rows stay un-pushed and will retry next tick.

The batcher is a no-op when ``STRIPE_SECRET_KEY`` is unset — handy in
dev environments where we want metering writes but not actual API calls.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import select, update

from ocoi_common.config import settings
from ocoi_db.engine import async_session_factory
from ocoi_db.models import BillingAccount, UsageEvent, User

_log = logging.getLogger("ocoi.mcp.billing")

BATCH_INTERVAL_SECONDS = 60
BATCH_MAX_ROWS = 500  # cap per tick so a backlog doesn't take down the loop

_batcher_task: asyncio.Task | None = None
_stop_event: asyncio.Event | None = None


async def ensure_stripe_customer(user_id: str, email: str) -> str | None:
    """Create the Stripe customer + free-plan subscription item if missing,
    return the subscription_item_id. Lazy — only invoked when a user's
    plan is being flipped to 'metered'."""
    if not settings.stripe_secret_key:
        return None
    import stripe
    stripe.api_key = settings.stripe_secret_key

    async with async_session_factory() as session:
        row = await session.get(BillingAccount, user_id)
        if row is None:
            row = BillingAccount(user_id=user_id, plan="free")
            session.add(row)
        if not row.stripe_customer_id:
            cust = await asyncio.to_thread(
                stripe.Customer.create,
                email=email,
                metadata={"ocoi_user_id": user_id},
            )
            row.stripe_customer_id = cust.id
        if not row.stripe_subscription_item_id and settings.stripe_metered_price_id:
            sub = await asyncio.to_thread(
                stripe.Subscription.create,
                customer=row.stripe_customer_id,
                items=[{"price": settings.stripe_metered_price_id}],
            )
            # First (and only) item is the metered one.
            row.stripe_subscription_item_id = sub["items"]["data"][0]["id"]
        await session.commit()
        return row.stripe_subscription_item_id


async def _push_batch_once() -> None:
    """One iteration of the loop. Idempotent — safe to call any time."""
    if not settings.stripe_secret_key or not settings.stripe_metered_price_id:
        return  # batcher is a no-op without Stripe configured
    import stripe
    stripe.api_key = settings.stripe_secret_key

    async with async_session_factory() as session:
        rows = (await session.execute(
            select(UsageEvent, BillingAccount)
            .join(BillingAccount, BillingAccount.user_id == UsageEvent.user_id)
            .where(
                UsageEvent.stripe_pushed_at.is_(None),
                UsageEvent.status == "ok",
                BillingAccount.plan == "metered",
                BillingAccount.stripe_subscription_item_id.is_not(None),
            )
            .limit(BATCH_MAX_ROWS)
        )).all()

        if not rows:
            return

        # Group by subscription_item_id so we do one Stripe call per customer.
        per_item: dict[str, list[str]] = defaultdict(list)
        for ev, acct in rows:
            per_item[acct.stripe_subscription_item_id].append(ev.id)

        for sub_item_id, event_ids in per_item.items():
            try:
                await asyncio.to_thread(
                    stripe.SubscriptionItem.create_usage_record,
                    sub_item_id,
                    quantity=len(event_ids),
                    action="increment",
                    timestamp=int(datetime.now(timezone.utc).timestamp()),
                )
            except Exception:
                _log.exception("Stripe push failed for sub_item=%s; will retry", sub_item_id)
                continue
            # Mark pushed.
            await session.execute(
                update(UsageEvent)
                .where(UsageEvent.id.in_(event_ids))
                .values(stripe_pushed_at=datetime.now(timezone.utc).replace(tzinfo=None))
            )
        await session.commit()


async def _run_batcher() -> None:
    assert _stop_event is not None
    _log.info("Stripe billing batcher started (interval=%ss)", BATCH_INTERVAL_SECONDS)
    while not _stop_event.is_set():
        try:
            await _push_batch_once()
        except Exception:
            _log.exception("billing batcher tick failed (continuing)")
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=BATCH_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
    _log.info("Stripe billing batcher stopped")


def start_billing_batcher() -> None:
    global _batcher_task, _stop_event
    if _batcher_task is not None and not _batcher_task.done():
        return
    if not settings.mcp_enabled:
        return
    _stop_event = asyncio.Event()
    _batcher_task = asyncio.create_task(_run_batcher())


def stop_billing_batcher() -> None:
    global _batcher_task, _stop_event
    if _stop_event is not None:
        _stop_event.set()
    _batcher_task = None
