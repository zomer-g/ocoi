"""Registry sync & entity matching service — syncs Israeli gov DATAGOV registries
and matches extracted entities against them using fuzzy name similarity."""

import gc
from datetime import datetime

import httpx
from sqlalchemy import select, func, and_

from ocoi_common.config import settings
from ocoi_common.timezone import now_israel, now_israel_naive
from ocoi_common.logging import setup_logging
from ocoi_db.engine import async_session_factory, bg_session_factory
from ocoi_db.models import RegistryRecord, RegistrySyncStatus, Company, Association

# Lazy import — ocoi_matcher may not be installed in Docker (skipped in Dockerfile)
def _get_matcher():
    from ocoi_matcher.fuzzy_match import normalize_company_name, match_score
    return normalize_company_name, match_score

logger = setup_logging("ocoi.api.registry")

# ── Registry source definitions ───────────────────────────────────────────

REGISTRY_SOURCES = {
    "companies": {
        "resource_id": "f004176c-b85f-4542-8901-7b3176f9a054",
        "name_field": "שם חברה",
        "number_field": "מספר חברה",
        "status_field": "סטטוס חברה",
        "label": "חברות",
        "entity_type": "company",
    },
    "associations": {
        "resource_id": "be5b7935-3922-45d4-9638-08871b17ec95",
        "name_field": "שם עמותה בעברית",
        "number_field": "מספר עמותה",
        "status_field": "סטטוס עמותה",
        "label": "עמותות",
        "entity_type": "association",
    },
    "public_benefit": {
        "resource_id": "85e40960-5426-4f4c-874f-2d1ec1b94609",
        "name_field": "שם חלצ בעברית",
        "number_field": "מספר חלצ",
        "status_field": "סטטוס חלצ",
        "label": "חברות לתועלת הציבור",
        "entity_type": "company",
    },
    "local_authorities": {
        "resource_id": "c4916937-f5d3-4295-a22e-88a1af5cde6a",
        "name_field": "LocalAuthorityName",
        "number_field": "LocalAuthorityHPNumber",
        "status_field": None,
        "label": "רשויות מקומיות",
        "entity_type": "company",
    },
    "municipal_corporations": {
        "resource_id": "4d7e9bb8-2457-46f9-9eb3-0c0acf5cd766",
        "name_field": "corporation",
        "number_field": "corporation_number",
        "status_field": None,
        "label": "תאגידים עירוניים",
        "entity_type": "company",
        "deduplicate_by": ("corporation", "corporation_number"),
    },
}

# Source types that match against the "company" entity type
COMPANY_SOURCE_TYPES = [
    k for k, v in REGISTRY_SOURCES.items() if v["entity_type"] == "company"
]
# Source types that match against the "association" entity type
ASSOCIATION_SOURCE_TYPES = [
    k for k, v in REGISTRY_SOURCES.items() if v["entity_type"] == "association"
]


# ── Sync state (module-level dict for polling) ────────────────────────────

_registry_sync_state: dict = {
    "running": False,
    "source": None,
    "total_remote": 0,
    "fetched": 0,
    "saved": 0,
    "errors": 0,
    "error_messages": [],
    "started_at": None,
    "finished_at": None,
}


def get_registry_sync_state() -> dict:
    return dict(_registry_sync_state)


def _reset_sync_state():
    _registry_sync_state.update({
        "running": False,
        "source": None,
        "total_remote": 0,
        "fetched": 0,
        "saved": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": None,
        "finished_at": None,
    })


# ── Match state (module-level dict for polling) ──────────────────────────

_registry_match_state: dict = {
    "running": False,
    "total": 0,
    "processed": 0,
    "matched": 0,
    "errors": 0,
    "error_messages": [],
    "started_at": None,
    "finished_at": None,
}


def get_registry_match_state() -> dict:
    return dict(_registry_match_state)


# ── Registry sync ────────────────────────────────────────────────────────


async def run_registry_sync(source_type: str):
    """Sync a single registry source from CKAN DATAGOV. Background task."""
    global _registry_sync_state

    if source_type not in REGISTRY_SOURCES:
        raise ValueError(f"Unknown registry source: {source_type}")

    _reset_sync_state()
    _registry_sync_state.update({
        "running": True,
        "source": source_type,
        "started_at": now_israel().isoformat(),
    })

    source_config = REGISTRY_SOURCES[source_type]

    try:
        # Mark sync status as "syncing"
        async with bg_session_factory() as session:
            await _get_or_create_sync_status(session, source_type, "syncing")
            await session.commit()

        batch_size = settings.registry_sync_batch_size
        resource_id = source_config["resource_id"]
        base_url = f"{settings.datagov_base_url}/api/3/action/datastore_search"

        offset = 0
        total_saved = 0
        seen_keys: set[str] = set()  # For deduplication (municipal_corporations)

        # Only fetch the fields we actually need (name, number, status)
        needed_fields = [source_config["name_field"], source_config["number_field"]]
        if source_config.get("status_field"):
            needed_fields.append(source_config["status_field"])
        if source_config.get("deduplicate_by"):
            for f in source_config["deduplicate_by"]:
                if f not in needed_fields:
                    needed_fields.append(f)
        fields_param = ",".join(needed_fields)

        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as http:
            # First request to get total
            first_resp = await _fetch_ckan_page(http, base_url, resource_id, 0, batch_size, fields=fields_param)
            total_remote = first_resp.get("result", {}).get("total", 0)
            _registry_sync_state["total_remote"] = total_remote

            records = first_resp.get("result", {}).get("records", [])
            saved = await _process_batch(
                source_type, source_config, records, seen_keys
            )
            total_saved += saved
            offset += len(records)
            _registry_sync_state["fetched"] = offset
            _registry_sync_state["saved"] = total_saved

            # Paginate through remaining
            while offset < total_remote:
                try:
                    data = await _fetch_ckan_page(http, base_url, resource_id, offset, batch_size, fields=fields_param)
                    records = data.get("result", {}).get("records", [])
                    if not records:
                        break
                    saved = await _process_batch(
                        source_type, source_config, records, seen_keys
                    )
                    total_saved += saved
                    offset += len(records)
                    _registry_sync_state["fetched"] = offset
                    _registry_sync_state["saved"] = total_saved
                except Exception as e:
                    _registry_sync_state["errors"] += 1
                    if len(_registry_sync_state["error_messages"]) < 20:
                        _registry_sync_state["error_messages"].append(
                            f"Page offset={offset}: {e}"
                        )
                    offset += batch_size  # Skip this batch and continue

                gc.collect()

        # Update sync status
        async with bg_session_factory() as session:
            sync_row = await _get_or_create_sync_status(session, source_type, "completed")
            sync_row.record_count = total_saved
            sync_row.last_synced_at = now_israel_naive()
            sync_row.error_message = None
            await session.commit()

        logger.info(f"Registry sync complete: {source_type} — {total_saved} records saved")

    except Exception as e:
        logger.error(f"Registry sync failed for {source_type}: {e}", exc_info=True)
        _registry_sync_state["errors"] += 1
        _registry_sync_state["error_messages"].append(f"Fatal: {e}")

        try:
            async with bg_session_factory() as session:
                sync_row = await _get_or_create_sync_status(session, source_type, "failed")
                sync_row.error_message = str(e)[:500]
                await session.commit()
        except Exception:
            pass

    finally:
        _registry_sync_state["running"] = False
        _registry_sync_state["finished_at"] = now_israel().isoformat()


async def _fetch_ckan_page(
    http: httpx.AsyncClient, base_url: str, resource_id: str, offset: int, limit: int,
    retries: int = 12, fields: str = "",
) -> dict:
    """Fetch a single page from the CKAN datastore API with retries.

    Handles 409 Conflict (CKAN datastore busy/rebuilding) with aggressive
    back-off — up to ~10 minutes total wait for 409s.
    """
    import asyncio

    url = f"{base_url}?resource_id={resource_id}&limit={limit}&offset={offset}&fields={fields}" if fields else f"{base_url}?resource_id={resource_id}&limit={limit}&offset={offset}"
    for attempt in range(retries):
        try:
            resp = await http.get(url)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            if attempt == retries - 1:
                raise
            if e.response.status_code == 409:
                # 409 = CKAN datastore busy/rebuilding — wait 20-90s per attempt
                delay = min(20 * (attempt + 1), 90)
                logger.warning(f"CKAN 409 (attempt {attempt + 1}/{retries}), retrying in {delay}s...")
                await asyncio.sleep(delay)
            else:
                await asyncio.sleep(2 ** attempt)
        except Exception as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)
    return {}  # unreachable


async def _process_batch(
    source_type: str,
    source_config: dict,
    records: list[dict],
    seen_keys: set[str],
) -> int:
    """Process a batch of CKAN records into RegistryRecord rows. Returns count saved."""
    name_field = source_config["name_field"]
    number_field = source_config["number_field"]
    status_field = source_config.get("status_field")
    dedup_fields = source_config.get("deduplicate_by")

    normalize_company_name, _ = _get_matcher()
    rows_to_upsert: list[dict] = []
    for rec in records:
        name = str(rec.get(name_field, "")).strip()
        reg_number = str(rec.get(number_field, "")).strip() or None
        if not name:
            continue

        # Dedup by registration number across all batches
        dedup_key = f"{source_type}|{reg_number}" if reg_number else None
        if dedup_key:
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
        # Extra deduplication for municipal_corporations (same corp appears per board member)
        elif dedup_fields:
            dedup_key = "|".join(str(rec.get(f, "")) for f in dedup_fields)
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

        status = str(rec.get(status_field, "")).strip() if status_field else None
        name_norm = normalize_company_name(name)

        rows_to_upsert.append({
            "source_type": source_type,
            "name": name,
            "name_normalized": name_norm,
            "registration_number": reg_number,
            "status": status or None,
        })

    if not rows_to_upsert:
        return 0

    # Batch upsert into DB — group by registration_number to avoid duplicates
    async with bg_session_factory() as session:
        saved = 0
        # Collect existing reg numbers in one query for the whole batch
        reg_numbers = [r["registration_number"] for r in rows_to_upsert if r["registration_number"]]
        existing_by_reg: dict[str, RegistryRecord] = {}
        if reg_numbers:
            # Query in chunks of 500 to avoid overly large IN clauses
            for i in range(0, len(reg_numbers), 500):
                chunk = reg_numbers[i:i+500]
                result = await session.execute(
                    select(RegistryRecord).where(and_(
                        RegistryRecord.source_type == source_type,
                        RegistryRecord.registration_number.in_(chunk),
                    ))
                )
                for rec in result.scalars().all():
                    existing_by_reg[rec.registration_number] = rec

        for row in rows_to_upsert:
            existing = existing_by_reg.get(row["registration_number"]) if row["registration_number"] else None
            if existing:
                existing.name = row["name"]
                existing.name_normalized = row["name_normalized"]
                existing.status = row["status"]
                # updated_at handled automatically by onupdate=func.now()
            else:
                session.add(RegistryRecord(**row))
            saved += 1

        await session.commit()
        gc.collect()
        return saved


async def _get_or_create_sync_status(session, source_type: str, status: str) -> RegistrySyncStatus:
    """Get or create a RegistrySyncStatus row."""
    result = await session.execute(
        select(RegistrySyncStatus).where(RegistrySyncStatus.source_type == source_type)
    )
    row = result.scalars().first()
    if not row:
        row = RegistrySyncStatus(source_type=source_type, sync_status=status)
        session.add(row)
    else:
        row.sync_status = status
    return row


async def run_all_registry_syncs():
    """Sync all registry sources sequentially. Background task."""
    for source_type in REGISTRY_SOURCES:
        try:
            await run_registry_sync(source_type)
        except Exception as e:
            logger.error(f"Sync failed for {source_type}: {e}", exc_info=True)
            # Continue with next source even if one fails


# ── Entity matching ──────────────────────────────────────────────────────


async def match_entity_against_registry(
    session, entity_type: str, entity_name: str, entity_id: str,
) -> dict | None:
    """Try to match an entity against registry records by name similarity.

    Returns match info dict or None if no match found.
    """
    if not entity_name:
        return None

    # Determine which source_types to search
    if entity_type == "company":
        source_types = COMPANY_SOURCE_TYPES
    elif entity_type == "association":
        source_types = ASSOCIATION_SOURCE_TYPES
    else:
        return None

    normalize_company_name, match_score = _get_matcher()
    name_norm = normalize_company_name(entity_name)
    threshold = settings.registry_match_threshold

    # Step 1: Try exact match on name_normalized
    result = await session.execute(
        select(RegistryRecord).where(and_(
            RegistryRecord.source_type.in_(source_types),
            RegistryRecord.name_normalized == name_norm,
        )).limit(1)
    )
    exact = result.scalars().first()
    if exact:
        return await _apply_match(session, entity_type, entity_id, exact, 1.0)

    # Step 2: Prefix filter — first 3 chars of normalized name
    if len(name_norm) < 2:
        return None

    prefix = name_norm[:3]
    result = await session.execute(
        select(RegistryRecord).where(and_(
            RegistryRecord.source_type.in_(source_types),
            RegistryRecord.name_normalized.like(f"{prefix}%"),
        )).limit(1000)
    )
    candidates = result.scalars().all()

    if not candidates:
        return None

    # Step 3: Fuzzy match against candidates
    best_record = None
    best_score = 0.0
    for rec in candidates:
        score = match_score(entity_name, rec.name)
        if score > best_score:
            best_score = score
            best_record = rec

    if best_record and best_score >= threshold:
        return await _apply_match(session, entity_type, entity_id, best_record, best_score)

    return None


async def _apply_match(
    session, entity_type: str, entity_id: str, record: RegistryRecord, score: float,
) -> dict:
    """Apply a registry match to an entity — update registration_number, confidence, FK."""
    if entity_type == "company":
        result = await session.execute(
            select(Company).where(Company.id == entity_id)
        )
        entity = result.scalars().first()
        if entity:
            entity.registration_number = record.registration_number
            entity.match_confidence = score
            entity.registry_record_id = record.id
    elif entity_type == "association":
        result = await session.execute(
            select(Association).where(Association.id == entity_id)
        )
        entity = result.scalars().first()
        if entity:
            entity.registration_number = record.registration_number
            entity.match_confidence = score
            entity.registry_record_id = record.id

    match_info = {
        "registry_record_id": record.id,
        "registration_number": record.registration_number,
        "registry_name": record.name,
        "source_type": record.source_type,
        "score": score,
    }
    logger.debug(
        f"Matched {entity_type} '{entity_id}' → registry '{record.name}' "
        f"(#{record.registration_number}, score={score:.2f})"
    )
    return match_info


# ── Batch matching (retroactive) ─────────────────────────────────────────


async def match_all_unmatched():
    """Match all existing entities that have no registration_number against the registry.

    Background task with progress polling via _registry_match_state.
    """
    global _registry_match_state

    _registry_match_state.update({
        "running": True,
        "total": 0,
        "processed": 0,
        "matched": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": now_israel().isoformat(),
        "finished_at": None,
    })

    try:
        # Gather unmatched entities
        async with bg_session_factory() as session:
            companies = (await session.execute(
                select(Company.id, Company.name_hebrew).where(
                    Company.registration_number.is_(None)
                )
            )).all()
            associations = (await session.execute(
                select(Association.id, Association.name_hebrew).where(
                    Association.registration_number.is_(None)
                )
            )).all()

        entities = [
            ("company", eid, name) for eid, name in companies
        ] + [
            ("association", eid, name) for eid, name in associations
        ]
        _registry_match_state["total"] = len(entities)
        logger.info(f"Matching {len(entities)} unmatched entities against registry")

        for entity_type, entity_id, entity_name in entities:
            try:
                async with bg_session_factory() as session:
                    match = await match_entity_against_registry(
                        session, entity_type, entity_name, entity_id
                    )
                    if match:
                        _registry_match_state["matched"] += 1
                    await session.commit()
            except Exception as e:
                _registry_match_state["errors"] += 1
                if len(_registry_match_state["error_messages"]) < 20:
                    _registry_match_state["error_messages"].append(
                        f"{entity_type} {entity_id}: {e}"
                    )

            _registry_match_state["processed"] += 1

            if _registry_match_state["processed"] % 50 == 0:
                gc.collect()

    except Exception as e:
        logger.error(f"Batch matching failed: {e}", exc_info=True)
        _registry_match_state["errors"] += 1
        _registry_match_state["error_messages"].append(f"Fatal: {e}")
    finally:
        _registry_match_state["running"] = False
        _registry_match_state["finished_at"] = now_israel().isoformat()
