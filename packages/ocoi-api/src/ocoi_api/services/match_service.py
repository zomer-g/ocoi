"""Background scan job + merge primitive for the entity-duplicate
detection flow.

Why a separate service module: the scan loop can produce thousands of
proposals over several minutes, so it runs as a background task with its
own polled progress dict (mirroring ``import_service._import_state``).
The merge primitive is in here too so both the proposal-approval endpoint
and the manual /admin/entities/merge endpoint use the same code path.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Iterable

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_common.timezone import now_israel
from ocoi_db.crud import _get_aliases  # already JSON-aware
from ocoi_db.engine import async_session_factory, bg_session_factory
from ocoi_db.models import (
    Association,
    Company,
    EntityMatchProposal,
    EntityRelationship,
    Person,
)
from ocoi_matcher.hebrew_names import blocking_key, similarity

logger = logging.getLogger("ocoi.api.matching")


# ── Type → model + scan kind ───────────────────────────────────────────

_SCAN_MODELS = {
    "person": (Person, "person"),
    "company": (Company, "company"),
    "association": (Association, "association"),
}


# ── Scan-progress state (polled by the admin UI) ───────────────────────

_match_state: dict = {
    "running": False,
    "phase": None,           # "loading" / "comparing" / "writing"
    "scanned_entities": 0,
    "total_entities": 0,
    "candidate_pairs": 0,    # pairs scored
    "proposals_written": 0,
    "duplicates_skipped": 0, # already-existing proposals
    "errors": 0,
    "started_at": None,
    "finished_at": None,
    "current_kind": None,    # last entity_type we worked on
}


def get_match_status() -> dict:
    return dict(_match_state)


def reset_match_state() -> None:
    global _match_state
    _match_state.update({
        "running": False,
        "phase": None,
        "scanned_entities": 0,
        "total_entities": 0,
        "candidate_pairs": 0,
        "proposals_written": 0,
        "duplicates_skipped": 0,
        "errors": 0,
        "started_at": None,
        "finished_at": None,
        "current_kind": None,
    })


def try_claim_scan() -> bool:
    """Atomic 'reserve the scan slot' — same idea as
    ``import_service.try_claim_import``: closes the race between the
    trigger endpoint returning and the background task actually
    flipping the running flag."""
    global _match_state
    if _match_state.get("running"):
        return False
    _match_state.update({
        "running": True,
        "phase": "loading",
        "scanned_entities": 0,
        "total_entities": 0,
        "candidate_pairs": 0,
        "proposals_written": 0,
        "duplicates_skipped": 0,
        "errors": 0,
        "started_at": now_israel().isoformat(),
        "finished_at": None,
        "current_kind": None,
    })
    return True


# ── Scan ───────────────────────────────────────────────────────────────


SCORE_THRESHOLD = 0.85  # only write proposals at-or-above this confidence


async def run_duplicate_scan(
    entity_kinds: Iterable[str] = ("person", "company", "association"),
) -> dict:
    """Scan the given entity tables for likely duplicates and write a
    ``EntityMatchProposal`` row for every candidate pair scoring at or
    above ``SCORE_THRESHOLD``.

    The endpoint that calls this is expected to have already claimed the
    scan slot via ``try_claim_scan()``. If not, we claim it ourselves so
    the function is still safe to call directly (e.g. from a future
    cron job).
    """
    global _match_state
    if not _match_state["running"]:
        if not try_claim_scan():
            return {"status": "error", "message": "Scan already running"}

    try:
        for kind in entity_kinds:
            if kind not in _SCAN_MODELS:
                continue
            _match_state["current_kind"] = kind
            try:
                await _scan_one_kind(kind)
            except Exception as e:  # noqa: BLE001 — surface in state dict
                _match_state["errors"] += 1
                logger.exception("duplicate scan failed for %s: %s", kind, e)
    finally:
        _match_state["running"] = False
        _match_state["phase"] = None
        _match_state["finished_at"] = now_israel().isoformat()

    return get_match_status()


async def _scan_one_kind(kind: str) -> None:
    """Load all rows for one entity kind, bucket them by blocking key,
    score every intra-bucket pair, and write the high-confidence ones
    as ``pending`` proposals (skipping any pair that already has a
    proposal, regardless of outcome — we never re-suggest an
    already-reviewed match)."""
    model, _ = _SCAN_MODELS[kind]

    # 1. Load (id, name, aliases, hidden) for every row, skip hidden.
    async with async_session_factory() as session:
        rows_q = await session.execute(
            select(model.id, model.name_hebrew, model.aliases)
            .where(model.hidden.is_(False))
        )
        rows = [(str(r[0]), r[1] or "", r[2]) for r in rows_q.fetchall()]

    _match_state["total_entities"] += len(rows)
    _match_state["phase"] = "comparing"

    # 2. Bucket by blocking key. Each bucket is a list of (id, name,
    #    aliases) tuples we'll compare pair-wise.
    buckets: dict[str, list[tuple[str, str, list[str]]]] = {}
    for rid, name, aliases in rows:
        names_to_index = [name] + (_safe_parse_aliases(aliases) or [])
        for n in names_to_index:
            key = blocking_key(n, kind=kind if kind != "person" else "person")
            if not key:
                continue
            buckets.setdefault(key, []).append((rid, name, names_to_index))

    # 3. Pre-load existing proposal pairs so the scan is idempotent —
    #    we never re-write a pair that's already pending/approved/etc.
    async with async_session_factory() as session:
        existing_q = await session.execute(
            select(
                EntityMatchProposal.entity_type,
                EntityMatchProposal.entity_id,
                EntityMatchProposal.target_type,
                EntityMatchProposal.target_id,
            ).where(
                EntityMatchProposal.proposal_kind == "duplicate",
                EntityMatchProposal.entity_type == kind,
            )
        )
        existing_pairs: set[frozenset[tuple[str, str]]] = set()
        for etype, eid, ttype, tid in existing_q.fetchall():
            existing_pairs.add(
                frozenset(((str(etype), str(eid)), (str(ttype), str(tid))))
            )

    # 4. Walk each bucket, score every unordered pair.
    proposals_to_insert: list[dict] = []
    scanned = 0
    for key, members in buckets.items():
        if len(members) < 2:
            continue
        seen_within_bucket: set[frozenset[str]] = set()
        for i in range(len(members)):
            id_a, name_a, aliases_a = members[i]
            for j in range(i + 1, len(members)):
                id_b, name_b, aliases_b = members[j]
                if id_a == id_b:
                    continue
                # Dedup pairs that appear in multiple alias buckets.
                pair_key = frozenset((id_a, id_b))
                if pair_key in seen_within_bucket:
                    continue
                seen_within_bucket.add(pair_key)

                pair_full = frozenset(((kind, id_a), (kind, id_b)))
                if pair_full in existing_pairs:
                    _match_state["duplicates_skipped"] += 1
                    continue

                # Score the best (name × name) pairing across both
                # entities' full alias sets so a near-miss via an alias
                # still surfaces.
                best_score = 0.0
                best_reasons: list[str] = []
                for na in aliases_a:
                    for nb in aliases_b:
                        s, reasons = similarity(na, nb, kind=kind)
                        if s > best_score:
                            best_score = s
                            best_reasons = reasons
                            if best_score >= 0.999:
                                break
                    if best_score >= 0.999:
                        break

                _match_state["candidate_pairs"] += 1
                if best_score < SCORE_THRESHOLD:
                    continue

                proposals_to_insert.append({
                    "proposal_kind": "duplicate",
                    "entity_type": kind,
                    "entity_id": id_a,
                    "target_kind": "entity",
                    "target_type": kind,
                    "target_id": id_b,
                    "score": best_score,
                    "reasons": json.dumps(best_reasons, ensure_ascii=False),
                    "status": "pending",
                })

            scanned += 1
            _match_state["scanned_entities"] += 1

    # 5. Write proposals in chunks so a bug in one row doesn't lose
    #    the rest.
    _match_state["phase"] = "writing"
    CHUNK = 200
    async with async_session_factory() as session:
        for i in range(0, len(proposals_to_insert), CHUNK):
            batch = proposals_to_insert[i : i + CHUNK]
            try:
                session.add_all([EntityMatchProposal(**p) for p in batch])
                await session.commit()
                _match_state["proposals_written"] += len(batch)
            except Exception as e:  # noqa: BLE001
                _match_state["errors"] += len(batch)
                logger.warning("duplicate proposal batch failed: %s", e)
                try:
                    await session.rollback()
                except Exception:
                    pass


def _safe_parse_aliases(raw) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [s for s in parsed if isinstance(s, str)]


# ── Merge primitive ────────────────────────────────────────────────────


async def merge_entities(
    session: AsyncSession,
    entity_type: str,
    keep_id: str,
    merge_id: str,
) -> dict:
    """Move every relationship from ``merge_id`` to ``keep_id``, fold the
    merge entity's name + aliases into the keep entity's alias list, and
    delete the merge entity row. All in one DB transaction managed by
    the caller.

    Returns a small summary dict for the API response.

    Raises ``ValueError`` if the entity type is unknown or the two ids
    refer to the same row.
    """
    if entity_type not in _SCAN_MODELS:
        raise ValueError(f"unsupported entity type: {entity_type}")
    if keep_id == merge_id:
        raise ValueError("keep_id and merge_id must differ")

    model, _ = _SCAN_MODELS[entity_type]

    keep = await session.get(model, keep_id)
    merge = await session.get(model, merge_id)
    if not keep:
        raise ValueError("keep entity not found")
    if not merge:
        raise ValueError("merge entity not found")

    # Move SOURCE-side relationships
    src_result = await session.execute(
        update(EntityRelationship)
        .where(
            EntityRelationship.source_entity_type == entity_type,
            EntityRelationship.source_entity_id == merge_id,
        )
        .values(source_entity_id=keep_id)
    )
    moved_src = src_result.rowcount or 0

    # Move TARGET-side relationships
    tgt_result = await session.execute(
        update(EntityRelationship)
        .where(
            EntityRelationship.target_entity_type == entity_type,
            EntityRelationship.target_entity_id == merge_id,
        )
        .values(target_entity_id=keep_id)
    )
    moved_tgt = tgt_result.rowcount or 0

    # Some rows may now point person→person back at themselves —
    # delete those self-loops so the graph doesn't render nonsense.
    self_loop_result = await session.execute(
        EntityRelationship.__table__.delete().where(
            EntityRelationship.source_entity_type == entity_type,
            EntityRelationship.target_entity_type == entity_type,
            EntityRelationship.source_entity_id == keep_id,
            EntityRelationship.target_entity_id == keep_id,
        )
    )
    self_loops_dropped = self_loop_result.rowcount or 0

    # Fold merge's name + aliases into keep's alias list (dedup).
    existing_aliases = set(_get_aliases(keep))
    merged_in: list[str] = []
    candidates = [merge.name_hebrew or ""] + list(_get_aliases(merge))
    for cand in candidates:
        cand = (cand or "").strip()
        if not cand or cand == (keep.name_hebrew or "").strip():
            continue
        if cand in existing_aliases:
            continue
        existing_aliases.add(cand)
        merged_in.append(cand)
    keep.aliases = json.dumps(sorted(existing_aliases), ensure_ascii=False) if existing_aliases else None

    # Drop the merge entity row.
    await session.delete(merge)
    await session.flush()

    return {
        "kept_id": str(keep.id),
        "merged_id": str(merge_id),
        "moved_source_rels": moved_src,
        "moved_target_rels": moved_tgt,
        "self_loops_removed": self_loops_dropped,
        "aliases_added": merged_in,
    }


# ── Pretty payloads for the admin UI ───────────────────────────────────


async def proposal_to_dict(
    session: AsyncSession,
    proposal: EntityMatchProposal,
) -> dict:
    """Resolve display names for both sides so the admin UI doesn't need
    extra fetches per row."""
    try:
        reasons = json.loads(proposal.reasons) if proposal.reasons else []
    except Exception:
        reasons = []
    if not isinstance(reasons, list):
        reasons = []

    left = await _entity_summary(session, proposal.entity_type, proposal.entity_id)

    if proposal.target_kind == "entity":
        right = await _entity_summary(session, proposal.target_type, proposal.target_id)
    else:
        right = {
            "id": str(proposal.target_id),
            "type": proposal.target_type,
            "name": "",
            "kind": "registry",
        }

    reviewer_name = None
    if proposal.reviewed_by:
        from ocoi_db.models import User
        reviewer = await session.get(User, proposal.reviewed_by)
        if reviewer:
            reviewer_name = reviewer.name or reviewer.email

    return {
        "id": str(proposal.id),
        "proposal_kind": proposal.proposal_kind,
        "status": proposal.status,
        "score": float(proposal.score or 0.0),
        "reasons": [str(r) for r in reasons],
        "left": left,
        "right": right,
        "reviewed_by_name": reviewer_name,
        "reviewed_at": proposal.reviewed_at.isoformat() if proposal.reviewed_at else None,
        "created_at": proposal.created_at.isoformat() if proposal.created_at else None,
    }


async def _entity_summary(session: AsyncSession, etype: str, eid: str) -> dict:
    info = _SCAN_MODELS.get(etype)
    if not info:
        return {"id": str(eid), "type": etype, "name": ""}
    model, _ = info
    row = await session.get(model, eid)
    if not row:
        return {"id": str(eid), "type": etype, "name": "(נמחק)"}
    summary = {
        "id": str(row.id),
        "type": etype,
        "name": row.name_hebrew or "",
        "aliases": _get_aliases(row),
    }
    if etype == "person":
        summary["title"] = getattr(row, "title", None)
        summary["position"] = getattr(row, "position", None)
        summary["ministry"] = getattr(row, "ministry", None)
    return summary
