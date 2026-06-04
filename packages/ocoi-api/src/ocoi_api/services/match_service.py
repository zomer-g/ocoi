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

from sqlalchemy import and_, func, or_, select, update
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

    # ── Dedup before the UPDATEs ──
    # The compound unique index `ix_rel_compound` covers
    #   (source_type, source_id, target_type, target_id,
    #    relationship_type, document_id)
    # so the moment two duplicates share an edge (which they routinely do
    # — that's WHY they're duplicates), a naïve UPDATE collides with the
    # canonical's existing row and the whole transaction aborts.
    #
    # We drop the about-to-collide rows from the merge side first, so the
    # subsequent UPDATEs only have to relocate truly-unique edges. The
    # canonical's copy stays, so the relationship is preserved — just
    # de-duplicated.
    er = EntityRelationship.__table__
    # Aliased SELECT-for-delete: bind each alias to a local variable so
    # the same alias object is reused in JOIN and WHERE — otherwise
    # SQLAlchemy generates two distinct subqueries that don't correlate.
    keep_src_alias = er.alias("k_src")
    src_collisions = (await session.execute(
        select(er.c.id)
        .select_from(
            er.join(
                keep_src_alias,
                and_(
                    er.c.source_entity_type == keep_src_alias.c.source_entity_type,
                    er.c.target_entity_type == keep_src_alias.c.target_entity_type,
                    er.c.target_entity_id == keep_src_alias.c.target_entity_id,
                    er.c.relationship_type == keep_src_alias.c.relationship_type,
                    er.c.document_id == keep_src_alias.c.document_id,
                ),
            )
        )
        .where(
            er.c.source_entity_type == entity_type,
            er.c.source_entity_id == merge_id,
            keep_src_alias.c.source_entity_id == keep_id,
        )
    )).scalars().all()
    keep_tgt_alias = er.alias("k_tgt")
    tgt_collisions = (await session.execute(
        select(er.c.id)
        .select_from(
            er.join(
                keep_tgt_alias,
                and_(
                    er.c.source_entity_type == keep_tgt_alias.c.source_entity_type,
                    er.c.source_entity_id == keep_tgt_alias.c.source_entity_id,
                    er.c.target_entity_type == keep_tgt_alias.c.target_entity_type,
                    er.c.relationship_type == keep_tgt_alias.c.relationship_type,
                    er.c.document_id == keep_tgt_alias.c.document_id,
                ),
            )
        )
        .where(
            er.c.target_entity_type == entity_type,
            er.c.target_entity_id == merge_id,
            keep_tgt_alias.c.target_entity_id == keep_id,
        )
    )).scalars().all()
    collision_ids = set(src_collisions) | set(tgt_collisions)
    if collision_ids:
        await session.execute(
            er.delete().where(er.c.id.in_(list(collision_ids)))
        )

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
        "duplicate_edges_removed": len(collision_ids),
    }


async def merge_entities_cross_type(
    session: AsyncSession,
    *,
    keep_type: str,
    keep_id: str,
    merge_type: str,
    merge_id: str,
) -> dict:
    """Cross-type merge: fold ``merge`` (any entity type) into ``keep``
    (any entity type). The classic case is the same legal entity
    appearing as both a 'company' AND an 'association' because the
    source documents misclassified it ("עמותת הצלחה" recorded as a
    company plus "עמותת הצלחה" recorded as an association).

    The relationship rows that touched the merge side are rewritten
    with both ``entity_type`` AND ``entity_id`` swapped to the keep
    side, and the merge entity is then deleted from its own table.
    Aliases (and the merge entity's display name) fold into the keep
    entity's alias list so the public surface still answers searches
    for the old name.
    """
    if keep_type not in _SCAN_MODELS or merge_type not in _SCAN_MODELS:
        raise ValueError(f"unsupported entity types: {keep_type}/{merge_type}")
    if keep_type == merge_type and keep_id == merge_id:
        raise ValueError("keep and merge identify the same entity")
    keep_id = str(keep_id)
    merge_id = str(merge_id)

    keep_model = _SCAN_MODELS[keep_type][0]
    merge_model = _SCAN_MODELS[merge_type][0]

    keep = await session.get(keep_model, keep_id)
    merge = await session.get(merge_model, merge_id)
    if not keep:
        raise ValueError("keep entity not found")
    if not merge:
        raise ValueError("merge entity not found")

    er = EntityRelationship.__table__

    # Dedup colliding edges BEFORE the UPDATE. Same reasoning as
    # merge_entities() — the compound unique index will abort the
    # transaction the moment a rewritten row collides with a keep-side
    # row sharing the same (compound-key fields).
    #
    # For cross-type, the keep side compares on (keep_type, keep_id) and
    # the merge side has (merge_type, merge_id). After the rewrite, the
    # merge-side row becomes (keep_type, keep_id, …); collision iff
    # there's an existing keep-side row with the same target compound +
    # rel_type + doc_id.
    keep_src_alias = er.alias("xk_src")
    src_collisions = (await session.execute(
        select(er.c.id)
        .select_from(
            er.join(
                keep_src_alias,
                and_(
                    er.c.target_entity_type == keep_src_alias.c.target_entity_type,
                    er.c.target_entity_id == keep_src_alias.c.target_entity_id,
                    er.c.relationship_type == keep_src_alias.c.relationship_type,
                    er.c.document_id == keep_src_alias.c.document_id,
                ),
            )
        )
        .where(
            er.c.source_entity_type == merge_type,
            er.c.source_entity_id == merge_id,
            keep_src_alias.c.source_entity_type == keep_type,
            keep_src_alias.c.source_entity_id == keep_id,
        )
    )).scalars().all()

    keep_tgt_alias = er.alias("xk_tgt")
    tgt_collisions = (await session.execute(
        select(er.c.id)
        .select_from(
            er.join(
                keep_tgt_alias,
                and_(
                    er.c.source_entity_type == keep_tgt_alias.c.source_entity_type,
                    er.c.source_entity_id == keep_tgt_alias.c.source_entity_id,
                    er.c.relationship_type == keep_tgt_alias.c.relationship_type,
                    er.c.document_id == keep_tgt_alias.c.document_id,
                ),
            )
        )
        .where(
            er.c.target_entity_type == merge_type,
            er.c.target_entity_id == merge_id,
            keep_tgt_alias.c.target_entity_type == keep_type,
            keep_tgt_alias.c.target_entity_id == keep_id,
        )
    )).scalars().all()

    collision_ids = set(src_collisions) | set(tgt_collisions)
    if collision_ids:
        await session.execute(
            er.delete().where(er.c.id.in_(list(collision_ids)))
        )

    # Move SOURCE-side: rewrite both the type and the id.
    src_result = await session.execute(
        update(EntityRelationship)
        .where(
            EntityRelationship.source_entity_type == merge_type,
            EntityRelationship.source_entity_id == merge_id,
        )
        .values(source_entity_type=keep_type, source_entity_id=keep_id)
    )
    moved_src = src_result.rowcount or 0

    # Move TARGET-side.
    tgt_result = await session.execute(
        update(EntityRelationship)
        .where(
            EntityRelationship.target_entity_type == merge_type,
            EntityRelationship.target_entity_id == merge_id,
        )
        .values(target_entity_type=keep_type, target_entity_id=keep_id)
    )
    moved_tgt = tgt_result.rowcount or 0

    # Self-loop cleanup — any row that now points keep→keep gets dropped.
    self_loop_result = await session.execute(
        er.delete().where(
            EntityRelationship.source_entity_type == keep_type,
            EntityRelationship.target_entity_type == keep_type,
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
    keep.aliases = (
        json.dumps(sorted(existing_aliases), ensure_ascii=False)
        if existing_aliases else None
    )

    # Drop the merge entity row from its own table.
    await session.delete(merge)
    await session.flush()

    return {
        "kept_type": keep_type,
        "kept_id": str(keep.id),
        "merged_type": merge_type,
        "merged_id": merge_id,
        "moved_source_rels": moved_src,
        "moved_target_rels": moved_tgt,
        "self_loops_removed": self_loops_dropped,
        "duplicate_edges_removed": len(collision_ids),
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


# ── Cluster view (union-find over pending duplicate proposals) ─────────
#
# The per-pair "approve / reject" flow is fine when there are five rows
# called "משה כחלון" — but breaks down at thirty rows called "הצלחה",
# where the scanner emits C(30,2)=435 proposals all referring to the
# same logical group. ``build_duplicate_clusters`` collapses those
# proposals into connected components so the admin sees one card per
# cluster and merges the whole thing in a single click.


async def build_duplicate_clusters(
    session: AsyncSession,
    *,
    entity_type: str | None = None,
    min_score: float = SCORE_THRESHOLD,
    limit: int | None = None,
    timings: dict | None = None,
) -> list[dict]:
    """Return clusters of entities that share at least one pending
    duplicate proposal of score ≥ ``min_score``.

    Each cluster is the connected component obtained by treating every
    pending proposal as an undirected edge (entity_id, target_id) inside
    the same entity_type. We also pre-pick a recommended ``canonical_id``
    — the entity that should be kept when the cluster is merged — so the
    UI can show "מזג הכל ל-X" without another round-trip.

    The canonical heuristic is:
      1. Most relationships (highest in-degree + out-degree). The entity
         already touching the most edges has the strongest gravity; merging
         the others into it minimises the number of FK updates and is most
         likely the row a human would have picked anyway.
      2. Longest name_hebrew — ties usually mean "X ינר" vs "X" with the
         longer one being the more specific / corrected name.
      3. Lowest id — stable deterministic tiebreaker.
    """
    import time as _time
    _t0 = _time.perf_counter()
    def _mark(label: str) -> None:
        if timings is not None:
            timings[label] = round((_time.perf_counter() - _t0) * 1000, 1)

    # 1. Pull every pending duplicate proposal at-or-above the threshold.
    where = [
        EntityMatchProposal.proposal_kind == "duplicate",
        EntityMatchProposal.status == "pending",
        EntityMatchProposal.score >= min_score,
    ]
    if entity_type:
        where.append(EntityMatchProposal.entity_type == entity_type)
    rows = (await session.execute(
        select(
            EntityMatchProposal.id,
            EntityMatchProposal.entity_type,
            EntityMatchProposal.entity_id,
            EntityMatchProposal.target_id,
            EntityMatchProposal.score,
            EntityMatchProposal.reasons,
        ).where(*where)
    )).all()
    _mark("load_proposals")
    if not rows:
        return []

    # 2. Union-find over (entity_type, entity_id) keys.
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(node: tuple[str, str]) -> tuple[str, str]:
        # Path compression so a long chain collapses to O(1) on the next
        # walk; safe because the dict is rebuilt per call.
        root = node
        while parent.get(root, root) != root:
            root = parent[root]
        cur = node
        while parent.get(cur, cur) != root:
            nxt = parent[cur]
            parent[cur] = root
            cur = nxt
        return root

    def union(a: tuple[str, str], b: tuple[str, str]) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    proposals_by_root: dict[tuple[str, str], list[dict]] = {}
    for pid, etype, eid, tid, score, reasons_raw in rows:
        key_a = (str(etype), str(eid))
        key_b = (str(etype), str(tid))
        parent.setdefault(key_a, key_a)
        parent.setdefault(key_b, key_b)
        union(key_a, key_b)

    # 3. Re-group all proposals by their cluster root.
    components: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for pid, etype, eid, tid, score, reasons_raw in rows:
        key_a = (str(etype), str(eid))
        root = find(key_a)
        comp = components.setdefault(root, set())
        comp.add(key_a)
        comp.add((str(etype), str(tid)))
        try:
            reasons = json.loads(reasons_raw) if reasons_raw else []
        except Exception:
            reasons = []
        proposals_by_root.setdefault(root, []).append({
            "id": str(pid),
            "left_id": str(eid),
            "right_id": str(tid),
            "score": float(score or 0.0),
            "reasons": reasons if isinstance(reasons, list) else [],
        })

    # 4. Hydrate each cluster with display data + pick the canonical.
    # Performance: with ~1,500+ pending proposals the previous "one query
    # per cluster + one per member" pattern was 5,000+ round-trips and
    # blew the request timeout. We now batch:
    #   - For each entity_type, do ONE SELECT loading all entities at once.
    #   - For each entity_type, do TWO GROUP-BY SELECTs (source + target)
    #     yielding connection counts for every member in a single round-trip.
    # Net: ~6 DB queries total regardless of cluster count.

    # If a `limit` is set, sort components by size and keep only the top
    # N BEFORE hydration so we don't waste DB round-trips on entities the
    # response is going to discard anyway. With 1,500+ pending proposals
    # the difference is ~12s vs ~0.3s wall-clock on Render's free tier.
    filtered_pairs: list[tuple[tuple[str, str], set[tuple[str, str]]]] = [
        (root, members) for root, members in components.items() if len(members) >= 2
    ]
    if limit is not None and limit > 0:
        filtered_pairs.sort(
            key=lambda kv: (-len(kv[1]), kv[0][0], kv[0][1]),
        )
        filtered_pairs = filtered_pairs[:limit]
    components_filtered: dict[tuple[str, str], set[tuple[str, str]]] = dict(filtered_pairs)
    _mark("union_find_and_filter")
    if timings is not None:
        timings["components_kept"] = len(components_filtered)
        timings["biggest_cluster_size"] = (
            max((len(v) for v in components_filtered.values()), default=0)
        )

    # Group member ids by entity_type so the batch queries can hit a
    # single table at a time.
    ids_by_type: dict[str, set[str]] = {}
    for _root, member_keys in components_filtered.items():
        for etype, eid in member_keys:
            ids_by_type.setdefault(etype, set()).add(eid)
    if timings is not None:
        timings["total_ids_to_hydrate"] = sum(len(s) for s in ids_by_type.values())

    entities_by_type: dict[str, dict[str, dict]] = {}
    counts_by_type: dict[str, dict[str, int]] = {}

    for etype, id_set in ids_by_type.items():
        if etype not in _SCAN_MODELS:
            continue
        id_list = list(id_set)
        model, _ = _SCAN_MODELS[etype]

        # Hydrate every entity in ONE query.
        cols = [model.id, model.name_hebrew, model.aliases]
        if etype == "person":
            cols.extend([model.title, model.position, model.ministry])
        rows = (await session.execute(
            select(*cols).where(model.id.in_(id_list))
        )).all()
        ent_map: dict[str, dict] = {}
        for r in rows:
            row_dict = r._mapping
            eid = str(row_dict["id"])
            summary: dict = {
                "id": eid,
                "type": etype,
                "name": row_dict.get("name_hebrew") or "",
                "aliases": _safe_parse_aliases(row_dict.get("aliases")),
            }
            if etype == "person":
                summary["title"] = row_dict.get("title")
                summary["position"] = row_dict.get("position")
                summary["ministry"] = row_dict.get("ministry")
            ent_map[eid] = summary
        # For ids that disappeared between proposal-write and now, leave
        # a stub so the cluster card still shows "(נמחק)".
        for eid in id_list:
            if eid not in ent_map:
                ent_map[eid] = {"id": eid, "type": etype, "name": "(נמחק)", "aliases": []}
        entities_by_type[etype] = ent_map

        # Connection-count GROUP BYs — one for the source side, one for
        # the target side. Merge in Python.
        src_rows = (await session.execute(
            select(
                EntityRelationship.source_entity_id,
                func.count(EntityRelationship.id),
            )
            .where(
                EntityRelationship.source_entity_type == etype,
                EntityRelationship.source_entity_id.in_(id_list),
            )
            .group_by(EntityRelationship.source_entity_id)
        )).all()
        tgt_rows = (await session.execute(
            select(
                EntityRelationship.target_entity_id,
                func.count(EntityRelationship.id),
            )
            .where(
                EntityRelationship.target_entity_type == etype,
                EntityRelationship.target_entity_id.in_(id_list),
            )
            .group_by(EntityRelationship.target_entity_id)
        )).all()
        counts: dict[str, int] = {}
        for eid, n in src_rows:
            counts[str(eid)] = counts.get(str(eid), 0) + int(n or 0)
        for eid, n in tgt_rows:
            counts[str(eid)] = counts.get(str(eid), 0) + int(n or 0)
        counts_by_type[etype] = counts
        _mark(f"hydrate_{etype}")

    out: list[dict] = []
    for root, member_keys in components_filtered.items():
        cluster_type = root[0]
        member_ids = [eid for (_etype, eid) in member_keys]

        ent_map = entities_by_type.get(cluster_type, {})
        counts = counts_by_type.get(cluster_type, {})

        members = []
        for mid in member_ids:
            summary = dict(ent_map.get(mid, {
                "id": mid, "type": cluster_type, "name": "(נמחק)", "aliases": [],
            }))
            summary["connection_count"] = counts.get(mid, 0)
            members.append(summary)

        # Sort key per heuristic (above). The first element is canonical.
        members.sort(key=lambda m: (
            -m.get("connection_count", 0),
            -len((m.get("name") or "")),
            m["id"],
        ))
        canonical_id = members[0]["id"]

        out.append({
            "entity_type": cluster_type,
            "size": len(members),
            "canonical_id": canonical_id,
            "members": members,
            "proposals": proposals_by_root.get(root, []),
        })

    # Largest clusters first — those are the biggest cleanup wins.
    out.sort(key=lambda c: (-c["size"], c["entity_type"], c["canonical_id"]))
    return out


async def merge_cluster(
    session: AsyncSession,
    *,
    entity_type: str,
    canonical_id: str,
    member_ids: list[str],
    reviewer_id: str | None = None,
) -> dict:
    """Collapse an entire duplicate cluster into ``canonical_id`` in one
    transaction (managed by the caller). Runs ``merge_entities`` for
    every member ≠ canonical, then marks all pending proposals that
    reference any cluster member as ``approved`` with ``reviewer_id``.

    Returns a summary dict the admin UI can display."""
    if entity_type not in _SCAN_MODELS:
        raise ValueError(f"unsupported entity type: {entity_type}")
    canonical_id = str(canonical_id)
    member_ids = [str(m) for m in member_ids if str(m) != canonical_id]
    if not member_ids:
        return {
            "canonical_id": canonical_id,
            "merged_count": 0,
            "moved_source_rels": 0,
            "moved_target_rels": 0,
            "self_loops_removed": 0,
            "aliases_added": [],
            "proposals_approved": 0,
        }

    total_src = 0
    total_tgt = 0
    total_loops = 0
    aliases_added: list[str] = []
    merged_ids: list[str] = []

    for mid in member_ids:
        try:
            summary = await merge_entities(session, entity_type, canonical_id, mid)
        except ValueError:
            # Entity may have been merged in a prior pass (e.g. user ran
            # merge twice). Skip silently rather than aborting the rest
            # of the cluster.
            continue
        total_src += int(summary.get("moved_source_rels") or 0)
        total_tgt += int(summary.get("moved_target_rels") or 0)
        total_loops += int(summary.get("self_loops_removed") or 0)
        aliases_added.extend(summary.get("aliases_added") or [])
        merged_ids.append(mid)

    # Mark every still-pending proposal that touched ANY member of the
    # cluster as approved. Without this, the merged-away ids would still
    # produce ghost proposals in the queue.
    all_ids = [canonical_id] + member_ids
    approved_q = await session.execute(
        update(EntityMatchProposal)
        .where(
            EntityMatchProposal.proposal_kind == "duplicate",
            EntityMatchProposal.entity_type == entity_type,
            EntityMatchProposal.status == "pending",
            or_(
                EntityMatchProposal.entity_id.in_(all_ids),
                EntityMatchProposal.target_id.in_(all_ids),
            ),
        )
        .values(
            status="approved",
            reviewed_by=reviewer_id,
            reviewed_at=datetime.utcnow(),
        )
    )

    return {
        "canonical_id": canonical_id,
        "merged_count": len(merged_ids),
        "merged_ids": merged_ids,
        "moved_source_rels": total_src,
        "moved_target_rels": total_tgt,
        "self_loops_removed": total_loops,
        "aliases_added": sorted(set(aliases_added)),
        "proposals_approved": int(approved_q.rowcount or 0),
    }
