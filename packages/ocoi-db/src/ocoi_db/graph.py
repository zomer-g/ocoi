"""Graph queries using recursive CTEs on entity_relationships table.

Compatible with both SQLite and PostgreSQL.

All query helpers accept an optional ``excluded_origins`` set so the
public surface can hide edges from specific ``origin_kind`` values
(notably ``mk_expense`` — Knesset constituent-outreach payment imports,
which are useful but visually overwhelm the COI declaration graph by
default). Pass ``None`` or empty to include everything (current
behaviour).
"""

from typing import Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_common.models import ConnectionEdge, EntitySummary, EntityType, SubGraph


def _origin_filter_sql(
    excluded_origins: Iterable[str] | None,
    *,
    alias: str = "r",
) -> str:
    """Return a SQL fragment that must be AND-ed onto the WHERE clause
    of any query against ``entity_relationships``. Empty string when
    nothing's excluded so the caller can splice it unconditionally.

    Uses an expanding bindparam so SQLAlchemy renders ``IN (?,?,...)``
    correctly on both SQLite and PostgreSQL.
    """
    if not excluded_origins:
        return ""
    return f" AND {alias}.origin_kind NOT IN :excluded_origins"


def _bind_excluded(stmt, excluded_origins: Iterable[str] | None):
    """Attach an expanding bindparam for ``excluded_origins`` when the
    caller passed a non-empty set; no-op otherwise."""
    if not excluded_origins:
        return stmt
    return stmt.bindparams(bindparam("excluded_origins", expanding=True))


async def get_neighbors(
    session: AsyncSession,
    entity_id,
    entity_type: str,
    depth: int = 1,
    *,
    excluded_origins: Iterable[str] | None = None,
) -> SubGraph:
    """Get neighboring entities up to `depth` hops away."""
    eid = str(entity_id)
    excluded = list(excluded_origins) if excluded_origins else None
    if depth == 1:
        return await _get_direct_neighbors(session, eid, entity_type, excluded)
    return await _get_recursive_neighbors(session, eid, entity_type, depth, excluded)


async def _get_direct_neighbors(
    session: AsyncSession,
    entity_id: str,
    entity_type: str,
    excluded_origins: list[str] | None = None,
) -> SubGraph:
    extra = _origin_filter_sql(excluded_origins)
    query = text(f"""
        SELECT
            r.source_entity_type, r.source_entity_id,
            r.target_entity_type, r.target_entity_id,
            r.relationship_type, r.details, r.origin_kind, r.verified,
            r.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE ((r.source_entity_type = :etype AND r.source_entity_id = :eid)
            OR (r.target_entity_type = :etype AND r.target_entity_id = :eid))
          {extra}
    """)
    query = _bind_excluded(query, excluded_origins)
    params: dict = {"eid": entity_id, "etype": entity_type}
    if excluded_origins:
        params["excluded_origins"] = excluded_origins
    result = await session.execute(query, params)
    rows = result.fetchall()
    return _build_subgraph_from_rows(rows)


async def _get_recursive_neighbors(
    session: AsyncSession,
    entity_id: str,
    entity_type: str,
    depth: int,
    excluded_origins: list[str] | None = None,
) -> SubGraph:
    """Multi-hop neighbor query using recursive CTE (works on both SQLite and PostgreSQL)."""
    # Filter must apply to BOTH the anchor select AND the recursive step
    # so excluded edges are also unavailable as hop-springboards.
    extra = _origin_filter_sql(excluded_origins)
    query = text(f"""
        WITH RECURSIVE graph_walk AS (
            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details, r.origin_kind, r.verified,
                r.document_id,
                1 AS depth
            FROM entity_relationships r
            WHERE ((r.source_entity_type = :etype AND r.source_entity_id = :eid)
                OR (r.target_entity_type = :etype AND r.target_entity_id = :eid))
              {extra}

            UNION

            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details, r.origin_kind, r.verified,
                r.document_id,
                gw.depth + 1
            FROM entity_relationships r
            JOIN graph_walk gw ON (
                (r.source_entity_type = gw.target_entity_type
                 AND r.source_entity_id = gw.target_entity_id)
                OR
                (r.target_entity_type = gw.source_entity_type
                 AND r.target_entity_id = gw.source_entity_id)
            )
            WHERE gw.depth < :max_depth
              {extra}
        )
        SELECT DISTINCT
            gw.source_entity_type, gw.source_entity_id,
            gw.target_entity_type, gw.target_entity_id,
            gw.relationship_type, gw.details, gw.origin_kind, gw.verified,
            gw.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM graph_walk gw
        LEFT JOIN documents d ON d.id = gw.document_id
    """)
    query = _bind_excluded(query, excluded_origins)
    params: dict = {"eid": entity_id, "etype": entity_type, "max_depth": depth}
    if excluded_origins:
        params["excluded_origins"] = excluded_origins
    result = await session.execute(query, params)
    rows = result.fetchall()
    return _build_subgraph_from_rows(rows)


async def find_path(
    session: AsyncSession,
    from_id,
    from_type: str,
    to_id,
    to_type: str,
    max_hops: int = 4,
) -> SubGraph | None:
    """Find path between two entities. SQLite + PostgreSQL compatible."""
    fid = str(from_id)
    tid = str(to_id)

    query = text("""
        WITH RECURSIVE path_search AS (
            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details, r.origin_kind, r.verified,
                r.document_id,
                1 AS depth
            FROM entity_relationships r
            WHERE (r.source_entity_type = :from_type AND r.source_entity_id = :from_id)
               OR (r.target_entity_type = :from_type AND r.target_entity_id = :from_id)

            UNION

            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details, r.origin_kind, r.verified,
                r.document_id,
                ps.depth + 1
            FROM entity_relationships r
            JOIN path_search ps ON (
                (r.source_entity_type = ps.target_entity_type
                 AND r.source_entity_id = ps.target_entity_id)
                OR
                (r.target_entity_type = ps.source_entity_type
                 AND r.target_entity_id = ps.source_entity_id)
            )
            WHERE ps.depth < :max_hops
        )
        SELECT DISTINCT
            ps.source_entity_type, ps.source_entity_id,
            ps.target_entity_type, ps.target_entity_id,
            ps.relationship_type, ps.details, ps.origin_kind, ps.verified,
            ps.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM path_search ps
        LEFT JOIN documents d ON d.id = ps.document_id
        WHERE (ps.source_entity_type = :to_type AND ps.source_entity_id = :to_id)
           OR (ps.target_entity_type = :to_type AND ps.target_entity_id = :to_id)
        LIMIT 20
    """)
    result = await session.execute(query, {
        "from_id": fid, "from_type": from_type,
        "to_id": tid, "to_type": to_type,
        "max_hops": max_hops,
    })
    rows = result.fetchall()
    if not rows:
        return None
    return _build_subgraph_from_rows(rows)


async def _hidden_id_set(session: AsyncSession, table: str) -> set[str]:
    """Return the set of `id` values flagged hidden in the given entity
    table. Used to skip generic-placeholder rows when picking a showcase
    pair."""
    rows = await session.execute(text(f"SELECT id FROM {table} WHERE hidden = TRUE"))
    return {str(r[0]) for r in rows.fetchall()}


async def find_showcase_pair(
    session: AsyncSession,
    rotation_seed: int = 0,
    *,
    excluded_origins: Iterable[str] | None = None,
) -> SubGraph | None:
    """Find a "two suns" showcase: pick two persons who share at least one
    visible (non-hidden) hub and return the full union of their direct
    neighborhoods. Visually this lays out as two dense stars whose overlap
    sits between them — exactly the kind of picture that explains what
    the project surfaces.

    Hidden / blocklisted entities (generic placeholders like
    "עניינים אישיים") are excluded both as candidate hubs *and* as
    connectors in the resulting graph. If after filtering the chosen pair
    no longer share a visible neighbour, the next candidate hub is tried.

    `rotation_seed` rotates the choice across a list of top candidate hubs:
    callers can pass e.g. today's ordinal to give each day a different
    showcase pair. Same seed → same result, so daily caching is trivial.

    Falls back to a single star (one hub with ≥ 2 persons) and finally to
    a direct person → person edge if no overlapping pair exists.
    """

    # Pre-load hidden id sets per type once. Cheap (single index scan).
    hidden_persons = await _hidden_id_set(session, "persons")
    hidden_companies = await _hidden_id_set(session, "companies")
    hidden_associations = await _hidden_id_set(session, "associations")
    hidden_domains = await _hidden_id_set(session, "domains")
    hidden_by_type: dict[str, set[str]] = {
        "person": hidden_persons,
        "company": hidden_companies,
        "association": hidden_associations,
        "domain": hidden_domains,
    }

    def _is_hidden(etype: str, eid: str) -> bool:
        return eid in hidden_by_type.get(etype, set())

    # ── Pair selection ───────────────────────────────────────────────────
    # Over-fetch top-N hubs, drop hidden ones, then iterate from the
    # rotation seed until we find a hub whose two chosen persons still
    # share at least one VISIBLE neighbour after pruning.
    HUB_CANDIDATES = 20
    HUB_OVER_FETCH = 80
    # When excluded_origins is set we count only the surviving edges, so
    # a hub that owes its top rank to MK-expense rows isn't preferred.
    excluded = list(excluded_origins) if excluded_origins else None
    hub_origin_filter = _origin_filter_sql(excluded, alias="entity_relationships")
    hub_pick_q = text(f"""
        SELECT target_entity_type AS hub_type,
               target_entity_id   AS hub_id,
               COUNT(DISTINCT source_entity_id) AS person_count
        FROM entity_relationships
        WHERE source_entity_type = 'person'
          AND target_entity_type IN ('company', 'association', 'domain')
          {hub_origin_filter}
        GROUP BY target_entity_type, target_entity_id
        HAVING COUNT(DISTINCT source_entity_id) >= 2
        ORDER BY person_count DESC, target_entity_id
        LIMIT {HUB_OVER_FETCH}
    """)
    hub_pick_q = _bind_excluded(hub_pick_q, excluded)
    hub_params = {"excluded_origins": excluded} if excluded else {}
    raw_hub_rows = (await session.execute(hub_pick_q, hub_params)).fetchall()

    visible_hubs: list[tuple[str, str]] = []
    for ht, hi, _pc in raw_hub_rows:
        hi_str = str(hi)
        if _is_hidden(ht, hi_str):
            continue
        visible_hubs.append((ht, hi_str))
    visible_hubs = visible_hubs[:HUB_CANDIDATES]

    nbr_extra = _origin_filter_sql(excluded)
    nbr_q = text(f"""
        SELECT r.source_entity_type, r.source_entity_id,
               r.target_entity_type, r.target_entity_id,
               r.relationship_type, r.details, r.origin_kind, r.verified,
               r.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE ((r.source_entity_type = 'person' AND r.source_entity_id IN (:p1, :p2))
            OR (r.target_entity_type = 'person' AND r.target_entity_id IN (:p1, :p2)))
          {nbr_extra}
    """)
    nbr_q = _bind_excluded(nbr_q, excluded)

    if visible_hubs:
        n_hubs = len(visible_hubs)
        # Walk hubs starting from rotation_seed; first one whose pair has a
        # visible shared neighbour wins.
        for hub_offset in range(n_hubs):
            ht, hi = visible_hubs[(rotation_seed + hub_offset) % n_hubs]

            person_rows = (await session.execute(text("""
                SELECT DISTINCT source_entity_id
                FROM entity_relationships
                WHERE source_entity_type = 'person'
                  AND target_entity_type = :ht
                  AND target_entity_id   = :hi
                ORDER BY source_entity_id
            """), {"ht": ht, "hi": hi})).fetchall()

            visible_persons = [
                str(r[0]) for r in person_rows
                if str(r[0]) not in hidden_persons
            ]
            if len(visible_persons) < 2:
                continue

            n = len(visible_persons)
            i1 = rotation_seed % n
            i2 = (rotation_seed + 1 + (rotation_seed // n)) % n
            if i2 == i1:
                i2 = (i1 + 1) % n
            p1 = visible_persons[i1]
            p2 = visible_persons[i2]

            nbr_params: dict = {"p1": p1, "p2": p2}
            if excluded:
                nbr_params["excluded_origins"] = excluded
            rows = (await session.execute(nbr_q, nbr_params)).fetchall()
            # Drop edges that touch any hidden entity so the validity check
            # below sees what the user will actually render.
            visible_rows = [
                r for r in rows
                if not _is_hidden(r[0], str(r[1])) and not _is_hidden(r[2], str(r[3]))
            ]
            if not visible_rows:
                continue

            # Validate: do p1 and p2 still share at least one visible
            # neighbour? Without this check the home page can end up
            # showing two disconnected stars (the original connector was
            # hidden) — a confusing picture.
            p1_neighbours: set[str] = set()
            p2_neighbours: set[str] = set()
            for r in visible_rows:
                src_id = str(r[1])
                tgt_id = str(r[3])
                if src_id == p1: p1_neighbours.add(tgt_id)
                if tgt_id == p1: p1_neighbours.add(src_id)
                if src_id == p2: p2_neighbours.add(tgt_id)
                if tgt_id == p2: p2_neighbours.add(src_id)
            shared = (p1_neighbours & p2_neighbours) - {p1, p2}
            if not shared:
                continue

            return _trim_two_suns(visible_rows, p1, p2)

    # ── Fallback: star (1 visible hub, ≥ 2 visible persons) ─────────────
    star_filter = _origin_filter_sql(excluded, alias="entity_relationships")
    star_pick_q = text(f"""
        SELECT target_entity_type AS hub_type,
               target_entity_id   AS hub_id,
               COUNT(DISTINCT source_entity_id) AS person_count
        FROM entity_relationships
        WHERE source_entity_type = 'person'
          AND target_entity_type IN ('company', 'association')
          {star_filter}
        GROUP BY target_entity_type, target_entity_id
        HAVING COUNT(DISTINCT source_entity_id) >= 2
        ORDER BY person_count DESC, target_entity_id
        LIMIT 20
    """)
    star_pick_q = _bind_excluded(star_pick_q, excluded)
    star_params = {"excluded_origins": excluded} if excluded else {}
    star_candidates = (await session.execute(star_pick_q, star_params)).fetchall()
    for ht, hi, _pc in star_candidates:
        hi_str = str(hi)
        if _is_hidden(ht, hi_str):
            continue
        edges_filter = _origin_filter_sql(excluded)
        edges_q = text(f"""
            SELECT r.source_entity_type, r.source_entity_id,
                   r.target_entity_type, r.target_entity_id,
                   r.relationship_type, r.details, r.origin_kind, r.verified,
                   r.document_id, d.title AS doc_title, d.file_url AS doc_url
            FROM entity_relationships r
            LEFT JOIN documents d ON d.id = r.document_id
            WHERE r.source_entity_type = 'person'
              AND r.target_entity_type = :hub_type
              AND r.target_entity_id   = :hub_id
              {edges_filter}
            ORDER BY r.created_at, r.id
            LIMIT 12
        """)
        edges_q = _bind_excluded(edges_q, excluded)
        edges_params: dict = {"hub_type": ht, "hub_id": hi_str}
        if excluded:
            edges_params["excluded_origins"] = excluded
        rows = (await session.execute(edges_q, edges_params)).fetchall()
        # Skip rows whose person endpoint is hidden so the star stays clean.
        visible_rows = [
            r for r in rows
            if not _is_hidden(r[0], str(r[1])) and not _is_hidden(r[2], str(r[3]))
        ]
        if len(visible_rows) >= 2:
            return _build_subgraph_from_rows(visible_rows[:6])

    # ── Fallback: any direct person → person edge (both visible) ─────────
    direct_filter = _origin_filter_sql(excluded)
    direct_q = text(f"""
        SELECT r.source_entity_type, r.source_entity_id,
               r.target_entity_type, r.target_entity_id,
               r.relationship_type, r.details, r.origin_kind, r.verified,
               r.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE r.source_entity_type = 'person'
          AND r.target_entity_type = 'person'
          AND r.source_entity_id <> r.target_entity_id
          {direct_filter}
        LIMIT 20
    """)
    direct_q = _bind_excluded(direct_q, excluded)
    direct_params = {"excluded_origins": excluded} if excluded else {}
    direct_rows = (await session.execute(direct_q, direct_params)).fetchall()
    for r in direct_rows:
        if _is_hidden(r[0], str(r[1])) or _is_hidden(r[2], str(r[3])):
            continue
        return _build_subgraph_from_rows([r])

    return None


def _trim_two_suns(rows, p1: str, p2: str, per_person_cap: int = 8) -> SubGraph:
    """Pick rows so the resulting graph has both persons in the centre, all
    of their *shared* neighbours, and up to `per_person_cap` extra unique
    neighbours per person."""
    # Group rows by (which person it touches, neighbour key)
    p1_groups: dict[str, list] = {}
    p2_groups: dict[str, list] = {}

    for row in rows:
        src_type, src_id, tgt_type, tgt_id = row[:4]
        s = str(src_id)
        t = str(tgt_id)
        # Each row has at least one endpoint that is p1 or p2 (by construction).
        if s == p1:
            p1_groups.setdefault(f"{tgt_type}:{t}", []).append(row)
        elif t == p1:
            p1_groups.setdefault(f"{src_type}:{s}", []).append(row)
        if s == p2:
            p2_groups.setdefault(f"{tgt_type}:{t}", []).append(row)
        elif t == p2:
            p2_groups.setdefault(f"{src_type}:{s}", []).append(row)

    shared = set(p1_groups) & set(p2_groups)
    p1_only = sorted(set(p1_groups) - shared)[:per_person_cap]
    p2_only = sorted(set(p2_groups) - shared)[:per_person_cap]

    selected: list = []
    seen_row_ids: set[int] = set()

    def _add(row):
        rid = id(row)
        if rid in seen_row_ids:
            return
        seen_row_ids.add(rid)
        selected.append(row)

    # Shared connectors: include both edges so the connector visibly
    # touches both persons.
    for k in sorted(shared):
        for r in p1_groups.get(k, []):
            _add(r)
        for r in p2_groups.get(k, []):
            _add(r)

    for k in p1_only:
        for r in p1_groups[k]:
            _add(r)
    for k in p2_only:
        for r in p2_groups[k]:
            _add(r)

    return _build_subgraph_from_rows(selected)


def _build_subgraph_from_rows(rows) -> SubGraph:
    """Build a SubGraph from query rows.

    Canonical column order used by every SQL above:
        0..5  src_type, src_id, tgt_type, tgt_id, rel_type, details
        6     origin_kind        (str, e.g. 'coi_declaration' / 'mk_expense')
        7     verified           (bool)
        8..10 document_id, doc_title, doc_url   (optional, fall back to None)

    A small legacy-shape detection remains for older callers that didn't
    include origin_kind / verified — those default to 'coi_declaration'
    and False respectively.
    """
    nodes_map: dict[str, EntitySummary] = {}
    edges: list[ConnectionEdge] = []

    for row in rows:
        src_type, src_id, tgt_type, tgt_id, rel_type, details = row[:6]

        origin_kind = "coi_declaration"
        verified = False
        doc_offset = 6
        # Detect new vs legacy row shape based on the cell at position 6.
        if len(row) >= 7:
            cand = row[6]
            if isinstance(cand, str) and not _looks_like_uuid(cand):
                origin_kind = cand or "coi_declaration"
                # Position 7 is `verified` when origin_kind is present.
                if len(row) >= 8 and isinstance(row[7], (bool, int)):
                    verified = bool(row[7])
                    doc_offset = 8
                else:
                    doc_offset = 7
            else:
                doc_offset = 6

        doc_id = str(row[doc_offset]) if len(row) > doc_offset and row[doc_offset] else None
        doc_title = row[doc_offset + 1] if len(row) > doc_offset + 1 else None
        doc_url = row[doc_offset + 2] if len(row) > doc_offset + 2 else None

        src_id_str = str(src_id)
        tgt_id_str = str(tgt_id)

        src_key = f"{src_type}:{src_id_str}"
        tgt_key = f"{tgt_type}:{tgt_id_str}"

        if src_key not in nodes_map:
            nodes_map[src_key] = EntitySummary(
                id=src_id_str, entity_type=EntityType(src_type), name="",
            )
        if tgt_key not in nodes_map:
            nodes_map[tgt_key] = EntitySummary(
                id=tgt_id_str, entity_type=EntityType(tgt_type), name="",
            )

        edges.append(ConnectionEdge(
            source_id=src_id_str, source_type=EntityType(src_type), source_name="",
            target_id=tgt_id_str, target_type=EntityType(tgt_type), target_name="",
            relationship_type=rel_type, details=details,
            document_id=doc_id, document_title=doc_title, document_url=doc_url,
            origin_kind=origin_kind,
            verified=verified,
        ))

    return SubGraph(nodes=list(nodes_map.values()), edges=edges)


def _looks_like_uuid(s: str) -> bool:
    """Cheap shape check for a UUID string — used to disambiguate the
    column at position 6 between origin_kind and a legacy document_id."""
    if len(s) < 32:
        return False
    return s.count("-") >= 3 or all(c in "0123456789abcdefABCDEF" for c in s)
