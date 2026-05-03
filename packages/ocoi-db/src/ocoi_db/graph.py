"""Graph queries using recursive CTEs on entity_relationships table.

Compatible with both SQLite and PostgreSQL.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_common.models import ConnectionEdge, EntitySummary, EntityType, SubGraph


async def get_neighbors(
    session: AsyncSession,
    entity_id,
    entity_type: str,
    depth: int = 1,
) -> SubGraph:
    """Get neighboring entities up to `depth` hops away."""
    eid = str(entity_id)
    if depth == 1:
        return await _get_direct_neighbors(session, eid, entity_type)
    return await _get_recursive_neighbors(session, eid, entity_type, depth)


async def _get_direct_neighbors(
    session: AsyncSession,
    entity_id: str,
    entity_type: str,
) -> SubGraph:
    query = text("""
        SELECT
            r.source_entity_type, r.source_entity_id,
            r.target_entity_type, r.target_entity_id,
            r.relationship_type, r.details,
            r.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE (r.source_entity_type = :etype AND r.source_entity_id = :eid)
           OR (r.target_entity_type = :etype AND r.target_entity_id = :eid)
    """)
    result = await session.execute(query, {"eid": entity_id, "etype": entity_type})
    rows = result.fetchall()
    return _build_subgraph_from_rows(rows)


async def _get_recursive_neighbors(
    session: AsyncSession,
    entity_id: str,
    entity_type: str,
    depth: int,
) -> SubGraph:
    """Multi-hop neighbor query using recursive CTE (works on both SQLite and PostgreSQL)."""
    query = text("""
        WITH RECURSIVE graph_walk AS (
            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details,
                r.document_id,
                1 AS depth
            FROM entity_relationships r
            WHERE (r.source_entity_type = :etype AND r.source_entity_id = :eid)
               OR (r.target_entity_type = :etype AND r.target_entity_id = :eid)

            UNION

            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details,
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
        )
        SELECT DISTINCT
            gw.source_entity_type, gw.source_entity_id,
            gw.target_entity_type, gw.target_entity_id,
            gw.relationship_type, gw.details,
            gw.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM graph_walk gw
        LEFT JOIN documents d ON d.id = gw.document_id
    """)
    result = await session.execute(
        query, {"eid": entity_id, "etype": entity_type, "max_depth": depth}
    )
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
                r.relationship_type, r.details,
                r.document_id,
                1 AS depth
            FROM entity_relationships r
            WHERE (r.source_entity_type = :from_type AND r.source_entity_id = :from_id)
               OR (r.target_entity_type = :from_type AND r.target_entity_id = :from_id)

            UNION

            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details,
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
            ps.relationship_type, ps.details,
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


async def _fetch_edge(session: AsyncSession, source_id: str, hub_type: str, hub_id: str):
    q = text("""
        SELECT r.source_entity_type, r.source_entity_id,
               r.target_entity_type, r.target_entity_id,
               r.relationship_type, r.details,
               r.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE r.source_entity_type = 'person'
          AND r.source_entity_id   = :pid
          AND r.target_entity_type = :hub_type
          AND r.target_entity_id   = :hub_id
        ORDER BY r.created_at, r.id
        LIMIT 1
    """)
    res = await session.execute(q, {"pid": source_id, "hub_type": hub_type, "hub_id": hub_id})
    return res.fetchone()


async def find_showcase_pair(session: AsyncSession) -> SubGraph | None:
    """Find a richer "showcase" subgraph for the home page.

    Preferred shape — a "bowtie": one bridge person who declared restrictions
    on TWO different hubs (companies / associations), with each hub also
    sharing a restriction with another distinct person. That gives a graph
    of 3 persons + 2 hubs + 4 edges, which tells a real story:
    "Person X is restricted from Hub A *and* Hub B; Hub A also touches Y;
    Hub B also touches Z."

    Fallbacks (in order):
      • star: one hub with ≥ 2 distinct persons (3 nodes / 2 edges).
      • a direct person → person edge (2 nodes / 1 edge).
    Returns None if no usable pattern exists.
    """
    # ── Try bowtie ───────────────────────────────────────────────────────
    # 1. Bridge person: connected to ≥ 2 distinct company/association hubs,
    #    where at least one of those hubs is shared with another person.
    bridge_q = text("""
        WITH person_hubs AS (
            SELECT source_entity_id AS person_id,
                   target_entity_type, target_entity_id
            FROM entity_relationships
            WHERE source_entity_type = 'person'
              AND target_entity_type IN ('company', 'association')
            GROUP BY source_entity_id, target_entity_type, target_entity_id
        ),
        hub_other_persons AS (
            SELECT target_entity_type, target_entity_id,
                   COUNT(DISTINCT source_entity_id) AS person_count
            FROM entity_relationships
            WHERE source_entity_type = 'person'
              AND target_entity_type IN ('company', 'association')
            GROUP BY target_entity_type, target_entity_id
        )
        SELECT ph.person_id,
               COUNT(*) AS hub_count,
               SUM(hop.person_count) AS reach
        FROM person_hubs ph
        JOIN hub_other_persons hop
          ON hop.target_entity_type = ph.target_entity_type
         AND hop.target_entity_id   = ph.target_entity_id
        WHERE hop.person_count >= 2
        GROUP BY ph.person_id
        HAVING COUNT(*) >= 2
        ORDER BY hub_count DESC, reach DESC, ph.person_id
        LIMIT 1
    """)
    bridge_row = (await session.execute(bridge_q)).fetchone()

    if bridge_row:
        bridge_id = str(bridge_row[0])
        # 2. Pick two of the bridge person's shared hubs.
        hubs_q = text("""
            SELECT er.target_entity_type, er.target_entity_id
            FROM entity_relationships er
            JOIN (
                SELECT target_entity_type, target_entity_id
                FROM entity_relationships
                WHERE source_entity_type = 'person'
                  AND target_entity_type IN ('company', 'association')
                GROUP BY target_entity_type, target_entity_id
                HAVING COUNT(DISTINCT source_entity_id) >= 2
            ) shared
              ON shared.target_entity_type = er.target_entity_type
             AND shared.target_entity_id   = er.target_entity_id
            WHERE er.source_entity_type = 'person'
              AND er.source_entity_id   = :bridge
              AND er.target_entity_type IN ('company', 'association')
            GROUP BY er.target_entity_type, er.target_entity_id
            ORDER BY er.target_entity_id
            LIMIT 2
        """)
        hub_rows = (await session.execute(hubs_q, {"bridge": bridge_id})).fetchall()

        if len(hub_rows) >= 2:
            rows = []
            seen_persons = {bridge_id}
            for hub_type, hub_id in hub_rows:
                hub_id_s = str(hub_id)
                # Bridge person → hub edge
                bridge_edge = await _fetch_edge(session, bridge_id, hub_type, hub_id_s)
                if bridge_edge is None:
                    continue
                rows.append(bridge_edge)
                # Pick another distinct person on this hub
                other_q = text("""
                    SELECT DISTINCT source_entity_id
                    FROM entity_relationships
                    WHERE source_entity_type = 'person'
                      AND target_entity_type = :hub_type
                      AND target_entity_id   = :hub_id
                      AND source_entity_id  <> :bridge
                    ORDER BY source_entity_id
                    LIMIT 5
                """)
                others = (await session.execute(
                    other_q,
                    {"hub_type": hub_type, "hub_id": hub_id_s, "bridge": bridge_id},
                )).fetchall()
                for (other_pid,) in others:
                    pid = str(other_pid)
                    if pid in seen_persons:
                        continue
                    other_edge = await _fetch_edge(session, pid, hub_type, hub_id_s)
                    if other_edge:
                        rows.append(other_edge)
                        seen_persons.add(pid)
                        break
            # Need 4 edges = 3 persons + 2 hubs
            if len(rows) >= 4 and len(seen_persons) >= 3:
                return _build_subgraph_from_rows(rows)

    # ── Fallback: star (1 hub, ≥ 2 persons) ──────────────────────────────
    hub_q = text("""
        SELECT target_entity_type AS hub_type,
               target_entity_id   AS hub_id,
               COUNT(DISTINCT source_entity_id) AS person_count
        FROM entity_relationships
        WHERE source_entity_type = 'person'
          AND target_entity_type IN ('company', 'association')
        GROUP BY target_entity_type, target_entity_id
        HAVING COUNT(DISTINCT source_entity_id) >= 2
        ORDER BY person_count DESC, hub_id
        LIMIT 1
    """)
    hub_row = (await session.execute(hub_q)).fetchone()
    if hub_row:
        hub_type, hub_id, _ = hub_row
        persons_q = text("""
            SELECT DISTINCT source_entity_id
            FROM entity_relationships
            WHERE source_entity_type = 'person'
              AND target_entity_type = :hub_type
              AND target_entity_id   = :hub_id
            ORDER BY source_entity_id
            LIMIT 4
        """)
        person_rows = (await session.execute(
            persons_q, {"hub_type": hub_type, "hub_id": str(hub_id)}
        )).fetchall()
        if len(person_rows) >= 2:
            rows = []
            for (pid,) in person_rows:
                edge = await _fetch_edge(session, str(pid), hub_type, str(hub_id))
                if edge:
                    rows.append(edge)
            if len(rows) >= 2:
                return _build_subgraph_from_rows(rows)

    # ── Fallback: any direct person → person edge ────────────────────────
    direct_q = text("""
        SELECT r.source_entity_type, r.source_entity_id,
               r.target_entity_type, r.target_entity_id,
               r.relationship_type, r.details,
               r.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE r.source_entity_type = 'person'
          AND r.target_entity_type = 'person'
          AND r.source_entity_id <> r.target_entity_id
        LIMIT 1
    """)
    rows = (await session.execute(direct_q)).fetchall()
    if rows:
        return _build_subgraph_from_rows(rows)

    return None


def _build_subgraph_from_rows(rows) -> SubGraph:
    nodes_map: dict[str, EntitySummary] = {}
    edges: list[ConnectionEdge] = []

    for row in rows:
        src_type, src_id, tgt_type, tgt_id, rel_type, details = row[:6]
        # Optional document fields (present when query joins documents table)
        doc_id = str(row[6]) if len(row) > 6 and row[6] else None
        doc_title = row[7] if len(row) > 7 else None
        doc_url = row[8] if len(row) > 8 else None

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
        ))

    return SubGraph(nodes=list(nodes_map.values()), edges=edges)
