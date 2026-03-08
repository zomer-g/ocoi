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
            r.relationship_type, r.details
        FROM entity_relationships r
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
                1 AS depth
            FROM entity_relationships r
            WHERE (r.source_entity_type = :etype AND r.source_entity_id = :eid)
               OR (r.target_entity_type = :etype AND r.target_entity_id = :eid)

            UNION

            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details,
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
            source_entity_type, source_entity_id,
            target_entity_type, target_entity_id,
            relationship_type, details
        FROM graph_walk
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
                1 AS depth
            FROM entity_relationships r
            WHERE (r.source_entity_type = :from_type AND r.source_entity_id = :from_id)
               OR (r.target_entity_type = :from_type AND r.target_entity_id = :from_id)

            UNION

            SELECT
                r.source_entity_type, r.source_entity_id,
                r.target_entity_type, r.target_entity_id,
                r.relationship_type, r.details,
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
            source_entity_type, source_entity_id,
            target_entity_type, target_entity_id,
            relationship_type, details
        FROM path_search
        WHERE (source_entity_type = :to_type AND source_entity_id = :to_id)
           OR (target_entity_type = :to_type AND target_entity_id = :to_id)
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


def _build_subgraph_from_rows(rows) -> SubGraph:
    nodes_map: dict[str, EntitySummary] = {}
    edges: list[ConnectionEdge] = []

    for row in rows:
        src_type, src_id, tgt_type, tgt_id, rel_type, details = row
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
        ))

    return SubGraph(nodes=list(nodes_map.values()), edges=edges)
