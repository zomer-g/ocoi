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
