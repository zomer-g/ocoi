const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "/api/v1";

export interface EntitySummary {
  id: string;
  entity_type: "person" | "company" | "association" | "domain";
  name: string;
  extra?: Record<string, unknown>;
}

export interface ConnectionEdge {
  source_id: string;
  source_type: string;
  source_name: string;
  target_id: string;
  target_type: string;
  target_name: string;
  relationship_type: string;
  details?: string;
  document_id?: string;
  document_title?: string;
  document_url?: string;
}

export interface SubGraph {
  nodes: EntitySummary[];
  edges: ConnectionEdge[];
}

export interface PaginatedResponse<T> {
  status: string;
  data: T[];
  meta: { total: number; page: number; limit: number; pages: number };
}

async function fetchApi<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  const json = await res.json();
  return json;
}

export async function search(
  query: string,
  type?: string,
  page = 1,
  limit = 20
) {
  const params = new URLSearchParams({ q: query, page: String(page), limit: String(limit) });
  if (type) params.set("type", type);
  return fetchApi<PaginatedResponse<EntitySummary>>(`/search?${params}`);
}

export async function suggest(query: string) {
  return fetchApi<{ data: { text: string; type: string; id: string }[] }>(
    `/search/suggest?q=${encodeURIComponent(query)}`
  );
}

export async function getEntity(type: string, id: string) {
  const plural = type === "person" ? "persons" : type + (type.endsWith("y") ? "ies" : "s");
  return fetchApi<{ data: Record<string, unknown> }>(`/${plural}/${id}`);
}

export async function getNeighbors(entityId: string, entityType: string, depth = 1) {
  return fetchApi<{ data: SubGraph }>(
    `/graph/neighbors/${entityId}?type=${entityType}&depth=${depth}`
  );
}

export async function getPath(fromId: string, fromType: string, toId: string, toType: string) {
  return fetchApi<{ data: SubGraph }>(
    `/graph/path?from_id=${fromId}&from_type=${fromType}&to_id=${toId}&to_type=${toType}`
  );
}

export async function getStats() {
  return fetchApi<{ data: Record<string, number> }>("/external/stats");
}

export interface RankedEntity {
  id: string;
  entity_type: "person" | "company" | "association" | "domain";
  name: string;
  connection_count: number;
  position?: string | null;
  ministry?: string | null;
}

export async function getTopConnected(type?: string, page = 1, limit = 20) {
  const params = new URLSearchParams({ page: String(page), limit: String(limit) });
  if (type) params.set("type", type);
  return fetchApi<PaginatedResponse<RankedEntity>>(`/entities/top-connected?${params}`);
}

export interface MinistryInfo {
  ministry: string;
  person_count: number;
  connection_count: number;
}

export async function getMinistries() {
  return fetchApi<{ data: MinistryInfo[] }>("/entities/ministries");
}
