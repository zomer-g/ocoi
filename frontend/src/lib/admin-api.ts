const ADMIN_BASE = "/api/v1/admin";

async function adminFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${ADMIN_BASE}${path}`, {
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (res.status === 401) {
    window.location.href = "/admin/login";
    throw new Error("Not authenticated");
  }
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API error ${res.status}: ${body}`);
  }
  return res.json();
}

// Stats
export function getAdminStats() {
  return adminFetch<{ status: string; data: Record<string, number> }>("/stats");
}

// Persons
export function createPerson(data: Record<string, string | null>) {
  return adminFetch("/persons", { method: "POST", body: JSON.stringify(data) });
}
export function updatePerson(id: string, data: Record<string, string | null>) {
  return adminFetch(`/persons/${id}`, { method: "PUT", body: JSON.stringify(data) });
}
export function deletePerson(id: string) {
  return adminFetch(`/persons/${id}`, { method: "DELETE" });
}

// Companies
export function createCompany(data: Record<string, string | null>) {
  return adminFetch("/companies", { method: "POST", body: JSON.stringify(data) });
}
export function updateCompany(id: string, data: Record<string, string | null>) {
  return adminFetch(`/companies/${id}`, { method: "PUT", body: JSON.stringify(data) });
}
export function deleteCompany(id: string) {
  return adminFetch(`/companies/${id}`, { method: "DELETE" });
}

// Associations
export function createAssociation(data: Record<string, string | null>) {
  return adminFetch("/associations", { method: "POST", body: JSON.stringify(data) });
}
export function updateAssociation(id: string, data: Record<string, string | null>) {
  return adminFetch(`/associations/${id}`, { method: "PUT", body: JSON.stringify(data) });
}
export function deleteAssociation(id: string) {
  return adminFetch(`/associations/${id}`, { method: "DELETE" });
}

// Domains
export function createDomain(data: Record<string, string | null>) {
  return adminFetch("/domains", { method: "POST", body: JSON.stringify(data) });
}
export function updateDomain(id: string, data: Record<string, string | null>) {
  return adminFetch(`/domains/${id}`, { method: "PUT", body: JSON.stringify(data) });
}
export function deleteDomain(id: string) {
  return adminFetch(`/domains/${id}`, { method: "DELETE" });
}

// Relationships
export function deleteRelationship(id: string) {
  return adminFetch(`/relationships/${id}`, { method: "DELETE" });
}

// Documents
export function getAdminDocuments(page = 1, status?: string) {
  const params = new URLSearchParams({ page: String(page) });
  if (status) params.set("status", status);
  return adminFetch(`/documents?${params}`);
}
export function deleteDocument(id: string) {
  return adminFetch(`/documents/${id}`, { method: "DELETE" });
}
export function purgeMetadataOnlyDocuments() {
  return adminFetch<{ status: string; data: { deleted: number } }>("/documents/purge/metadata-only", { method: "DELETE" });
}

// Import — CKAN search + selective import
export interface CkanSearchResult {
  id: string;
  title: string;
  notes: string | null;
  metadata_created: string | null;
  metadata_modified: string | null;
  tags: string[];
  num_resources: number;
  num_documents: number;
  already_imported: number;
}

export interface CkanSearchResponse {
  total: number;
  start: number;
  rows: number;
  results: CkanSearchResult[];
}

export function searchCkan(q: string, rows = 20, start = 0) {
  const params = new URLSearchParams({ q, rows: String(rows), start: String(start) });
  return adminFetch<{ status: string; data: CkanSearchResponse }>(`/import/ckan/search?${params}`);
}

export interface ImportStats {
  imported: number;
  skipped: number;
  errors: number;
  error_messages: string[];
}

export function importCkanDatasets(datasetIds: string[]) {
  return adminFetch<{ status: string; data: ImportStats }>("/import/ckan/import", {
    method: "POST",
    body: JSON.stringify({ dataset_ids: datasetIds }),
  });
}

// Import — Gov.il: browser-side fetch + server-side processing
export function triggerGovilImport(limit: number = 0) {
  const params = new URLSearchParams({ limit: String(limit) });
  return adminFetch(`/import/govil/trigger?${params}`, { method: "POST" });
}

// Send pre-fetched Gov.il records from the browser to backend for processing
export function submitGovilRecords(records: GovilApiItem[]) {
  return adminFetch<{ status: string; message: string }>("/import/govil/submit", {
    method: "POST",
    body: JSON.stringify({ records }),
  });
}

export interface GovilApiItem {
  Data: Record<string, unknown>;
  UrlName: string;
  [key: string]: unknown;
}

const GOVIL_API_URL = "https://www.gov.il/he/api/DynamicCollector";
const GOVIL_TEMPLATE_ID = "c6e0f53e-02c0-4db1-ae89-76590f0f502e";

/** Fetch all Gov.il records directly from the user's browser (bypasses Cloudflare). */
export async function fetchGovilFromBrowser(
  onProgress?: (fetched: number, total: number) => void
): Promise<GovilApiItem[]> {
  const pageSize = 20;
  const firstResp = await fetch(GOVIL_API_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json;charset=utf-8" },
    body: JSON.stringify({
      DynamicTemplateID: GOVIL_TEMPLATE_ID,
      QueryFilters: {},
      From: 0,
      Quantity: pageSize,
    }),
  });
  if (!firstResp.ok) throw new Error(`Gov.il API error: ${firstResp.status}`);
  const firstData = await firstResp.json();
  const totalResults: number = firstData.TotalResults || 0;
  const allItems: GovilApiItem[] = [...(firstData.Results || [])];
  onProgress?.(allItems.length, totalResults);

  let skip = allItems.length;
  while (skip < totalResults) {
    const resp = await fetch(GOVIL_API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json;charset=utf-8" },
      body: JSON.stringify({
        DynamicTemplateID: GOVIL_TEMPLATE_ID,
        QueryFilters: {},
        From: skip,
        Quantity: pageSize,
      }),
    });
    if (!resp.ok) break;
    const data = await resp.json();
    const results: GovilApiItem[] = data.Results || [];
    if (results.length === 0) break;
    allItems.push(...results);
    skip += results.length;
    onProgress?.(allItems.length, totalResults);
  }
  return allItems;
}

export interface ImportStatus {
  running: boolean;
  source: string | null;
  total_on_website: number;
  already_in_db: number;
  new_to_import: number;
  total: number;
  imported: number;
  skipped: number;
  errors: number;
  error_messages: string[];
  started_at: string | null;
  finished_at: string | null;
}

export function getImportStatus() {
  return adminFetch<{ status: string; data: ImportStatus }>("/import/status");
}

// Extraction — DeepSeek entity extraction
export interface ExtractionPrompt {
  system_prompt: string;
  user_prompt: string;
}

export interface ExtractionStatus {
  running: boolean;
  total: number;
  processed: number;
  entities_found: number;
  relationships_found: number;
  errors: number;
  error_messages: string[];
  started_at: string | null;
  finished_at: string | null;
}

export function getExtractionPrompt() {
  return adminFetch<{ status: string; data: ExtractionPrompt }>("/extraction/prompt");
}

export function updateExtractionPrompt(system_prompt: string, user_prompt: string) {
  return adminFetch("/extraction/prompt", {
    method: "PUT",
    body: JSON.stringify({ system_prompt, user_prompt }),
  });
}

export function triggerExtraction(documentIds?: string[]) {
  return adminFetch("/extraction/trigger", {
    method: "POST",
    body: JSON.stringify(documentIds ? { document_ids: documentIds } : {}),
  });
}

export function getExtractionStatus() {
  return adminFetch<{ status: string; data: ExtractionStatus }>("/extraction/status");
}

// Users
export function getAdminUsers() {
  return adminFetch<{ status: string; data: string[] }>("/users");
}
