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

// Upload
export interface UploadResult {
  id: string;
  title: string;
  file_size: number;
  markdown_length: number;
}

export function uploadDocument(
  file: File,
  onProgress?: (loaded: number, total: number) => void
): Promise<{ status: string; data: UploadResult }> {
  const formData = new FormData();
  formData.append("file", file);

  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${ADMIN_BASE}/documents/upload`);
    xhr.withCredentials = true;

    xhr.upload.addEventListener("progress", (e) => {
      if (e.lengthComputable && onProgress) onProgress(e.loaded, e.total);
    });

    xhr.addEventListener("load", () => {
      if (xhr.status === 401) {
        window.location.href = "/admin/login";
        reject(new Error("Not authenticated"));
        return;
      }
      if (xhr.status >= 400) {
        try {
          const body = JSON.parse(xhr.responseText);
          reject(new Error(body.detail || `Upload failed (${xhr.status})`));
        } catch {
          reject(new Error(`Upload failed (${xhr.status})`));
        }
        return;
      }
      resolve(JSON.parse(xhr.responseText));
    });

    xhr.addEventListener("error", () => reject(new Error("שגיאת רשת")));
    xhr.send(formData);
  });
}

// Import — CKAN search + selective import
export interface CkanResource {
  url: string;
  title: string;
  format: string;
  size: number | null;
  resource_id: string | null;
  already_imported: boolean;
}

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
  resources: CkanResource[];
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

export interface CkanResourceImport {
  dataset_id: string;
  url: string;
  title: string;
  format: string;
  size: number | null;
  resource_id: string | null;
}

export function importCkanResources(resources: CkanResourceImport[]) {
  return adminFetch<{ status: string; data: ImportStats }>("/import/ckan/import", {
    method: "POST",
    body: JSON.stringify({ resources }),
  });
}

// Import — Gov.il: browser-side fetch + server-side processing
export function triggerGovilImport(limit: number = 0, url: string = "") {
  const params = new URLSearchParams({ limit: String(limit) });
  return adminFetch(`/import/govil/trigger?${params}`, {
    method: "POST",
    body: JSON.stringify({ url }),
  });
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

const GOVIL_TEMPLATE_ID = "c6e0f53e-02c0-4db1-ae89-76590f0f502e";

/** Fetch a single page from Gov.il via our backend proxy (avoids CORS). */
async function govilProxyFetch(skip: number, quantity: number = 20) {
  const res = await fetch(`${ADMIN_BASE}/import/govil/proxy`, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      DynamicTemplateID: GOVIL_TEMPLATE_ID,
      QueryFilters: {},
      From: skip,
      Quantity: quantity,
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`שגיאה בגישה ל-Gov.il (${res.status}): ${body.slice(0, 100)}`);
  }
  return res.json();
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Fetch all Gov.il records page by page via backend proxy with retries. */
export async function fetchGovilFromBrowser(
  onProgress?: (fetched: number, total: number) => void
): Promise<GovilApiItem[]> {
  const pageSize = 20;
  const firstData = await govilProxyFetch(0, pageSize);
  const totalResults: number = firstData.TotalResults || 0;
  const allItems: GovilApiItem[] = [...(firstData.Results || [])];
  onProgress?.(allItems.length, totalResults);

  let skip = allItems.length;
  let consecutiveFailures = 0;
  while (skip < totalResults) {
    // Small delay between pages to avoid rate-limiting
    await sleep(500);
    try {
      const data = await govilProxyFetch(skip, pageSize);
      const results: GovilApiItem[] = data.Results || [];
      if (results.length === 0) break;
      allItems.push(...results);
      skip += results.length;
      consecutiveFailures = 0;
      onProgress?.(allItems.length, totalResults);
    } catch {
      consecutiveFailures++;
      if (consecutiveFailures >= 3) {
        // Return what we have so far instead of failing completely
        if (allItems.length > 0) break;
        throw new Error("Gov.il לא זמין כרגע. נסה שוב מאוחר יותר.");
      }
      await sleep(3000 * consecutiveFailures);
    }
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

export function resetImportState() {
  return adminFetch<{ status: string; message: string }>("/import/reset", { method: "POST" });
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
