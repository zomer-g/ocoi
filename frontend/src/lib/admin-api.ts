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

// Import
export function triggerImport() {
  return adminFetch("/import/trigger", { method: "POST" });
}

// Users
export function getAdminUsers() {
  return adminFetch<{ status: string; data: string[] }>("/users");
}
