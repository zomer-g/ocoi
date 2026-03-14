"use client";

import { useEffect, useState } from "react";

/* ── Types for parsed OpenAPI schema ─────────────────────────────────── */
interface OpenAPIParam {
  name: string;
  in: string;
  required?: boolean;
  description?: string;
  schema?: { type?: string; default?: unknown; enum?: string[] };
}

interface OpenAPIEndpoint {
  path: string;
  method: string;
  summary?: string;
  description?: string;
  parameters?: OpenAPIParam[];
  tags?: string[];
}

/* ── Tag grouping config ─────────────────────────────────────────────── */
const TAG_LABELS: Record<string, string> = {
  search: "חיפוש",
  entities: "ישויות",
  documents: "מסמכים",
  connections: "קשרים",
  external: "חיצוני",
};

function getTagLabel(tag: string): string {
  return TAG_LABELS[tag] || tag;
}

/* ── Parse OpenAPI into flat endpoint list ──────────────────────────── */
function parseEndpoints(schema: Record<string, unknown>): OpenAPIEndpoint[] {
  const paths = (schema.paths || {}) as Record<string, Record<string, Record<string, unknown>>>;
  const endpoints: OpenAPIEndpoint[] = [];

  for (const [path, methods] of Object.entries(paths)) {
    for (const [method, op] of Object.entries(methods)) {
      if (method === "parameters") continue;
      endpoints.push({
        path,
        method: method.toUpperCase(),
        summary: (op.summary as string) || "",
        description: (op.description as string) || "",
        parameters: (op.parameters as OpenAPIParam[]) || [],
        tags: (op.tags as string[]) || ["other"],
      });
    }
  }

  return endpoints;
}

/* ── Components ──────────────────────────────────────────────────────── */

function MethodBadge({ method }: { method: string }) {
  const colors: Record<string, string> = {
    GET: "bg-green-50 text-green-700 border-green-200",
    POST: "bg-blue-50 text-blue-700 border-blue-200",
    PUT: "bg-amber-50 text-amber-700 border-amber-200",
    DELETE: "bg-red-50 text-red-700 border-red-200",
  };

  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-mono font-bold border ${colors[method] || "bg-gray-50 text-gray-700 border-gray-200"}`}>
      {method}
    </span>
  );
}

function ParamRow({ param }: { param: OpenAPIParam }) {
  return (
    <tr className="border-b border-gray-100 last:border-0">
      <td className="px-3 py-2 font-mono text-primary-700 text-sm">{param.name}</td>
      <td className="px-3 py-2 text-gray-500 text-sm">{param.schema?.type || "string"}</td>
      <td className="px-3 py-2 text-sm">
        {param.required ? (
          <span className="text-xs bg-red-50 text-red-600 px-1.5 py-0.5 rounded">חובה</span>
        ) : (
          <span className="text-xs bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded">אופציונלי</span>
        )}
      </td>
      <td className="px-3 py-2 text-gray-600 text-sm">{param.description || "—"}</td>
    </tr>
  );
}

function EndpointCard({ endpoint, baseUrl }: { endpoint: OpenAPIEndpoint; baseUrl: string }) {
  const [open, setOpen] = useState(false);
  const queryParams = endpoint.parameters?.filter((p) => p.in === "query") || [];
  const pathParams = endpoint.parameters?.filter((p) => p.in === "path") || [];
  const allParams = [...pathParams, ...queryParams];

  // Build example curl
  const exampleUrl = `${baseUrl}${endpoint.path}`;

  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-3 p-4 text-start hover:bg-gray-50 transition-colors"
      >
        <MethodBadge method={endpoint.method} />
        <code className="text-primary-700 text-sm font-mono flex-1" dir="ltr">{endpoint.path}</code>
        <span className="text-gray-500 text-sm hidden sm:block">{endpoint.summary}</span>
        <svg
          className={`w-4 h-4 text-gray-400 transition-transform ${open ? "rotate-180" : ""}`}
          fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2"
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="border-t border-gray-200 p-4 space-y-4">
          {endpoint.description && (
            <p className="text-gray-600 text-sm">{endpoint.description}</p>
          )}

          {allParams.length > 0 && (
            <div>
              <h4 className="text-sm font-semibold text-gray-900 mb-2">פרמטרים</h4>
              <div className="overflow-x-auto rounded border border-gray-200">
                <table className="w-full text-sm" dir="rtl">
                  <thead>
                    <tr className="bg-gray-50 text-gray-600">
                      <th className="px-3 py-2 text-start font-medium">שם</th>
                      <th className="px-3 py-2 text-start font-medium">סוג</th>
                      <th className="px-3 py-2 text-start font-medium">נדרש</th>
                      <th className="px-3 py-2 text-start font-medium">תיאור</th>
                    </tr>
                  </thead>
                  <tbody>
                    {allParams.map((p) => (
                      <ParamRow key={p.name} param={p} />
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          <div>
            <h4 className="text-sm font-semibold text-gray-900 mb-2">דוגמה</h4>
            <div className="bg-gray-100 rounded-lg p-4 overflow-x-auto" dir="ltr">
              <code className="text-sm font-mono text-gray-800 whitespace-pre">
                curl {exampleUrl}
              </code>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Main Page ───────────────────────────────────────────────────────── */

export default function ApiDocsPage() {
  const [endpoints, setEndpoints] = useState<OpenAPIEndpoint[]>([]);
  const [tags, setTags] = useState<string[]>([]);
  const [activeTag, setActiveTag] = useState<string>("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // Detect base URL for curl examples
  const baseUrl = typeof window !== "undefined"
    ? `${window.location.origin}/api/v1`
    : "/api/v1";

  useEffect(() => {
    fetch("/api/public-openapi.json")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((schema) => {
        const eps = parseEndpoints(schema);
        setEndpoints(eps);

        // Collect unique tags
        const tagSet = new Set<string>();
        eps.forEach((e) => e.tags?.forEach((t) => tagSet.add(t)));
        const tagArr = Array.from(tagSet);
        setTags(tagArr);
        if (tagArr.length > 0) setActiveTag(tagArr[0]);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const filteredEndpoints = activeTag
    ? endpoints.filter((e) => e.tags?.includes(activeTag))
    : endpoints;

  return (
    <>
      {/* Hero */}
      <section className="bg-gradient-to-b from-primary-800 to-primary-700 py-10 px-4">
        <div className="max-w-4xl mx-auto text-center">
          <h1 className="text-2xl sm:text-3xl font-bold text-white mb-2">API ציבורי</h1>
          <p className="text-primary-200 text-sm sm:text-base">
            ממשק פתוח לקריאת נתוני ניגוד עניינים — ישויות, קשרים, מסמכים ומרשמים
          </p>
        </div>
      </section>

      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {loading && (
          <div className="text-center py-12 text-gray-400">טוען תיעוד API...</div>
        )}

        {error && (
          <div className="text-center py-12 text-red-500">שגיאה בטעינת תיעוד: {error}</div>
        )}

        {!loading && !error && (
          <>
            {/* Info bar */}
            <div className="bg-primary-50 border border-primary-200 rounded-lg p-4 mb-6 text-sm">
              <p className="text-primary-800">
                <strong>כתובת בסיס:</strong>{" "}
                <code className="bg-white px-2 py-0.5 rounded border border-primary-200 font-mono text-sm" dir="ltr">
                  {baseUrl}
                </code>
              </p>
              <p className="text-primary-700 mt-1">
                כל הנקודות הקצה הן לקריאה בלבד (GET). אין צורך באימות.
              </p>
            </div>

            {/* Tag tabs */}
            {tags.length > 1 && (
              <div className="flex flex-wrap gap-2 mb-6">
                {tags.map((tag) => (
                  <button
                    key={tag}
                    onClick={() => setActiveTag(tag)}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                      activeTag === tag
                        ? "bg-primary-700 text-white"
                        : "bg-white text-gray-600 border border-gray-200 hover:bg-gray-50"
                    }`}
                  >
                    {getTagLabel(tag)}
                  </button>
                ))}
              </div>
            )}

            {/* Endpoint cards */}
            <div className="space-y-3">
              {filteredEndpoints.map((ep) => (
                <EndpointCard
                  key={`${ep.method}-${ep.path}`}
                  endpoint={ep}
                  baseUrl={baseUrl}
                />
              ))}

              {filteredEndpoints.length === 0 && (
                <p className="text-gray-400 text-center py-8">אין נקודות קצה בקטגוריה זו</p>
              )}
            </div>
          </>
        )}
      </div>
    </>
  );
}
