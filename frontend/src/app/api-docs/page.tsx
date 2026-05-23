"use client";

import { useEffect, useMemo, useState } from "react";

/* ── Types for parsed OpenAPI schema ─────────────────────────────────── */
interface OpenAPIParam {
  name: string;
  in: string;
  required?: boolean;
  description?: string;
  schema?: { type?: string; default?: unknown; enum?: string[] };
  example?: unknown;
}

interface OpenAPIResponseContent {
  schema?: Record<string, unknown>;
  example?: unknown;
  examples?: Record<string, { value?: unknown }>;
}

interface OpenAPIResponse {
  description?: string;
  content?: Record<string, OpenAPIResponseContent>;
}

interface OpenAPIEndpoint {
  path: string;
  method: string;
  summary?: string;
  description?: string;
  parameters?: OpenAPIParam[];
  tags?: string[];
  responses?: Record<string, OpenAPIResponse>;
  anchorId: string;
}

/* ── Tag grouping config ─────────────────────────────────────────────── */
const TAG_LABELS: Record<string, string> = {
  search: "חיפוש",
  entities: "ישויות",
  documents: "מסמכים",
  connections: "קשרים",
  external: "אינטגרציות חיצוניות",
  site: "תוכן אתר",
  other: "שונות",
};

const TAG_DESCRIPTIONS: Record<string, string> = {
  search: "חיפוש חופשי, השלמות אוטומטיות וחיפוש מאוחד על פני ישויות.",
  entities: "אנשים, חברות, עמותות ותחומים — רשימות ופרטים.",
  documents: "מסמכי הצהרה — מטא־דאטה, טקסט, ישויות מקושרות וקובץ PDF.",
  connections: "מפת הקשרים — שכנים, מסלולים ותתי־גרף סביב ישות.",
  external: "נקודות קצה ייעודיות לשילוב באתרים חיצוניים (שקיפות תקציבית וכו').",
  site: "תוכן ניהולי של האתר (כותרות וטקסטים).",
  other: "נקודות קצה נוספות.",
};

function getTagLabel(tag: string): string {
  return TAG_LABELS[tag] || tag;
}

/* ── Parse OpenAPI into flat endpoint list ──────────────────────────── */
function makeAnchorId(method: string, path: string): string {
  return `${method}-${path}`.replace(/[^a-zA-Z0-9-]/g, "_");
}

function parseEndpoints(schema: Record<string, unknown>): OpenAPIEndpoint[] {
  const paths = (schema.paths || {}) as Record<string, Record<string, Record<string, unknown>>>;
  const endpoints: OpenAPIEndpoint[] = [];

  for (const [path, methods] of Object.entries(paths)) {
    for (const [method, op] of Object.entries(methods)) {
      if (method === "parameters") continue;
      const upperMethod = method.toUpperCase();
      endpoints.push({
        path,
        method: upperMethod,
        summary: (op.summary as string) || "",
        description: (op.description as string) || "",
        parameters: (op.parameters as OpenAPIParam[]) || [],
        tags: (op.tags as string[]) || ["other"],
        responses: (op.responses as Record<string, OpenAPIResponse>) || {},
        anchorId: makeAnchorId(upperMethod, path),
      });
    }
  }

  return endpoints;
}

/* ── Example value generation ────────────────────────────────────────── */
function exampleForType(type?: string): string {
  switch (type) {
    case "integer":
    case "number":
      return "1";
    case "boolean":
      return "true";
    default:
      return "example";
  }
}

function buildExampleUrl(baseUrl: string, endpoint: OpenAPIEndpoint): string {
  let path = endpoint.path;
  const pathParams = endpoint.parameters?.filter((p) => p.in === "path") || [];
  for (const p of pathParams) {
    const ex =
      (p.example as string | undefined) ??
      (p.schema?.default as string | undefined) ??
      (p.name.endsWith("_id") ? "00000000-0000-0000-0000-000000000000" : exampleForType(p.schema?.type));
    path = path.replace(`{${p.name}}`, String(ex));
  }

  const requiredQuery = endpoint.parameters?.filter((p) => p.in === "query" && p.required) || [];
  if (requiredQuery.length > 0) {
    const qs = requiredQuery
      .map((p) => {
        const ex =
          (p.example as string | undefined) ??
          (p.schema?.enum?.[0] as string | undefined) ??
          (p.schema?.default as string | undefined) ??
          exampleForType(p.schema?.type);
        return `${encodeURIComponent(p.name)}=${encodeURIComponent(String(ex))}`;
      })
      .join("&");
    path += `?${qs}`;
  }

  return `${baseUrl}${path}`;
}

function genericExampleResponse(path: string): unknown {
  // Match the canonical response shape used across the routers
  if (path.includes("/pdf") || path.includes("/markdown")) {
    return "[binary or markdown content]";
  }
  const isList = !path.match(/\{[^}]+\}$/) && !path.includes("/showcase") && !path.includes("/path");
  if (isList) {
    return {
      status: "ok",
      data: ["..."],
      meta: { total: 0, page: 1, limit: 20, pages: 0 },
    };
  }
  return { status: "ok", data: { "...": "..." } };
}

function extractResponseExample(endpoint: OpenAPIEndpoint): unknown {
  const ok = endpoint.responses?.["200"] || endpoint.responses?.["default"];
  const json = ok?.content?.["application/json"];
  if (json?.example !== undefined) return json.example;
  if (json?.examples) {
    const first = Object.values(json.examples)[0];
    if (first?.value !== undefined) return first.value;
  }
  return genericExampleResponse(endpoint.path);
}

/* ── Code sample generation ──────────────────────────────────────────── */
type SampleLang = "curl" | "js" | "py";

function codeSample(lang: SampleLang, url: string): string {
  switch (lang) {
    case "curl":
      return `curl '${url}'`;
    case "js":
      return `const res = await fetch("${url}");\nconst json = await res.json();\nconsole.log(json);`;
    case "py":
      return `import requests\n\nres = requests.get("${url}")\nres.raise_for_status()\nprint(res.json())`;
  }
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
  const enumValues = param.schema?.enum;
  return (
    <tr className="border-b border-gray-100 last:border-0 align-top">
      <td className="px-3 py-2 font-mono text-primary-700 text-sm whitespace-nowrap">{param.name}</td>
      <td className="px-3 py-2 text-gray-500 text-sm whitespace-nowrap">
        {param.in === "path" ? "path" : "query"} · {param.schema?.type || "string"}
      </td>
      <td className="px-3 py-2 text-sm whitespace-nowrap">
        {param.required ? (
          <span className="text-xs bg-red-50 text-red-600 px-1.5 py-0.5 rounded">חובה</span>
        ) : (
          <span className="text-xs bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded">אופציונלי</span>
        )}
      </td>
      <td className="px-3 py-2 text-gray-600 text-sm">
        <div>{param.description || "—"}</div>
        {enumValues && enumValues.length > 0 && (
          <div className="mt-1 text-xs text-gray-500" dir="ltr">
            ערכים מותרים: {enumValues.map((v) => <code key={v} className="bg-gray-100 px-1 rounded mx-0.5">{v}</code>)}
          </div>
        )}
      </td>
    </tr>
  );
}

function CodeBlock({ children }: { children: string }) {
  return (
    <pre className="bg-gray-900 text-gray-100 rounded-lg p-4 overflow-x-auto text-xs sm:text-sm font-mono leading-relaxed" dir="ltr">
      <code>{children}</code>
    </pre>
  );
}

function EndpointCard({
  endpoint,
  baseUrl,
  defaultOpen,
}: {
  endpoint: OpenAPIEndpoint;
  baseUrl: string;
  defaultOpen: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  const [lang, setLang] = useState<SampleLang>("curl");

  const queryParams = endpoint.parameters?.filter((p) => p.in === "query") || [];
  const pathParams = endpoint.parameters?.filter((p) => p.in === "path") || [];
  const allParams = [...pathParams, ...queryParams];

  const exampleUrl = useMemo(() => buildExampleUrl(baseUrl, endpoint), [baseUrl, endpoint]);
  const responseExample = useMemo(() => extractResponseExample(endpoint), [endpoint]);
  const sample = useMemo(() => codeSample(lang, exampleUrl), [lang, exampleUrl]);

  return (
    <div id={endpoint.anchorId} className="bg-white rounded-lg border border-gray-200 overflow-hidden scroll-mt-24">
      <button
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        className="w-full flex items-center gap-3 p-4 text-start hover:bg-gray-50 transition-colors"
      >
        <MethodBadge method={endpoint.method} />
        <code className="text-primary-700 text-sm font-mono flex-1 break-all" dir="ltr">{endpoint.path}</code>
        <span className="text-gray-500 text-sm hidden md:block">{endpoint.summary}</span>
        <svg
          className={`w-4 h-4 text-gray-400 transition-transform shrink-0 ${open ? "rotate-180" : ""}`}
          fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2"
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="border-t border-gray-200 p-4 space-y-5">
          {endpoint.description && (
            <p className="text-gray-600 text-sm whitespace-pre-line">{endpoint.description}</p>
          )}

          {allParams.length > 0 && (
            <div>
              <h4 className="text-sm font-semibold text-gray-900 mb-2">פרמטרים</h4>
              <div className="overflow-x-auto rounded border border-gray-200">
                <table className="w-full text-sm" dir="rtl">
                  <thead>
                    <tr className="bg-gray-50 text-gray-600">
                      <th className="px-3 py-2 text-start font-medium">שם</th>
                      <th className="px-3 py-2 text-start font-medium">מיקום · סוג</th>
                      <th className="px-3 py-2 text-start font-medium">נדרש</th>
                      <th className="px-3 py-2 text-start font-medium">תיאור</th>
                    </tr>
                  </thead>
                  <tbody>
                    {allParams.map((p) => (
                      <ParamRow key={`${p.in}-${p.name}`} param={p} />
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          <div>
            <div className="flex items-center justify-between mb-2 flex-wrap gap-2">
              <h4 className="text-sm font-semibold text-gray-900">דוגמת קריאה</h4>
              <div className="flex gap-1 text-xs">
                {(["curl", "js", "py"] as const).map((l) => (
                  <button
                    key={l}
                    onClick={() => setLang(l)}
                    className={`px-2.5 py-1 rounded font-medium border ${
                      lang === l
                        ? "bg-primary-700 text-white border-primary-700"
                        : "bg-white text-gray-600 border-gray-200 hover:bg-gray-50"
                    }`}
                  >
                    {l === "curl" ? "cURL" : l === "js" ? "JavaScript" : "Python"}
                  </button>
                ))}
              </div>
            </div>
            <CodeBlock>{sample}</CodeBlock>
          </div>

          <div>
            <h4 className="text-sm font-semibold text-gray-900 mb-2">דוגמת תשובה (HTTP 200)</h4>
            <CodeBlock>
              {typeof responseExample === "string"
                ? responseExample
                : JSON.stringify(responseExample, null, 2)}
            </CodeBlock>
            <p className="text-xs text-gray-500 mt-2">
              סוג תוכן: <code className="bg-gray-100 px-1 rounded">application/json</code> · קודי שגיאה אפשריים: 400 (פרמטר חסר/לא תקין), 404 (לא נמצא), 422 (ולידציה).
            </p>
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
  const [openAnchor, setOpenAnchor] = useState<string>("");

  // Detect base URL for examples
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

        const tagSet = new Set<string>();
        eps.forEach((e) => e.tags?.forEach((t) => tagSet.add(t)));
        const tagArr = Array.from(tagSet);
        setTags(tagArr);

        // If URL has a hash that matches an endpoint, jump to its tag and open it
        const hash = typeof window !== "undefined" ? window.location.hash.slice(1) : "";
        if (hash) {
          const target = eps.find((e) => e.anchorId === hash);
          if (target) {
            setActiveTag(target.tags?.[0] || tagArr[0] || "");
            setOpenAnchor(hash);
            setTimeout(() => {
              document.getElementById(hash)?.scrollIntoView({ behavior: "smooth", block: "start" });
            }, 100);
            return;
          }
        }
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
            {/* MCP — discoverable separately from the REST docs because it
                speaks a different protocol (JSON-RPC over HTTPS), needs
                auth (Google OAuth), and is positioned as a closed beta. */}
            <section className="mb-8">
              <div className="bg-gradient-to-br from-amber-50 to-amber-100 border border-amber-300 rounded-xl p-5 sm:p-6">
                <div className="flex items-start justify-between gap-3 flex-wrap mb-3">
                  <div>
                    <h2 className="text-xl font-bold text-amber-900 mb-1">
                      MCP — חיבור ישיר ל-Claude / Cursor / סוכני AI
                    </h2>
                    <p className="text-sm text-amber-800">
                      גישה מובנית לדאטה דרך Model Context Protocol — ה-LLM יכול
                      לחפש, לטייל בגרף הקשרים ולצטט מסמכי מקור בלי לעבור דרך ה-API למעלה.
                    </p>
                  </div>
                  <span className="shrink-0 text-xs px-2 py-1 rounded-full bg-amber-200 text-amber-900 border border-amber-300 font-medium">
                    ביתא סגורה
                  </span>
                </div>

                {/* Request access */}
                <div className="bg-white/70 border border-amber-200 rounded-lg p-4 mb-4">
                  <h3 className="text-sm font-semibold text-amber-900 mb-1">
                    איך מקבלים גישה?
                  </h3>
                  <p className="text-sm text-amber-800">
                    שלחו אימייל ל-{" "}
                    <a
                      href="mailto:guy@z-g.co.il?subject=בקשת%20גישה%20ל-MCP%20של%20ניגוד%20עניינים%20לעם"
                      className="font-mono text-amber-900 underline decoration-amber-400 underline-offset-2 hover:decoration-amber-700"
                    >
                      guy@z-g.co.il
                    </a>{" "}
                    עם כתובת ה-Google שאתם רוצים לחבר ושורה־שתיים על מטרת השימוש (מחקר, עיתונאות, כלי משלכם וכו׳). ההזמנה ניתנת ידנית, בדרך כלל באותו יום.
                  </p>
                </div>

                {/* How to connect after approval */}
                <div className="bg-white/70 border border-amber-200 rounded-lg p-4">
                  <h3 className="text-sm font-semibold text-amber-900 mb-2">
                    אחרי שאישרנו את הכתובת — איך מתחברים מ-Claude
                  </h3>
                  <ol className="text-sm text-amber-800 space-y-2 list-decimal pr-5 marker:text-amber-700 marker:font-semibold">
                    <li>
                      ב-Claude (אפליקציית הדסקטופ או claude.ai בדפדפן) פתחו <strong>Settings</strong> → <strong>Connectors</strong>.
                    </li>
                    <li>
                      לחצו על הכפתור <strong>+ Add custom connector</strong> (או &quot;Personal plugins&quot; → <strong>+</strong>).
                    </li>
                    <li>
                      בשדה <em>Name</em> כתבו שם שתזהו (למשל &quot;ניגוד עניינים&quot;).<br/>
                      בשדה <em>URL</em> או <em>Server URL</em> הדביקו את הכתובת:
                      <div className="mt-2 flex items-center gap-2 flex-wrap">
                        <code className="bg-amber-900 text-amber-50 px-3 py-1.5 rounded font-mono text-sm" dir="ltr">
                          https://www.ocoi.org.il/mcp
                        </code>
                        <button
                          type="button"
                          onClick={() => navigator.clipboard?.writeText("https://www.ocoi.org.il/mcp")}
                          className="text-xs px-2 py-1 rounded border border-amber-300 bg-white text-amber-900 hover:bg-amber-50"
                        >
                          העתק
                        </button>
                      </div>
                    </li>
                    <li>
                      לחצו <strong>Connect</strong>. ייפתח חלון Google — התחברו <strong>עם אותה כתובת מייל</strong> שעליה ביקשתם הזמנה.
                    </li>
                    <li>
                      Google יחזיר אתכם אוטומטית ל-Claude. ה-connector יסומן כ-Connected, ובסרגל הכלים של השיחה יופיעו פעולות חדשות (חיפוש, מפת קשרים, מסמכי מקור).
                    </li>
                    <li>
                      פתחו שיחה חדשה ושאלו שאלה כמו{" "}
                      <em>&quot;חפש ב-OCOI את שם בעל התפקיד והצג קשרים&quot;</em> —
                      Claude יקרא אוטומטית לכלים, יצטט את מסמכי המקור, ויקשר חזרה לאתר.
                    </li>
                  </ol>
                  <p className="text-xs text-amber-700 mt-3">
                    אם החיבור נכשל עם &quot;Couldn&apos;t reach the MCP server&quot;: ודאו ש-URL מתחיל ב-<code className="bg-white px-1 rounded" dir="ltr">https://www.</code> (עם www) ולא רק <code className="bg-white px-1 rounded" dir="ltr">ocoi.org.il</code>.
                  </p>
                </div>
              </div>
            </section>

            {/* Intro / how-to-use section */}
            <section className="mb-8 space-y-4">
              <div className="bg-primary-50 border border-primary-200 rounded-lg p-4 text-sm space-y-2">
                <p className="text-primary-800">
                  <strong>כתובת בסיס:</strong>{" "}
                  <code className="bg-white px-2 py-0.5 rounded border border-primary-200 font-mono text-sm" dir="ltr">
                    {baseUrl}
                  </code>
                </p>
                <p className="text-primary-700">
                  כל ה־endpoints הם <strong>GET</strong> ציבוריים. <strong>אין צורך באימות</strong>, אין מפתח API.
                </p>
                <p className="text-primary-700">
                  השימוש פתוח לציבור הרחב — חוקרים, עיתונאים, אקטיביסטים ופרויקטים אחרים. אם אתם בונים אינטגרציה רחבה, אנא הימנעו ממיליוני קריאות מקבילות ושמרו מטמון מקומי.
                </p>
              </div>

              <details className="bg-white border border-gray-200 rounded-lg p-4 text-sm" open>
                <summary className="font-semibold text-gray-900 cursor-pointer">פורמט תשובה</summary>
                <div className="mt-3 space-y-2 text-gray-700">
                  <p>כל ה־endpoints מחזירים JSON במבנה אחיד:</p>
                  <CodeBlock>{`{
  "status": "ok",
  "data": <object | array | null>,
  "meta": { "total": 0, "page": 1, "limit": 20, "pages": 0 }   // ברשימות בלבד
}`}</CodeBlock>
                  <p>
                    שדה <code className="bg-gray-100 px-1 rounded">meta</code> מופיע רק ב־endpoints של רשימות מעומדות־דפים. עבור הורדת קבצים (PDF), התשובה היא binary stream עם <code className="bg-gray-100 px-1 rounded">Content-Type</code> מתאים.
                  </p>
                </div>
              </details>

              <details className="bg-white border border-gray-200 rounded-lg p-4 text-sm">
                <summary className="font-semibold text-gray-900 cursor-pointer">דפדוף (Pagination)</summary>
                <div className="mt-3 space-y-2 text-gray-700">
                  <p>endpoints של רשימות תומכים בשני פרמטרים:</p>
                  <ul className="list-disc pr-5 space-y-1">
                    <li><code className="bg-gray-100 px-1 rounded">page</code> — מספר עמוד (החל מ־1). ברירת מחדל: 1.</li>
                    <li><code className="bg-gray-100 px-1 rounded">limit</code> — תוצאות לעמוד. ברירת מחדל: 20. מקסימום: 100.</li>
                  </ul>
                  <p>השדה <code className="bg-gray-100 px-1 rounded">meta.pages</code> בתשובה אומר כמה עמודים סך הכל.</p>
                </div>
              </details>

              <details className="bg-white border border-gray-200 rounded-lg p-4 text-sm">
                <summary className="font-semibold text-gray-900 cursor-pointer">סוגי ישויות</summary>
                <div className="mt-3 space-y-2 text-gray-700">
                  <p>כל ישות במערכת היא אחת מ־4 הסוגים האלה:</p>
                  <ul className="list-disc pr-5 space-y-1">
                    <li><code className="bg-gray-100 px-1 rounded">person</code> — בעל תפקיד ציבורי (שר, ח״כ, מנכ״ל וכו׳)</li>
                    <li><code className="bg-gray-100 px-1 rounded">company</code> — חברה רשומה (ח.פ.)</li>
                    <li><code className="bg-gray-100 px-1 rounded">association</code> — עמותה רשומה</li>
                    <li><code className="bg-gray-100 px-1 rounded">domain</code> — תחום ניגוד עניינים</li>
                  </ul>
                  <p>שדה <code className="bg-gray-100 px-1 rounded">type</code> בפרמטרי חיפוש מקבל את אחד הערכים האלה.</p>
                </div>
              </details>

              <div className="bg-white border border-gray-200 rounded-lg p-4 text-sm flex items-center justify-between gap-3 flex-wrap">
                <div>
                  <p className="font-semibold text-gray-900">סכמת OpenAPI</p>
                  <p className="text-gray-600 text-xs mt-1">הורידו את הסכמה המלאה כקובץ JSON להכנסה לכלי כמו Postman / Insomnia / openapi-generator.</p>
                </div>
                <div className="flex gap-2">
                  <a
                    href="/api/public-openapi.json"
                    download="ocoi-openapi.json"
                    className="px-3 py-2 bg-primary-700 hover:bg-primary-800 text-white rounded-lg text-sm font-medium"
                  >
                    הורד OpenAPI
                  </a>
                  <a
                    href="/api/docs"
                    className="px-3 py-2 bg-white hover:bg-gray-50 text-primary-700 border border-primary-200 rounded-lg text-sm font-medium"
                  >
                    Swagger UI
                  </a>
                </div>
              </div>
            </section>

            {/* Tag tabs */}
            {tags.length > 1 && (
              <div className="flex flex-wrap gap-2 mb-3">
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

            {activeTag && TAG_DESCRIPTIONS[activeTag] && (
              <p className="text-sm text-gray-500 mb-5">{TAG_DESCRIPTIONS[activeTag]}</p>
            )}

            {/* Endpoint cards */}
            <h2 className="sr-only">{getTagLabel(activeTag)}</h2>
            <div className="space-y-3">
              {filteredEndpoints.map((ep) => (
                <EndpointCard
                  key={ep.anchorId}
                  endpoint={ep}
                  baseUrl={baseUrl}
                  defaultOpen={ep.anchorId === openAnchor}
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
