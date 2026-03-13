"use client";

import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { reconvertDocument, reextractDocument } from "@/lib/admin-api";

const TYPE_LABELS: Record<string, string> = {
  person: "אדם",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

const TYPE_COLORS: Record<string, string> = {
  person: "bg-blue-100 text-blue-700",
  company: "bg-amber-100 text-amber-700",
  association: "bg-green-100 text-green-700",
  domain: "bg-purple-100 text-purple-700",
};

const TYPE_TO_TAB: Record<string, string> = {
  person: "persons",
  company: "companies",
  association: "associations",
  domain: "domains",
};

interface DocDetail {
  id: string;
  title: string;
  source_type: string | null;
  source_title: string | null;
  file_format: string;
  file_url: string | null;
  file_size: number | null;
  conversion_status: string;
  extraction_status: string;
  markdown_content: string;
  markdown_length: number;
  created_at: string | null;
  extraction_runs: ExtractionRun[];
  relationships: Relationship[];
  entities: Entity[];
}

interface ExtractionRun {
  id: string;
  extractor_type: string;
  model_version: string | null;
  entities_found: number;
  relationships_found: number;
  raw_output: Record<string, unknown> | null;
  created_at: string | null;
}

interface Relationship {
  id: string;
  entity1_name: string;
  entity1_type: string;
  entity2_name: string;
  entity2_type: string;
  relationship_type: string;
  details: string | null;
  confidence: number;
}

interface Entity {
  id: string;
  type: string;
  name: string;
}

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleDateString("he-IL", {
      day: "numeric", month: "numeric", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch { return iso; }
}

function formatSize(bytes: number | null) {
  if (!bytes) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

type Tab = "content" | "entities" | "extraction";

export default function DocumentDetailPage() {
  const searchParams = useSearchParams();
  const docId = searchParams.get("id");
  const [doc, setDoc] = useState<DocDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<Tab>("content");
  const [showRaw, setShowRaw] = useState<string | null>(null);
  const [reconverting, setReconverting] = useState(false);
  const [reextracting, setReextracting] = useState(false);
  const [actionMsg, setActionMsg] = useState<{ type: "ok" | "err"; text: string } | null>(null);

  const loadDoc = async () => {
    if (!docId) { setLoading(false); return; }
    try {
      const res = await fetch(`/api/v1/admin/documents/${docId}`, { credentials: "include" });
      const data = await res.json();
      setDoc(data.data);
    } catch {
      setDoc(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadDoc(); }, [docId]);

  const handleReconvert = async () => {
    if (!docId) return;
    setReconverting(true);
    setActionMsg(null);
    try {
      const res = await reconvertDocument(docId);
      setActionMsg({ type: "ok", text: `הומר מחדש — ${res.data.markdown_length.toLocaleString()} תווים` });
      await loadDoc();
    } catch (e) {
      setActionMsg({ type: "err", text: e instanceof Error ? e.message : "שגיאה בהמרה" });
    } finally {
      setReconverting(false);
    }
  };

  const handleReextract = async () => {
    if (!docId) return;
    setReextracting(true);
    setActionMsg(null);
    try {
      await reextractDocument(docId);
      setActionMsg({ type: "ok", text: "חילוץ מחדש הופעל — רענן את הדף בעוד מספר שניות" });
    } catch (e) {
      setActionMsg({ type: "err", text: e instanceof Error ? e.message : "שגיאה בחילוץ" });
    } finally {
      setReextracting(false);
    }
  };

  if (!docId) return <div className="text-red-500 py-8 text-center">מזהה מסמך חסר</div>;
  if (loading) return <div className="text-gray-400 py-8 text-center">טוען...</div>;
  if (!doc) return <div className="text-red-500 py-8 text-center">מסמך לא נמצא</div>;

  const statusColor: Record<string, string> = {
    pending: "bg-gray-100 text-gray-600",
    converted: "bg-green-100 text-green-700",
    no_text: "bg-amber-100 text-amber-700",
    extracted: "bg-blue-100 text-blue-700",
    failed: "bg-red-100 text-red-700",
  };

  const pdfUrl = doc.file_url && !doc.file_url.startsWith("upload://")
    ? doc.file_url
    : `/api/v1/admin/documents/${doc.id}/pdf`;

  const tabs: { key: Tab; label: string; count?: number }[] = [
    { key: "content", label: "תוכן המסמך" },
    { key: "entities", label: "ישויות וקשרים", count: doc.entities.length + doc.relationships.length },
    { key: "extraction", label: "הרצות חילוץ", count: doc.extraction_runs.length },
  ];

  return (
    <div>
      <div className="mb-4">
        <Link href="/admin/documents" className="text-sm text-primary-600 hover:underline">
          ← חזרה לרשימת מסמכים
        </Link>
      </div>

      {/* Header with title + actions */}
      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{doc.title || "מסמך"}</h1>
          <div className="flex items-center gap-3 mt-2 text-sm text-gray-500">
            <span>{doc.source_title || doc.source_type || "—"}</span>
            <span>·</span>
            <span>{formatSize(doc.file_size)}</span>
            <span>·</span>
            <span>{formatDate(doc.created_at)}</span>
            <span className={`inline-block px-2 py-0.5 rounded text-xs ${statusColor[doc.conversion_status] || "bg-gray-100 text-gray-600"}`}>
              {doc.conversion_status}
            </span>
            <span className={`inline-block px-2 py-0.5 rounded text-xs ${statusColor[doc.extraction_status] || "bg-gray-100 text-gray-600"}`}>
              {doc.extraction_status}
            </span>
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <button
            onClick={handleReconvert}
            disabled={reconverting}
            className="px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-sm font-medium disabled:opacity-50"
            title="המרה מחדש של ה-PDF לטקסט (תיקון RTL)"
          >
            {reconverting ? "ממיר..." : "המר מחדש"}
          </button>
          <button
            onClick={handleReextract}
            disabled={reextracting}
            className="px-3 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 transition-colors text-sm font-medium disabled:opacity-50"
            title="מחיקת ישויות קיימות וחילוץ מחדש באמצעות LLM"
          >
            {reextracting ? "מחלץ..." : "חלץ מחדש"}
          </button>
          <a
            href={pdfUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-3 py-2 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium"
          >
            <svg xmlns="http://www.w3.org/2000/svg" className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
              <polyline points="14 2 14 8 20 8" />
            </svg>
            PDF
          </a>
        </div>
      </div>

      {/* Action message */}
      {actionMsg && (
        <div className={`mb-4 p-3 rounded-lg text-sm ${actionMsg.type === "ok" ? "bg-green-50 text-green-800" : "bg-red-50 text-red-800"}`}>
          {actionMsg.text}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 mb-4 border-b border-gray-200">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              activeTab === t.key
                ? "border-primary-600 text-primary-700"
                : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
            }`}
          >
            {t.label}
            {t.count !== undefined && t.count > 0 && (
              <span className="mr-1.5 text-xs bg-gray-100 text-gray-600 px-1.5 py-0.5 rounded-full">
                {t.count}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Tab: Content */}
      {activeTab === "content" && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          {doc.markdown_content ? (
            <div>
              <div className="flex items-center justify-between mb-3">
                <h2 className="text-lg font-semibold text-gray-800">
                  תוכן מרקדאון ({doc.markdown_length.toLocaleString()} תווים)
                </h2>
              </div>
              <pre
                className="whitespace-pre-wrap text-sm text-gray-700 leading-relaxed bg-gray-50 rounded-lg p-4 max-h-[600px] overflow-y-auto border border-gray-100"
                dir="rtl"
              >
                {doc.markdown_content}
              </pre>
            </div>
          ) : (
            <div className="text-center py-12 text-gray-400">
              <svg xmlns="http://www.w3.org/2000/svg" className="w-12 h-12 mx-auto mb-3 text-gray-300" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                <polyline points="14 2 14 8 20 8" />
              </svg>
              <p className="text-sm">אין תוכן טקסט למסמך זה</p>
              <p className="text-xs mt-1">ייתכן שמדובר ב-PDF סרוק</p>
              <a
                href={pdfUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-block mt-4 px-4 py-2 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm"
              >
                צפה ב-PDF המקורי
              </a>
            </div>
          )}
        </div>
      )}

      {/* Tab: Entities & Relationships */}
      {activeTab === "entities" && (
        <div className="space-y-6">
          {/* Entities */}
          <div className="bg-white rounded-lg border border-gray-200 p-5">
            <h2 className="text-lg font-semibold text-gray-800 mb-3">
              ישויות ({doc.entities.length})
            </h2>
            {doc.entities.length === 0 ? (
              <p className="text-sm text-gray-400">לא נמצאו ישויות במסמך זה</p>
            ) : (
              <div className="flex flex-wrap gap-2">
                {doc.entities.map((e) => (
                  <Link
                    key={`${e.type}:${e.id}`}
                    href={`/admin/entities/detail?type=${TYPE_TO_TAB[e.type] || e.type}&id=${e.id}`}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-gray-200 hover:border-primary-300 hover:bg-primary-50 transition-colors"
                  >
                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${TYPE_COLORS[e.type] || "bg-gray-100 text-gray-700"}`}>
                      {TYPE_LABELS[e.type] || e.type}
                    </span>
                    <span className="text-sm font-medium text-gray-800">{e.name}</span>
                  </Link>
                ))}
              </div>
            )}
          </div>

          {/* Relationships */}
          <div className="bg-white rounded-lg border border-gray-200 p-5">
            <h2 className="text-lg font-semibold text-gray-800 mb-3">
              קשרים ({doc.relationships.length})
            </h2>
            {doc.relationships.length === 0 ? (
              <p className="text-sm text-gray-400">לא נמצאו קשרים במסמך זה</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 border-b border-gray-200">
                    <tr>
                      <th className="text-start px-3 py-2 font-medium text-gray-700">ישות 1</th>
                      <th className="text-start px-3 py-2 font-medium text-gray-700">סוג קשר</th>
                      <th className="text-start px-3 py-2 font-medium text-gray-700">ישות 2</th>
                      <th className="text-start px-3 py-2 font-medium text-gray-700">פרטים</th>
                      <th className="text-start px-3 py-2 font-medium text-gray-700 w-16">ביטחון</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {doc.relationships.map((r) => (
                      <tr key={r.id} className="hover:bg-gray-50">
                        <td className="px-3 py-2">
                          <div className="flex flex-col gap-0.5">
                            <span className="font-medium text-gray-900">{r.entity1_name}</span>
                            <span className={`text-[10px] px-1.5 py-0.5 rounded-full w-fit ${TYPE_COLORS[r.entity1_type] || "bg-gray-100 text-gray-700"}`}>
                              {TYPE_LABELS[r.entity1_type] || r.entity1_type}
                            </span>
                          </div>
                        </td>
                        <td className="px-3 py-2 text-gray-700">{r.relationship_type}</td>
                        <td className="px-3 py-2">
                          <div className="flex flex-col gap-0.5">
                            <span className="font-medium text-gray-900">{r.entity2_name}</span>
                            <span className={`text-[10px] px-1.5 py-0.5 rounded-full w-fit ${TYPE_COLORS[r.entity2_type] || "bg-gray-100 text-gray-700"}`}>
                              {TYPE_LABELS[r.entity2_type] || r.entity2_type}
                            </span>
                          </div>
                        </td>
                        <td className="px-3 py-2 text-gray-500 text-xs max-w-[200px] truncate" title={r.details || ""}>
                          {r.details || "—"}
                        </td>
                        <td className="px-3 py-2 text-gray-500 text-xs">
                          {Math.round(r.confidence * 100)}%
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Tab: Extraction Runs */}
      {activeTab === "extraction" && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h2 className="text-lg font-semibold text-gray-800 mb-3">
            הרצות חילוץ ({doc.extraction_runs.length})
          </h2>
          {doc.extraction_runs.length === 0 ? (
            <p className="text-sm text-gray-400">לא בוצעו הרצות חילוץ למסמך זה</p>
          ) : (
            <div className="space-y-3">
              {doc.extraction_runs.map((run) => (
                <div key={run.id} className="border border-gray-100 rounded-lg p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-3">
                      <span className="text-sm font-medium text-gray-800">{run.extractor_type}</span>
                      {run.model_version && (
                        <span className="text-xs text-gray-400">{run.model_version}</span>
                      )}
                    </div>
                    <span className="text-xs text-gray-400">{formatDate(run.created_at)}</span>
                  </div>
                  <div className="flex gap-4 text-xs text-gray-600 mb-2">
                    <span>{run.entities_found} ישויות</span>
                    <span>{run.relationships_found} קשרים</span>
                  </div>
                  {run.raw_output && (
                    <div>
                      <button
                        onClick={() => setShowRaw(showRaw === run.id ? null : run.id)}
                        className="text-xs text-primary-600 hover:underline"
                      >
                        {showRaw === run.id ? "הסתר פלט גולמי" : "הצג פלט גולמי"}
                      </button>
                      {showRaw === run.id && (
                        <pre className="mt-2 p-3 bg-gray-50 rounded text-xs text-gray-700 overflow-x-auto max-h-96 overflow-y-auto" dir="ltr">
                          {JSON.stringify(run.raw_output, null, 2)}
                        </pre>
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
