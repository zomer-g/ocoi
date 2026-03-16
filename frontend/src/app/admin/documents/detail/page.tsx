"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import {
  reconvertDocument,
  reextractDocument,
  deleteRelationship,
  deletePerson,
  deleteCompany,
  deleteAssociation,
  deleteDomain,
  createRelationship,
  replaceEntity,
  type RelationshipCreateData,
  type ReplaceEntityData,
} from "@/lib/admin-api";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "/api/v1";

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

const DELETE_ENTITY_FN: Record<string, (id: string) => Promise<unknown>> = {
  person: deletePerson,
  company: deleteCompany,
  association: deleteAssociation,
  domain: deleteDomain,
};

interface ProcessingStep {
  step: string;
  label: string;
  status: string;
  timestamp: string | null;
  details: string;
}

interface DocDetail {
  id: string;
  title: string;
  source_type: string | null;
  source_title: string | null;
  file_format: string;
  file_url: string | null;
  file_size: number | null;
  has_pdf: boolean;
  pdf_size: number | null;
  conversion_status: string;
  extraction_status: string;
  markdown_content: string;
  markdown_length: number;
  created_at: string | null;
  converted_at: string | null;
  extracted_at: string | null;
  processing_log: ProcessingStep[];
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

interface LookupResult {
  id: string;
  entity_type: string;
  name_hebrew: string;
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

const STEP_ICONS: Record<string, string> = {
  import: "📥",
  storage: "💾",
  conversion: "📝",
  extraction: "🔍",
};

const STEP_STATUS_COLORS: Record<string, string> = {
  pending: "bg-gray-100 text-gray-500 border-gray-300",
  failed: "bg-red-50 text-red-600 border-red-300",
  missing: "bg-orange-50 text-orange-600 border-orange-300",
  ckan: "bg-purple-50 text-purple-700 border-purple-300",
  govil: "bg-blue-50 text-blue-700 border-blue-300",
  upload: "bg-teal-50 text-teal-700 border-teal-300",
  unknown: "bg-gray-100 text-gray-500 border-gray-300",
  stored: "bg-green-50 text-green-700 border-green-300",
  converted: "bg-green-50 text-green-700 border-green-300",
  no_text: "bg-amber-50 text-amber-700 border-amber-300",
  extracted: "bg-blue-50 text-blue-700 border-blue-300",
};

/* ── Entity Autocomplete ──────────────────────────────────────────── */

interface SelectedEntity {
  id: string;
  type: string;
  name: string;
}

function EntityAutocomplete({
  value,
  onChange,
  placeholder,
}: {
  value: SelectedEntity | null;
  onChange: (e: SelectedEntity | null) => void;
  placeholder?: string;
}) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<LookupResult[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const wrapperRef = useRef<HTMLDivElement>(null);

  const search = useCallback(async (q: string) => {
    if (q.length < 2) { setResults([]); return; }
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/lookup?q=${encodeURIComponent(q)}&limit=10`, { credentials: "include" });
      const data = await res.json();
      setResults(data.data || []);
      setOpen(true);
    } catch {
      setResults([]);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleInput = (text: string) => {
    setQuery(text);
    onChange(null);
    clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => search(text), 300);
  };

  const pick = (r: LookupResult) => {
    onChange({ id: r.id, type: r.entity_type, name: r.name_hebrew });
    setQuery(r.name_hebrew);
    setOpen(false);
  };

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  return (
    <div ref={wrapperRef} className="relative">
      <input
        type="text"
        value={value ? value.name : query}
        onChange={(e) => handleInput(e.target.value)}
        onFocus={() => results.length > 0 && setOpen(true)}
        placeholder={placeholder || "חפש ישות..."}
        className="w-full px-2 py-1.5 text-sm border border-gray-300 rounded focus:ring-1 focus:ring-primary-500 focus:border-primary-500"
      />
      {value && (
        <button
          onClick={() => { onChange(null); setQuery(""); }}
          className="absolute left-1 top-1/2 -translate-y-1/2 text-gray-400 hover:text-red-500 text-xs px-1"
          title="נקה"
        >
          ✕
        </button>
      )}
      {loading && (
        <span className="absolute left-6 top-1/2 -translate-y-1/2 text-[10px] text-gray-400">...</span>
      )}
      {open && results.length > 0 && (
        <div className="absolute z-50 top-full mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-lg max-h-48 overflow-y-auto">
          {results.map((r) => (
            <button
              key={`${r.entity_type}:${r.id}`}
              onClick={() => pick(r)}
              className="w-full text-start px-3 py-2 text-sm hover:bg-primary-50 flex items-center gap-2"
            >
              <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${TYPE_COLORS[r.entity_type] || "bg-gray-100 text-gray-700"}`}>
                {TYPE_LABELS[r.entity_type] || r.entity_type}
              </span>
              <span className="text-gray-800">{r.name_hebrew}</span>
            </button>
          ))}
        </div>
      )}
      {open && query.length >= 2 && results.length === 0 && !loading && (
        <div className="absolute z-50 top-full mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-lg p-3 text-sm text-gray-400 text-center">
          לא נמצאו תוצאות
        </div>
      )}
    </div>
  );
}

/* ── Main Page ─────────────────────────────────────────────────── */

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

  // Add-relationship form state
  const [showAddRel, setShowAddRel] = useState(false);
  const [newRelEntity1, setNewRelEntity1] = useState<SelectedEntity | null>(null);
  const [newRelEntity2, setNewRelEntity2] = useState<SelectedEntity | null>(null);
  const [newRelType, setNewRelType] = useState("");
  const [newRelDetails, setNewRelDetails] = useState("");
  const [newRelConfidence, setNewRelConfidence] = useState("0.8");
  const [saving, setSaving] = useState(false);

  // Replace entity state
  const [replacingEntity, setReplacingEntity] = useState<Entity | null>(null);
  const [replaceTarget, setReplaceTarget] = useState<SelectedEntity | null>(null);

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

  const handleDeleteRelationship = async (relId: string) => {
    if (!confirm("למחוק קשר זה?")) return;
    try {
      await deleteRelationship(relId);
      setActionMsg({ type: "ok", text: "הקשר נמחק" });
      await loadDoc();
    } catch (e) {
      setActionMsg({ type: "err", text: e instanceof Error ? e.message : "שגיאה במחיקת קשר" });
    }
  };

  const handleDeleteEntity = async (entity: Entity) => {
    const fn = DELETE_ENTITY_FN[entity.type];
    if (!fn) return;
    if (!confirm(`למחוק את "${entity.name}"?\nשימו לב: כל הקשרים של ישות זו יימחקו גם כן.`)) return;
    try {
      await fn(entity.id);
      setActionMsg({ type: "ok", text: `"${entity.name}" נמחק/ה` });
      await loadDoc();
    } catch (e) {
      setActionMsg({ type: "err", text: e instanceof Error ? e.message : "שגיאה במחיקת ישות" });
    }
  };

  const handleAddRelationship = async () => {
    if (!doc || !newRelEntity1 || !newRelEntity2 || !newRelType.trim()) return;
    setSaving(true);
    try {
      const data: RelationshipCreateData = {
        source_entity_type: newRelEntity1.type,
        source_entity_id: newRelEntity1.id,
        target_entity_type: newRelEntity2.type,
        target_entity_id: newRelEntity2.id,
        relationship_type: newRelType.trim(),
        details: newRelDetails.trim() || null,
        document_id: doc.id,
        confidence: parseFloat(newRelConfidence) || 0.8,
      };
      await createRelationship(data);
      setActionMsg({ type: "ok", text: "קשר חדש נוצר" });
      // Reset form
      setNewRelEntity1(null);
      setNewRelEntity2(null);
      setNewRelType("");
      setNewRelDetails("");
      setNewRelConfidence("0.8");
      setShowAddRel(false);
      await loadDoc();
    } catch (e) {
      setActionMsg({ type: "err", text: e instanceof Error ? e.message : "שגיאה ביצירת קשר" });
    } finally {
      setSaving(false);
    }
  };

  const handleReplaceEntity = async () => {
    if (!doc || !replacingEntity || !replaceTarget) return;
    const data: ReplaceEntityData = {
      old_entity_type: replacingEntity.type,
      old_entity_id: replacingEntity.id,
      new_entity_type: replaceTarget.type,
      new_entity_id: replaceTarget.id,
      document_id: doc.id,
    };
    try {
      const res = await replaceEntity(data) as { updated: number };
      setActionMsg({ type: "ok", text: `הוחלפו ${res.updated} קשרים מ-"${replacingEntity.name}" ל-"${replaceTarget.name}"` });
      setReplacingEntity(null);
      setReplaceTarget(null);
      await loadDoc();
    } catch (e) {
      setActionMsg({ type: "err", text: e instanceof Error ? e.message : "שגיאה בהחלפת ישות" });
    }
  };

  if (!docId) return <div className="text-red-500 py-8 text-center">מזהה מסמך חסר</div>;
  if (loading) return <div className="text-gray-400 py-8 text-center">טוען...</div>;
  if (!doc) return <div className="text-red-500 py-8 text-center">מסמך לא נמצא</div>;

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
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
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

      {/* Processing Timeline */}
      <div className="bg-white rounded-lg border border-gray-200 p-5 mb-6">
        <h2 className="text-sm font-semibold text-gray-700 mb-4">תהליך עיבוד</h2>
        <div className="flex items-start gap-0">
          {doc.processing_log.map((step, i) => {
            const colors = STEP_STATUS_COLORS[step.status] || STEP_STATUS_COLORS.pending;
            const isComplete = !["pending", "failed", "missing"].includes(step.status);
            const isFailed = step.status === "failed";
            const canRerun = step.step === "conversion" || step.step === "extraction";

            return (
              <div key={step.step} className="flex-1 relative">
                {i > 0 && (
                  <div className={`absolute top-5 -right-0 w-full h-0.5 -z-10 ${isComplete ? "bg-green-300" : "bg-gray-200"}`} />
                )}
                <div className="flex flex-col items-center text-center px-2">
                  <div className={`w-10 h-10 rounded-full border-2 flex items-center justify-center text-lg ${colors}`}>
                    {STEP_ICONS[step.step] || "•"}
                  </div>
                  <span className="text-xs font-medium text-gray-700 mt-2">{step.label}</span>
                  <span className={`text-[10px] px-2 py-0.5 rounded-full mt-1 ${colors}`}>
                    {step.status === "stored" ? "נשמר" :
                     step.status === "missing" ? "חסר" :
                     step.status === "converted" ? "הומר" :
                     step.status === "no_text" ? "ללא טקסט" :
                     step.status === "extracted" ? "חולץ" :
                     step.status === "pending" ? "ממתין" :
                     step.status === "failed" ? "נכשל" :
                     step.status}
                  </span>
                  <span className="text-[10px] text-gray-400 mt-1 max-w-[120px] truncate" title={step.details}>
                    {step.details}
                  </span>
                  {step.timestamp && (
                    <span className="text-[10px] text-gray-300 mt-0.5">{formatDate(step.timestamp)}</span>
                  )}
                  {canRerun && (
                    <button
                      onClick={step.step === "conversion" ? handleReconvert : handleReextract}
                      disabled={step.step === "conversion" ? reconverting : reextracting}
                      className={`mt-2 px-2 py-1 text-[10px] font-medium rounded transition-colors disabled:opacity-50 ${
                        isFailed
                          ? "bg-red-100 text-red-700 hover:bg-red-200"
                          : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                      }`}
                    >
                      {step.step === "conversion"
                        ? (reconverting ? "ממיר..." : "המר מחדש")
                        : (reextracting ? "מחלץ..." : "חלץ מחדש")}
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>

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
                  <div
                    key={`${e.type}:${e.id}`}
                    className="group flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-gray-200 hover:border-primary-300 hover:bg-primary-50 transition-colors"
                  >
                    <Link
                      href={`/admin/entities/detail?type=${TYPE_TO_TAB[e.type] || e.type}&id=${e.id}`}
                      className="flex items-center gap-1.5"
                    >
                      <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${TYPE_COLORS[e.type] || "bg-gray-100 text-gray-700"}`}>
                        {TYPE_LABELS[e.type] || e.type}
                      </span>
                      <span className="text-sm font-medium text-gray-800">{e.name}</span>
                    </Link>
                    <button
                      onClick={() => { setReplacingEntity(e); setReplaceTarget(null); }}
                      className="opacity-0 group-hover:opacity-100 mr-0.5 text-gray-400 hover:text-primary-600 transition-opacity text-xs leading-none"
                      title="החלף ישות"
                    >
                      <svg xmlns="http://www.w3.org/2000/svg" className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="17 1 21 5 17 9" /><path d="M3 11V9a4 4 0 0 1 4-4h14" /><polyline points="7 23 3 19 7 15" /><path d="M21 13v2a4 4 0 0 1-4 4H3" /></svg>
                    </button>
                    <button
                      onClick={() => handleDeleteEntity(e)}
                      className="opacity-0 group-hover:opacity-100 mr-0.5 text-gray-400 hover:text-red-600 transition-opacity text-xs leading-none"
                      title="מחק ישות"
                    >
                      ✕
                    </button>
                  </div>
                ))}
              </div>
            )}
            {/* Replace entity inline form */}
            {replacingEntity && (
              <div className="mt-3 p-3 bg-amber-50 border border-amber-200 rounded-lg">
                <div className="flex items-center gap-2 mb-2 text-sm font-medium text-amber-800">
                  <svg xmlns="http://www.w3.org/2000/svg" className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="17 1 21 5 17 9" /><path d="M3 11V9a4 4 0 0 1 4-4h14" /><polyline points="7 23 3 19 7 15" /><path d="M21 13v2a4 4 0 0 1-4 4H3" /></svg>
                  החלפת &quot;{replacingEntity.name}&quot; בישות אחרת
                </div>
                <div className="flex items-center gap-2">
                  <div className="flex-1">
                    <EntityAutocomplete value={replaceTarget} onChange={setReplaceTarget} placeholder="חפש ישות חלופית..." />
                  </div>
                  <button
                    onClick={handleReplaceEntity}
                    disabled={!replaceTarget}
                    className="px-3 py-1.5 text-sm font-medium bg-amber-600 text-white rounded-lg hover:bg-amber-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    החלף
                  </button>
                  <button
                    onClick={() => { setReplacingEntity(null); setReplaceTarget(null); }}
                    className="px-3 py-1.5 text-sm font-medium text-gray-600 hover:text-gray-800 transition-colors"
                  >
                    ביטול
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Relationships */}
          <div className="bg-white rounded-lg border border-gray-200 p-5">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-lg font-semibold text-gray-800">
                קשרים ({doc.relationships.length})
              </h2>
              <button
                onClick={() => setShowAddRel(!showAddRel)}
                className="px-3 py-1.5 text-sm font-medium bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors"
              >
                {showAddRel ? "ביטול" : "הוסף קשר"}
              </button>
            </div>

            {/* Add relationship form */}
            {showAddRel && (
              <div className="mb-4 p-4 bg-gray-50 rounded-lg border border-gray-200 space-y-3">
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">ישות 1</label>
                    <EntityAutocomplete value={newRelEntity1} onChange={setNewRelEntity1} placeholder="חפש ישות מקור..." />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">ישות 2</label>
                    <EntityAutocomplete value={newRelEntity2} onChange={setNewRelEntity2} placeholder="חפש ישות יעד..." />
                  </div>
                </div>
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">סוג קשר</label>
                    <input
                      type="text"
                      value={newRelType}
                      onChange={(e) => setNewRelType(e.target.value)}
                      placeholder="לדוגמה: employed_by, board_member..."
                      className="w-full px-2 py-1.5 text-sm border border-gray-300 rounded focus:ring-1 focus:ring-primary-500 focus:border-primary-500"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">פרטים (אופציונלי)</label>
                    <input
                      type="text"
                      value={newRelDetails}
                      onChange={(e) => setNewRelDetails(e.target.value)}
                      className="w-full px-2 py-1.5 text-sm border border-gray-300 rounded focus:ring-1 focus:ring-primary-500 focus:border-primary-500"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">ביטחון (0-1)</label>
                    <input
                      type="number"
                      min="0"
                      max="1"
                      step="0.1"
                      value={newRelConfidence}
                      onChange={(e) => setNewRelConfidence(e.target.value)}
                      className="w-full px-2 py-1.5 text-sm border border-gray-300 rounded focus:ring-1 focus:ring-primary-500 focus:border-primary-500"
                    />
                  </div>
                </div>
                <div className="flex gap-2 justify-end">
                  <button
                    onClick={() => setShowAddRel(false)}
                    className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800"
                  >
                    ביטול
                  </button>
                  <button
                    onClick={handleAddRelationship}
                    disabled={saving || !newRelEntity1 || !newRelEntity2 || !newRelType.trim()}
                    className="px-4 py-1.5 text-sm font-medium bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors disabled:opacity-50"
                  >
                    {saving ? "שומר..." : "שמור קשר"}
                  </button>
                </div>
              </div>
            )}

            {doc.relationships.length === 0 && !showAddRel ? (
              <p className="text-sm text-gray-400">לא נמצאו קשרים במסמך זה</p>
            ) : doc.relationships.length > 0 && (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 border-b border-gray-200">
                    <tr>
                      <th className="text-start px-3 py-2 font-medium text-gray-700">ישות 1</th>
                      <th className="text-start px-3 py-2 font-medium text-gray-700">סוג קשר</th>
                      <th className="text-start px-3 py-2 font-medium text-gray-700">ישות 2</th>
                      <th className="text-start px-3 py-2 font-medium text-gray-700">פרטים</th>
                      <th className="text-start px-3 py-2 font-medium text-gray-700 w-16">ביטחון</th>
                      <th className="w-10"></th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {doc.relationships.map((r) => (
                      <tr key={r.id} className="hover:bg-gray-50 group">
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
                        <td className="px-3 py-2">
                          <button
                            onClick={() => handleDeleteRelationship(r.id)}
                            className="opacity-0 group-hover:opacity-100 text-gray-400 hover:text-red-600 transition-opacity"
                            title="מחק קשר"
                          >
                            <svg xmlns="http://www.w3.org/2000/svg" className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                              <polyline points="3 6 5 6 21 6" />
                              <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                            </svg>
                          </button>
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
