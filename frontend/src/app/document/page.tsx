"use client";

import { useEffect, useState, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { ConnectionMap } from "@/components/graph/ConnectionMap";
import { ConnectionTable } from "@/components/graph/ConnectionTable";
import { SuggestionDialog, SuggestButton, type SuggestionTarget } from "@/components/SuggestionDialog";
import type { SubGraph, ConnectionEdge } from "@/lib/api-client";

interface DocSummary {
  id: string;
  title: string;
  file_format: string;
  file_url: string | null;
  file_size: number | null;
  conversion_status: string;
  extraction_status: string;
}

const TYPE_LABEL: Record<string, string> = {
  person: "אדם",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

const TYPE_COLOR: Record<string, string> = {
  person: "bg-blue-100 text-blue-700",
  company: "bg-green-100 text-green-700",
  association: "bg-purple-100 text-purple-700",
  domain: "bg-amber-100 text-amber-700",
};

function DocumentContent() {
  const params = useSearchParams();
  const docId = params.get("id");
  const [doc, setDoc] = useState<DocSummary | null>(null);
  const [graph, setGraph] = useState<SubGraph | null>(null);
  const [loading, setLoading] = useState(true);
  const [showTable, setShowTable] = useState(false);
  const [suggestionTarget, setSuggestionTarget] = useState<SuggestionTarget | null>(null);
  // Default to split view on wide screens, stacked on narrow
  const [splitView, setSplitView] = useState<boolean>(() => {
    if (typeof window === "undefined") return true;
    return window.innerWidth >= 1024;
  });

  useEffect(() => {
    if (!docId) {
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    Promise.all([
      fetch(`/api/v1/documents/${docId}`).then((r) => r.json()),
      fetch(`/api/v1/documents/${docId}/graph`).then((r) => r.json()),
    ])
      .then(([docResp, graphResp]) => {
        if (cancelled) return;
        setDoc(docResp?.data ?? null);
        setGraph(graphResp?.data ?? null);
      })
      .catch(() => {
        if (cancelled) return;
        setDoc(null);
        setGraph(null);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [docId]);

  if (!docId) {
    return <div className="text-center py-12 text-red-500">מזהה מסמך חסר</div>;
  }
  if (loading) {
    return <div className="text-center py-12 text-gray-400">טוען...</div>;
  }
  if (!doc) {
    return <div className="text-center py-12 text-gray-400">המסמך לא נמצא</div>;
  }

  // Always serve the PDF through our own endpoint so the iframe is
  // same-origin and the X-Frame-Options header lets us embed it.
  const pdfSrc = `/api/v1/documents/${doc.id}/pdf`;
  const persons = (graph?.nodes || []).filter((n) => n.entity_type === "person");
  const otherEntities = (graph?.nodes || []).filter((n) => n.entity_type !== "person");

  const openSuggestion = (target: SuggestionTarget) => setSuggestionTarget(target);

  // Reusable cards
  const graphCard = (
    <div className="bg-white rounded-xl border border-gray-200 flex flex-col h-full">
      <div className="flex items-center justify-between p-4 border-b border-gray-200">
        <h2 className="text-lg font-semibold text-gray-900">
          מפת הקשרים שחולצה
          {graph && (
            <span className="mr-2 text-sm font-normal text-gray-500">
              ({graph.nodes.length} ישויות, {graph.edges.length} קשרים)
            </span>
          )}
        </h2>
        {graph && graph.edges.length > 0 && (
          <button
            onClick={() => setShowTable((v) => !v)}
            className="px-3 py-1.5 text-sm border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
          >
            {showTable ? "הסתר טבלה" : "הצג כטבלה"}
          </button>
        )}
      </div>
      <div className="flex-1 bg-gray-50 min-h-[420px]">
        {graph && graph.edges.length > 0 ? (
          <ConnectionMap
            graph={graph}
            onNodeClick={(nodeId, nodeType) => {
              window.location.href = `/entity?id=${nodeId}&type=${nodeType}`;
            }}
          />
        ) : (
          <div className="flex items-center justify-center h-full min-h-[420px] text-gray-400 text-sm">
            לא נמצאו קשרים שחולצו מהמסמך הזה
          </div>
        )}
      </div>
    </div>
  );

  const pdfCard = (
    <div className="bg-white rounded-xl border border-gray-200 flex flex-col h-full">
      <div className="flex items-center justify-between p-4 border-b border-gray-200">
        <h2 className="text-lg font-semibold text-gray-900">המסמך המקורי</h2>
        <a
          href={pdfSrc}
          target="_blank"
          rel="noopener noreferrer"
          className="text-sm text-primary-600 hover:underline"
        >
          פתח בלשונית חדשה ↗
        </a>
      </div>
      <div className="flex-1 min-h-[420px]" dir="ltr">
        <iframe
          src={pdfSrc}
          className="w-full h-full border-0"
          title={doc.title || "מסמך"}
        />
      </div>
    </div>
  );

  return (
    <div
      className={`mx-auto px-4 sm:px-6 lg:px-8 py-8 ${
        splitView ? "max-w-[1500px]" : "max-w-6xl"
      }`}
    >
      <div className="mb-4">
        <Link href="/documents" className="text-sm text-primary-600 hover:underline">
          ← חזרה לרשימת מסמכים
        </Link>
      </div>

      {/* Header */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 mb-6">
        <div className="flex items-start justify-between gap-4">
          <div className="flex-1">
            <div className="flex items-center gap-2 flex-wrap">
              <h1 className="text-2xl font-bold text-gray-900">
                {doc.title || "ללא כותרת"}
              </h1>
              <SuggestButton
                onClick={() =>
                  openSuggestion({
                    target_kind: "document",
                    target_id: doc.id,
                    field_name: "title",
                    field_label: "כותרת המסמך",
                    current_value: doc.title || "",
                    document_id: doc.id,
                  })
                }
              />
            </div>
            <div className="mt-2 text-sm text-gray-500 flex flex-wrap items-center gap-2">
              <span className="uppercase">{doc.file_format || "—"}</span>
              {doc.extraction_status === "extracted" && (
                <span className="px-1.5 py-0.5 rounded-full bg-green-50 text-green-700 border border-green-200 text-xs">
                  חולץ
                </span>
              )}
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={() => setSplitView((v) => !v)}
              className="hidden md:inline-flex items-center gap-1.5 px-3 py-2 rounded-lg border border-gray-300 text-sm text-gray-700 hover:bg-gray-50 transition-colors"
              title={splitView ? "תצוגה אנכית" : "תצוגה זו ליד זו"}
            >
              {splitView ? (
                <>
                  <svg xmlns="http://www.w3.org/2000/svg" className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <rect x="3" y="3" width="18" height="8" rx="2" />
                    <rect x="3" y="13" width="18" height="8" rx="2" />
                  </svg>
                  תצוגה אנכית
                </>
              ) : (
                <>
                  <svg xmlns="http://www.w3.org/2000/svg" className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <rect x="3" y="3" width="8" height="18" rx="2" />
                    <rect x="13" y="3" width="8" height="18" rx="2" />
                  </svg>
                  תצוגה זו ליד זו
                </>
              )}
            </button>
            <a
              href={pdfSrc}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-3 py-2 rounded-lg bg-primary-700 text-white text-sm font-medium hover:bg-primary-800 transition-colors"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="w-4 h-4"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                <polyline points="14 2 14 8 20 8" />
              </svg>
              פתח את ה-PDF
            </a>
          </div>
        </div>
      </div>

      {/* Side-by-side or stacked layout */}
      {splitView ? (
        <div
          className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6"
          style={{ height: "calc(100vh - 240px)", minHeight: 600 }}
        >
          {graphCard}
          {pdfCard}
        </div>
      ) : (
        <div className="space-y-4 mb-6">
          <div style={{ height: 500 }}>{graphCard}</div>
          <div style={{ height: 700 }}>{pdfCard}</div>
        </div>
      )}

      {/* Entities listing — always full width */}
      {graph && graph.nodes.length > 0 && (
        <div className="bg-white rounded-xl border border-gray-200 p-5 mb-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">ישויות במסמך</h2>

          {persons.length > 0 && (
            <div className="mb-4">
              <h3 className="text-sm font-medium text-gray-600 mb-2">בעלי תפקיד</h3>
              <ul className="space-y-1.5">
                {persons.map((p) => (
                  <li key={p.id} className="flex items-center gap-2 text-sm">
                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${TYPE_COLOR[p.entity_type]}`}>
                      {TYPE_LABEL[p.entity_type]}
                    </span>
                    <Link
                      href={`/entity?id=${p.id}&type=${p.entity_type}`}
                      className="font-medium text-gray-900 hover:text-primary-700"
                    >
                      {p.name}
                    </Link>
                    <SuggestButton
                      onClick={() =>
                        openSuggestion({
                          target_kind: "person",
                          target_id: p.id,
                          field_name: "name_hebrew",
                          field_label: `שם של ${p.name || "אדם"}`,
                          current_value: p.name,
                          document_id: doc.id,
                        })
                      }
                    />
                  </li>
                ))}
              </ul>
            </div>
          )}

          {otherEntities.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-600 mb-2">חברות / עמותות / תחומים</h3>
              <ul className="space-y-1.5">
                {otherEntities.map((e) => (
                  <li key={e.id} className="flex items-center gap-2 text-sm">
                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${TYPE_COLOR[e.entity_type] || "bg-gray-100 text-gray-700"}`}>
                      {TYPE_LABEL[e.entity_type] || e.entity_type}
                    </span>
                    <Link
                      href={`/entity?id=${e.id}&type=${e.entity_type}`}
                      className="font-medium text-gray-900 hover:text-primary-700"
                    >
                      {e.name}
                    </Link>
                    <SuggestButton
                      onClick={() =>
                        openSuggestion({
                          target_kind: e.entity_type as SuggestionTarget["target_kind"],
                          target_id: e.id,
                          field_name: "name_hebrew",
                          field_label: `שם של ${e.name || TYPE_LABEL[e.entity_type] || ""}`,
                          current_value: e.name,
                          document_id: doc.id,
                        })
                      }
                    />
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}

      {/* Relationships table view */}
      {showTable && graph && graph.edges.length > 0 && (
        <div className="bg-white rounded-xl border border-gray-200 p-5 mb-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-3">טבלת קשרים</h2>
          <ConnectionTable
            edges={graph.edges as ConnectionEdge[]}
            nodes={graph.nodes}
            caption={`קשרים שחולצו מהמסמך "${doc.title || ""}"`}
          />
        </div>
      )}

      {/* General comment box at the bottom */}
      <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 text-sm text-amber-900">
        <div className="flex items-center justify-between gap-3">
          <div>
            <p className="font-semibold">
              שמתם לב לטעות במסמך הזה?
            </p>
            <p className="text-xs mt-0.5">
              כל אחד מהשדות לעיל ניתן לסימון לתיקון בעזרת כפתור העריכה (✎) שלידו. ניתן גם להוסיף הערה כללית.
            </p>
          </div>
          <button
            onClick={() =>
              openSuggestion({
                target_kind: "document",
                target_id: doc.id,
                field_name: "general",
                field_label: "הערה כללית על המסמך",
                current_value: null,
                document_id: doc.id,
              })
            }
            className="shrink-0 px-3 py-2 rounded-lg bg-amber-600 text-white text-sm font-medium hover:bg-amber-700 transition-colors"
          >
            הערה כללית
          </button>
        </div>
      </div>

      <SuggestionDialog
        target={suggestionTarget}
        onClose={() => setSuggestionTarget(null)}
      />
    </div>
  );
}

export default function DocumentPage() {
  return (
    <Suspense fallback={<div className="text-center py-12 text-gray-400">טוען...</div>}>
      <DocumentContent />
    </Suspense>
  );
}
