"use client";

import { useEffect, useState, useCallback } from "react";
import { deleteDocument } from "@/lib/admin-api";

interface DocItem {
  id: string;
  title: string | null;
  source_type: string | null;
  conversion_status: string | null;
  extraction_status: string | null;
  file_url: string | null;
  file_size: number | null;
  created_at: string | null;
}

const SOURCE_LABELS: Record<string, { label: string; color: string }> = {
  govil: { label: "Gov.il", color: "bg-blue-50 text-blue-700 border-blue-200" },
  ckan: { label: "CKAN", color: "bg-purple-50 text-purple-700 border-purple-200" },
};

function formatDate(iso: string | null) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString("he-IL", {
    day: "numeric",
    month: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatSize(bytes: number | null) {
  if (!bytes) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export default function DocumentsPage() {
  const [items, setItems] = useState<DocItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [statusFilter, setStatusFilter] = useState("");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ page: String(page), limit: "20" });
      if (statusFilter) params.set("status", statusFilter);
      const res = await fetch(`/api/v1/admin/documents?${params}`, { credentials: "include" });
      const data = await res.json();
      setItems(data.data || []);
      setTotal(data.meta?.total || 0);
    } catch {
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [page, statusFilter]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleDelete = async (id: string) => {
    if (!confirm("למחוק את המסמך?")) return;
    await deleteDocument(id);
    fetchData();
  };

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-4">ניהול מסמכים</h1>

      <div className="flex items-center justify-between mb-4">
        <div className="flex gap-2">
          {["", "pending", "extracted", "failed"].map((s) => (
            <button
              key={s}
              onClick={() => { setStatusFilter(s); setPage(1); }}
              className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
                statusFilter === s ? "bg-primary-100 text-primary-700" : "text-gray-500 hover:bg-gray-100"
              }`}
            >
              {s === "" ? "הכל" : s === "pending" ? "ממתין" : s === "extracted" ? "חולץ" : "נכשל"}
            </button>
          ))}
        </div>
        <span className="text-xs text-gray-500">{total} מסמכים</span>
      </div>

      {loading ? (
        <div className="text-gray-400 py-8 text-center">טוען...</div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="text-start px-4 py-3 font-medium text-gray-700">כותרת</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700 w-20">מקור</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700 w-20">סטטוס</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700 w-36">תאריך ייבוא</th>
                <th className="px-4 py-3 w-28"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map((item) => {
                const source = SOURCE_LABELS[item.source_type || ""] || {
                  label: item.source_type || "—",
                  color: "bg-gray-50 text-gray-600 border-gray-200",
                };
                return (
                  <tr key={item.id} className="hover:bg-gray-50 group">
                    <td className="px-4 py-3">
                      <div className="text-gray-800 font-medium max-w-md truncate" title={item.title || ""}>
                        {item.title || "—"}
                      </div>
                      {item.file_size ? (
                        <span className="text-xs text-gray-400">{formatSize(item.file_size)}</span>
                      ) : null}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded border ${source.color}`}>
                        {source.label}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full ${
                        item.extraction_status === "extracted"
                          ? "bg-green-50 text-green-700"
                          : item.extraction_status === "failed"
                          ? "bg-red-50 text-red-700"
                          : "bg-yellow-50 text-yellow-700"
                      }`}>
                        {item.extraction_status === "pending" ? "ממתין"
                          : item.extraction_status === "extracted" ? "חולץ"
                          : item.extraction_status === "failed" ? "נכשל"
                          : item.extraction_status}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">
                      {formatDate(item.created_at)}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex gap-2 items-center justify-end">
                        {item.file_url && (
                          <a
                            href={item.file_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-xs text-primary-600 hover:text-primary-800 flex items-center gap-1"
                            title="צפייה ב-PDF"
                          >
                            <svg xmlns="http://www.w3.org/2000/svg" className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                              <polyline points="14 2 14 8 20 8" />
                              <line x1="16" y1="13" x2="8" y2="13" />
                              <line x1="16" y1="17" x2="8" y2="17" />
                              <polyline points="10 9 9 9 8 9" />
                            </svg>
                            PDF
                          </a>
                        )}
                        <button
                          onClick={() => handleDelete(item.id)}
                          className="text-xs text-red-500 hover:text-red-700 opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          מחק
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {items.length === 0 && (
                <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-400">אין מסמכים</td></tr>
              )}
            </tbody>
          </table>
          {total > 20 && (
            <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200">
              <span className="text-xs text-gray-500">
                עמוד {page} מתוך {Math.ceil(total / 20)}
              </span>
              <div className="flex gap-1">
                <button disabled={page <= 1} onClick={() => setPage((p) => p - 1)} className="px-3 py-1 text-xs rounded border disabled:opacity-50 hover:bg-gray-50">הקודם</button>
                <button disabled={page * 20 >= total} onClick={() => setPage((p) => p + 1)} className="px-3 py-1 text-xs rounded border disabled:opacity-50 hover:bg-gray-50">הבא</button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
