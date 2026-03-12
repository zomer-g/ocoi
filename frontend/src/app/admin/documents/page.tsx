"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import { deleteDocument, purgeMetadataOnlyDocuments, uploadDocument, type UploadResult } from "@/lib/admin-api";

interface DocItem {
  id: string;
  title: string | null;
  source_type: string | null;
  conversion_status: string | null;
  extraction_status: string | null;
  file_url: string | null;
  file_size: number | null;
  has_content: boolean;
  created_at: string | null;
}

interface UploadItem {
  file: File;
  status: "pending" | "uploading" | "done" | "error";
  progress: number;
  error?: string;
  result?: UploadResult;
}

const SOURCE_LABELS: Record<string, { label: string; color: string }> = {
  govil: { label: "Gov.il", color: "bg-blue-50 text-blue-700 border-blue-200" },
  ckan: { label: "CKAN", color: "bg-purple-50 text-purple-700 border-purple-200" },
  upload: { label: "העלאה", color: "bg-teal-50 text-teal-700 border-teal-200" },
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
  const [purging, setPurging] = useState(false);

  // Upload state
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

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

  const handlePurge = async () => {
    if (!confirm("למחוק את כל המסמכים שאין להם תוכן בפועל (רק מטאדאטה)?")) return;
    setPurging(true);
    try {
      const result = await purgeMetadataOnlyDocuments();
      alert(`נמחקו ${result.data.deleted} מסמכים ללא תוכן`);
      setPage(1);
      fetchData();
    } catch {
      alert("שגיאה במחיקה");
    } finally {
      setPurging(false);
    }
  };

  // ── Upload handling ──────────────────────────────────────────────────
  const handleFilesSelected = async (files: FileList | File[]) => {
    const pdfFiles = Array.from(files).filter(
      (f) => f.type === "application/pdf" || f.name.toLowerCase().endsWith(".pdf")
    );
    if (pdfFiles.length === 0) return;

    const startIdx = uploads.length;
    const newUploads: UploadItem[] = pdfFiles.map((f) => ({
      file: f,
      status: "pending" as const,
      progress: 0,
    }));
    setUploads((prev) => [...prev, ...newUploads]);

    // Upload sequentially (Render memory-friendly)
    for (let i = 0; i < pdfFiles.length; i++) {
      const idx = startIdx + i;
      setUploads((prev) =>
        prev.map((u, j) => (j === idx ? { ...u, status: "uploading" } : u))
      );

      try {
        const result = await uploadDocument(pdfFiles[i], (loaded, total) => {
          const progress = Math.round((loaded / total) * 100);
          setUploads((prev) =>
            prev.map((u, j) => (j === idx ? { ...u, progress } : u))
          );
        });
        setUploads((prev) =>
          prev.map((u, j) =>
            j === idx ? { ...u, status: "done", progress: 100, result: result.data } : u
          )
        );
      } catch (e) {
        setUploads((prev) =>
          prev.map((u, j) =>
            j === idx
              ? { ...u, status: "error", error: e instanceof Error ? e.message : "שגיאה" }
              : u
          )
        );
      }
    }

    fetchData();
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);
    if (e.dataTransfer.files.length > 0) {
      handleFilesSelected(e.dataTransfer.files);
    }
  };

  const metadataOnlyCount = items.filter((i) => !i.has_content).length;
  const activeUploads = uploads.filter((u) => u.status === "uploading" || u.status === "pending");

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-bold text-gray-900">ניהול מסמכים</h1>
        {metadataOnlyCount > 0 && (
          <button
            onClick={handlePurge}
            disabled={purging}
            className="px-3 py-1.5 text-xs font-medium rounded bg-red-50 text-red-700 border border-red-200 hover:bg-red-100 transition-colors disabled:opacity-50"
          >
            {purging ? "מוחק..." : `מחק מסמכים ללא תוכן (${metadataOnlyCount})`}
          </button>
        )}
      </div>

      {/* Drop zone */}
      <div
        onDragOver={(e) => { e.preventDefault(); setIsDragOver(true); }}
        onDragEnter={(e) => { e.preventDefault(); setIsDragOver(true); }}
        onDragLeave={() => setIsDragOver(false)}
        onDrop={handleDrop}
        onClick={() => fileInputRef.current?.click()}
        className={`mb-4 border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors ${
          isDragOver
            ? "border-primary-500 bg-primary-50"
            : "border-gray-300 bg-gray-50 hover:border-primary-400 hover:bg-primary-50/50"
        }`}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept=".pdf"
          multiple
          className="hidden"
          onChange={(e) => {
            if (e.target.files && e.target.files.length > 0) {
              handleFilesSelected(e.target.files);
              e.target.value = "";
            }
          }}
        />
        <div className="text-gray-500">
          <svg xmlns="http://www.w3.org/2000/svg" className="w-8 h-8 mx-auto mb-2 text-gray-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
            <polyline points="14 2 14 8 20 8" />
            <line x1="12" y1="18" x2="12" y2="12" />
            <line x1="9" y1="15" x2="12" y2="12" />
            <line x1="15" y1="15" x2="12" y2="12" />
          </svg>
          {isDragOver ? (
            <span className="text-sm font-medium text-primary-700">שחרר להעלאה</span>
          ) : (
            <span className="text-sm">גרור קבצי PDF לכאן או לחץ לבחירה</span>
          )}
        </div>
      </div>

      {/* Upload progress list */}
      {uploads.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 mb-4 divide-y divide-gray-100">
          {uploads.map((u, i) => (
            <div key={i} className="flex items-center gap-3 px-4 py-2.5">
              <span className="text-sm text-gray-700 truncate flex-1 min-w-0" title={u.file.name}>
                {u.file.name}
              </span>
              <span className="text-xs text-gray-400 shrink-0">{formatSize(u.file.size)}</span>
              <div className="w-32 shrink-0">
                {u.status === "pending" && (
                  <span className="text-xs text-gray-400">ממתין...</span>
                )}
                {u.status === "uploading" && (
                  <div className="flex items-center gap-2">
                    <div className="flex-1 bg-gray-200 rounded-full h-1.5">
                      <div
                        className="bg-primary-600 h-1.5 rounded-full transition-all"
                        style={{ width: `${u.progress}%` }}
                      />
                    </div>
                    <span className="text-xs text-gray-500 w-8">{u.progress}%</span>
                  </div>
                )}
                {u.status === "done" && (
                  <span className="text-xs text-green-600">
                    {u.result ? `${u.result.markdown_length.toLocaleString()} תווים` : "הועלה"}
                  </span>
                )}
                {u.status === "error" && (
                  <span className="text-xs text-red-600" title={u.error}>{u.error}</span>
                )}
              </div>
            </div>
          ))}
          {activeUploads.length === 0 && (
            <div className="px-4 py-2 text-end">
              <button
                onClick={() => setUploads([])}
                className="text-xs text-gray-400 hover:text-gray-600"
              >
                נקה רשימה
              </button>
            </div>
          )}
        </div>
      )}

      <div className="flex items-center justify-between mb-4">
        <div className="flex gap-2">
          {["", "pending", "converted", "extracted", "failed"].map((s) => (
            <button
              key={s}
              onClick={() => { setStatusFilter(s); setPage(1); }}
              className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
                statusFilter === s ? "bg-primary-100 text-primary-700" : "text-gray-500 hover:bg-gray-100"
              }`}
            >
              {s === "" ? "הכל" : s === "pending" ? "ממתין" : s === "converted" ? "הומר" : s === "extracted" ? "חולץ" : "נכשל"}
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
                <th className="text-start px-4 py-3 font-medium text-gray-700 w-24">תוכן</th>
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
                  <tr key={item.id} className={`hover:bg-gray-50 group ${!item.has_content ? "opacity-50" : ""}`}>
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
                      {item.has_content ? (
                        <span className="text-xs px-2 py-0.5 rounded-full bg-green-50 text-green-700">
                          {item.conversion_status === "converted" ? "PDF + MD" : "יש תוכן"}
                        </span>
                      ) : (
                        <span className="text-xs px-2 py-0.5 rounded-full bg-orange-50 text-orange-600">
                          מטאדאטה בלבד
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">
                      {formatDate(item.created_at)}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex gap-2 items-center justify-end">
                        {item.file_url && !item.file_url.startsWith("upload://") && (
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
