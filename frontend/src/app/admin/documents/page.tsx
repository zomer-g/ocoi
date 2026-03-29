"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import { deleteDocument, purgeMetadataOnlyDocuments, uploadDocument, backfillPdf, batchReconvert, batchExtract, batchResetStatus, type UploadResult } from "@/lib/admin-api";
import Link from "next/link";

interface DocItem {
  id: string;
  title: string | null;
  source_type: string | null;
  conversion_status: string | null;
  extraction_status: string | null;
  file_url: string | null;
  file_size: number | null;
  has_content: boolean;
  has_pdf: boolean;
  created_at: string | null;
  converted_at: string | null;
  extracted_at: string | null;
}

interface UploadItem {
  file: File;
  status: "pending" | "uploading" | "done" | "warning" | "error";
  progress: number;
  error?: string;
  result?: UploadResult;
}

const SOURCE_LABELS: Record<string, { label: string; color: string }> = {
  govil: { label: "Gov.il", color: "bg-blue-50 text-blue-700 border-blue-200" },
  ckan: { label: "CKAN", color: "bg-purple-50 text-purple-700 border-purple-200" },
  upload: { label: "העלאה", color: "bg-teal-50 text-teal-700 border-teal-200" },
};

const CONVERSION_BADGES: Record<string, { label: string; color: string }> = {
  pending: { label: "ממתין", color: "bg-gray-100 text-gray-600" },
  converted: { label: "הומר", color: "bg-green-50 text-green-700" },
  no_text: { label: "ללא טקסט", color: "bg-amber-50 text-amber-700" },
  failed: { label: "נכשל", color: "bg-red-50 text-red-700" },
};

const EXTRACTION_BADGES: Record<string, { label: string; color: string }> = {
  pending: { label: "ממתין", color: "bg-gray-100 text-gray-600" },
  extracted: { label: "חולץ", color: "bg-blue-50 text-blue-700" },
  failed: { label: "נכשל", color: "bg-red-50 text-red-700" },
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
  const [conversionFilter, setConversionFilter] = useState("");
  const [sourceFilter, setSourceFilter] = useState("");
  const [purging, setPurging] = useState(false);
  const [backfilling, setBackfilling] = useState(false);
  const [search, setSearch] = useState("");
  const [searchInput, setSearchInput] = useState("");

  // Selection state
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [batchLoading, setBatchLoading] = useState(false);

  // Upload state
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ page: String(page), limit: "50" });
      if (statusFilter) params.set("status", statusFilter);
      if (conversionFilter) params.set("conversion", conversionFilter);
      if (sourceFilter) params.set("source_type", sourceFilter);
      if (search) params.set("search", search);
      const res = await fetch(`/api/v1/admin/documents?${params}`, { credentials: "include" });
      const data = await res.json();
      setItems(data.data || []);
      setTotal(data.meta?.total || 0);
    } catch {
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [page, statusFilter, conversionFilter, sourceFilter, search]);

  useEffect(() => { fetchData(); }, [fetchData]);

  // Clear selection when filters change
  useEffect(() => { setSelectedIds(new Set()); }, [statusFilter, conversionFilter, sourceFilter, search, page]);

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

  // ── Selection ──────────────────────────────────────────────────
  const toggleSelect = (id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleSelectAll = () => {
    if (selectedIds.size === items.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(items.map((i) => i.id)));
    }
  };

  const handleBatchReconvert = async () => {
    setBatchLoading(true);
    try {
      const result = await batchReconvert(Array.from(selectedIds));
      alert(result.message);
      setSelectedIds(new Set());
      fetchData();
    } catch (e) {
      alert(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setBatchLoading(false);
    }
  };

  const handleBatchExtract = async () => {
    setBatchLoading(true);
    try {
      const result = await batchExtract(Array.from(selectedIds));
      alert(result.message);
      setSelectedIds(new Set());
      fetchData();
    } catch (e) {
      alert(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setBatchLoading(false);
    }
  };

  const handleBatchReset = async () => {
    setBatchLoading(true);
    try {
      await batchResetStatus(Array.from(selectedIds), "extraction_status", "pending");
      alert(`אופס ${selectedIds.size} מסמכים`);
      setSelectedIds(new Set());
      fetchData();
    } catch (e) {
      alert(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setBatchLoading(false);
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
        const scanned = result.data?.scanned;
        setUploads((prev) =>
          prev.map((u, j) =>
            j === idx ? {
              ...u,
              status: scanned ? "warning" : "done",
              progress: 100,
              result: result.data,
              error: scanned ? "PDF סרוק — הועלה ללא טקסט" : undefined,
            } : u
          )
        );
      } catch (e) {
        const msg = e instanceof Error ? e.message : "שגיאה";
        const isDuplicate = msg.includes("כבר קיים") || msg.includes("409");
        setUploads((prev) =>
          prev.map((u, j) =>
            j === idx
              ? { ...u, status: "error", error: isDuplicate ? `כבר קיים: ${pdfFiles[i].name}` : msg }
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

  const handleBackfill = async () => {
    if (!confirm("להוריד ולשמור PDFs חסרים עבור מסמכים קיימים? (רץ ברקע)")) return;
    setBackfilling(true);
    try {
      const result = await backfillPdf();
      alert(result.message);
    } catch (e) {
      alert(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setBackfilling(false);
    }
  };

  const metadataOnlyCount = items.filter((i) => !i.has_content).length;
  const activeUploads = uploads.filter((u) => u.status === "uploading" || u.status === "pending");

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-bold text-gray-900">ניהול מסמכים</h1>
        <div className="flex gap-2">
          <button
            onClick={handleBackfill}
            disabled={backfilling}
            className="px-3 py-1.5 text-xs font-medium rounded bg-blue-50 text-blue-700 border border-blue-200 hover:bg-blue-100 transition-colors disabled:opacity-50"
          >
            {backfilling ? "מוריד..." : "הורד PDFs חסרים"}
          </button>
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
                {u.status === "warning" && (
                  <span className="text-xs text-amber-600">{u.error || "PDF סרוק — הועלה ללא טקסט"}</span>
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

      {/* Search */}
      <div className="flex gap-2 mb-4">
        <input
          type="text"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && (() => { setSearch(searchInput.trim()); setPage(1); })()}
          placeholder="חיפוש מסמכים לפי שם..."
          className="flex-1 px-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:border-primary-500"
          dir="rtl"
        />
        <button onClick={() => { setSearch(searchInput.trim()); setPage(1); }} className="px-4 py-2 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium">חיפוש</button>
        {search && (
          <button onClick={() => { setSearch(""); setSearchInput(""); setPage(1); }} className="px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 text-sm text-gray-600">נקה</button>
        )}
      </div>

      {/* Filters */}
      <div className="space-y-2 mb-4">
        {/* Conversion status */}
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500 w-16 shrink-0">המרה:</span>
          <div className="flex gap-1">
            {[
              { value: "", label: "הכל" },
              { value: "pending", label: "ממתין" },
              { value: "converted", label: "הומר" },
              { value: "no_text", label: "ללא טקסט" },
              { value: "failed", label: "נכשל" },
            ].map((s) => (
              <button
                key={s.value}
                onClick={() => { setConversionFilter(s.value); setPage(1); }}
                className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${
                  conversionFilter === s.value ? "bg-primary-100 text-primary-700" : "text-gray-500 hover:bg-gray-100"
                }`}
              >
                {s.label}
              </button>
            ))}
          </div>
        </div>

        {/* Extraction status */}
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500 w-16 shrink-0">חילוץ:</span>
          <div className="flex gap-1">
            {[
              { value: "", label: "הכל" },
              { value: "pending", label: "ממתין" },
              { value: "extracted", label: "חולץ" },
              { value: "failed", label: "נכשל" },
            ].map((s) => (
              <button
                key={s.value}
                onClick={() => { setStatusFilter(s.value); setPage(1); }}
                className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${
                  statusFilter === s.value ? "bg-primary-100 text-primary-700" : "text-gray-500 hover:bg-gray-100"
                }`}
              >
                {s.label}
              </button>
            ))}
          </div>
        </div>

        {/* Source type */}
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500 w-16 shrink-0">מקור:</span>
          <div className="flex gap-1">
            {[
              { value: "", label: "הכל" },
              { value: "govil", label: "Gov.il" },
              { value: "ckan", label: "CKAN" },
              { value: "upload", label: "העלאה" },
            ].map((s) => (
              <button
                key={s.value}
                onClick={() => { setSourceFilter(s.value); setPage(1); }}
                className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${
                  sourceFilter === s.value ? "bg-primary-100 text-primary-700" : "text-gray-500 hover:bg-gray-100"
                }`}
              >
                {s.label}
              </button>
            ))}
          </div>
          <span className="text-xs text-gray-500 mr-auto">{total} מסמכים</span>
        </div>
      </div>

      {/* Batch action bar */}
      {selectedIds.size > 0 && (
        <div className="flex items-center gap-3 mb-4 p-3 bg-primary-50 border border-primary-200 rounded-lg">
          <span className="text-sm font-medium text-primary-800">{selectedIds.size} נבחרו</span>
          <button
            onClick={handleBatchReconvert}
            disabled={batchLoading}
            className="px-3 py-1.5 text-xs font-medium rounded bg-amber-100 text-amber-800 border border-amber-300 hover:bg-amber-200 transition-colors disabled:opacity-50"
          >
            המר מחדש ({selectedIds.size})
          </button>
          <button
            onClick={handleBatchExtract}
            disabled={batchLoading}
            className="px-3 py-1.5 text-xs font-medium rounded bg-blue-100 text-blue-800 border border-blue-300 hover:bg-blue-200 transition-colors disabled:opacity-50"
          >
            חלץ ישויות ({selectedIds.size})
          </button>
          <button
            onClick={handleBatchReset}
            disabled={batchLoading}
            className="px-3 py-1.5 text-xs font-medium rounded bg-gray-100 text-gray-700 border border-gray-300 hover:bg-gray-200 transition-colors disabled:opacity-50"
          >
            אפס סטטוס ({selectedIds.size})
          </button>
          <button
            onClick={() => setSelectedIds(new Set())}
            className="text-xs text-gray-500 hover:text-gray-700 mr-auto"
          >
            בטל בחירה
          </button>
        </div>
      )}

      {loading ? (
        <div className="text-gray-400 py-8 text-center">טוען...</div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="px-3 py-3 w-10">
                  <input
                    type="checkbox"
                    checked={items.length > 0 && selectedIds.size === items.length}
                    onChange={toggleSelectAll}
                    className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                  />
                </th>
                <th className="text-start px-4 py-3 font-medium text-gray-700">כותרת</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700 w-20">מקור</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700 w-20">המרה</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700 w-20">חילוץ</th>
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
                const conv = CONVERSION_BADGES[item.conversion_status || "pending"] || CONVERSION_BADGES.pending;
                const extr = EXTRACTION_BADGES[item.extraction_status || "pending"] || EXTRACTION_BADGES.pending;
                return (
                  <tr key={item.id} className={`hover:bg-gray-50 group ${!item.has_content && !item.has_pdf ? "opacity-50" : ""}`}>
                    <td className="px-3 py-3">
                      <input
                        type="checkbox"
                        checked={selectedIds.has(item.id)}
                        onChange={() => toggleSelect(item.id)}
                        className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                      />
                    </td>
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
                      <span className={`text-xs px-2 py-0.5 rounded-full ${conv.color}`}>
                        {conv.label}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full ${extr.color}`}>
                        {extr.label}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">
                      {formatDate(item.created_at)}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex gap-2 items-center justify-end">
                        <Link
                          href={`/admin/documents/detail?id=${item.id}`}
                          className="text-xs text-primary-600 hover:underline"
                        >
                          צפה
                        </Link>
                        {item.file_url && !item.file_url.startsWith("upload://") && (
                          <a
                            href={item.file_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-xs text-primary-600 hover:text-primary-800 flex items-center gap-1"
                            title="צפייה ב-PDF"
                          >
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
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">אין מסמכים</td></tr>
              )}
            </tbody>
          </table>
          {total > 20 && (
            <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200">
              <span className="text-xs text-gray-500">
                עמוד {page} מתוך {Math.ceil(total / 50)}
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
