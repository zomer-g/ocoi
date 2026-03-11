"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import {
  searchCkan,
  importCkanDatasets,
  triggerGovilImport,
  getImportStatus,
  type CkanSearchResult,
  type ImportStatus,
  type ImportStats,
} from "@/lib/admin-api";

type Tab = "ckan" | "govil";

export default function ImportPage() {
  const [tab, setTab] = useState<Tab>("ckan");

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">ייבוא מסמכים</h1>

      {/* Tab buttons */}
      <div className="flex gap-2 mb-6">
        <button
          onClick={() => setTab("ckan")}
          className={`px-5 py-2.5 rounded-lg font-medium text-sm transition-colors ${
            tab === "ckan" ? "bg-primary-700 text-white" : "bg-white border border-gray-200 text-gray-700 hover:bg-gray-50"
          }`}
        >
          CKAN (odata.org.il)
        </button>
        <button
          onClick={() => setTab("govil")}
          className={`px-5 py-2.5 rounded-lg font-medium text-sm transition-colors ${
            tab === "govil" ? "bg-primary-700 text-white" : "bg-white border border-gray-200 text-gray-700 hover:bg-gray-50"
          }`}
        >
          Gov.il
        </button>
      </div>

      {tab === "ckan" ? <CkanTab /> : <GovilTab />}
    </div>
  );
}

// ── CKAN Tab: Search + Select + Import ──────────────────────────────────

function CkanTab() {
  const [query, setQuery] = useState("ניגוד עניינים");
  const [results, setResults] = useState<CkanSearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(0);
  const [searching, setSearching] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState<ImportStats | null>(null);

  const doSearch = async (start = 0) => {
    if (!query.trim()) return;
    setSearching(true);
    setImportResult(null);
    try {
      const res = await searchCkan(query.trim(), 20, start);
      setResults(res.data.results);
      setTotal(res.data.total);
      setPage(start);
      setSelected(new Set());
    } catch {
      // ignore
    } finally {
      setSearching(false);
    }
  };

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const selectAll = () => {
    if (selected.size === results.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(results.map((r) => r.id)));
    }
  };

  const doImport = async () => {
    if (selected.size === 0) return;
    setImporting(true);
    setImportResult(null);
    try {
      const res = await importCkanDatasets(Array.from(selected));
      setImportResult(res.data);
      // Refresh search to update "already_imported" counts
      await doSearch(page);
    } catch {
      setImportResult({ imported: 0, skipped: 0, errors: 1, error_messages: ["שגיאה בייבוא"] });
    } finally {
      setImporting(false);
    }
  };

  const totalPages = Math.ceil(total / 20);
  const currentPage = Math.floor(page / 20) + 1;

  return (
    <div>
      {/* Search bar */}
      <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
        <div className="flex gap-2">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && doSearch(0)}
            placeholder="חיפוש מסמכים ב-odata.org.il..."
            className="flex-1 px-4 py-2.5 border border-gray-300 rounded-lg text-sm focus:outline-none focus:border-primary-500"
            dir="rtl"
          />
          <button
            onClick={() => doSearch(0)}
            disabled={searching}
            className="px-6 py-2.5 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium disabled:opacity-50"
          >
            {searching ? "מחפש..." : "חיפוש"}
          </button>
        </div>
        {total > 0 && (
          <div className="text-xs text-gray-500 mt-2">{total} תוצאות נמצאו</div>
        )}
      </div>

      {/* Results */}
      {results.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 mb-4">
          {/* Header */}
          <div className="flex items-center justify-between p-3 border-b border-gray-100">
            <div className="flex items-center gap-3">
              <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
                <input
                  type="checkbox"
                  checked={selected.size === results.length && results.length > 0}
                  onChange={selectAll}
                  className="rounded"
                />
                בחר הכל
              </label>
              {selected.size > 0 && (
                <span className="text-xs text-primary-700 font-medium">{selected.size} נבחרו</span>
              )}
            </div>
            <button
              onClick={doImport}
              disabled={selected.size === 0 || importing}
              className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors text-sm font-medium disabled:opacity-50"
            >
              {importing ? "מייבא..." : `ייבא ${selected.size > 0 ? `(${selected.size})` : ""}`}
            </button>
          </div>

          {/* Dataset rows */}
          {results.map((ds) => (
            <div
              key={ds.id}
              className={`flex items-start gap-3 p-3 border-b border-gray-50 hover:bg-gray-50 transition-colors ${
                ds.already_imported === ds.num_documents && ds.num_documents > 0 ? "opacity-60" : ""
              }`}
            >
              <input
                type="checkbox"
                checked={selected.has(ds.id)}
                onChange={() => toggleSelect(ds.id)}
                className="mt-1 rounded"
              />
              <div className="flex-1 min-w-0">
                <div className="font-medium text-gray-900 text-sm">{ds.title}</div>
                {ds.notes && (
                  <div className="text-xs text-gray-500 mt-0.5 line-clamp-2">{ds.notes}</div>
                )}
                <div className="flex flex-wrap gap-3 mt-1.5 text-xs text-gray-400">
                  <span>{ds.num_documents} מסמכים</span>
                  {ds.already_imported > 0 && (
                    <span className="text-yellow-600">{ds.already_imported} כבר יובאו</span>
                  )}
                  {ds.metadata_modified && (
                    <span>עודכן: {new Date(ds.metadata_modified).toLocaleDateString("he-IL")}</span>
                  )}
                  {ds.tags.length > 0 && (
                    <span>{ds.tags.slice(0, 3).join(", ")}</span>
                  )}
                </div>
              </div>
            </div>
          ))}

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 p-3">
              <button
                onClick={() => doSearch(page - 20)}
                disabled={page === 0 || searching}
                className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-30"
              >
                הקודם
              </button>
              <span className="text-sm text-gray-600">
                עמוד {currentPage} מתוך {totalPages}
              </span>
              <button
                onClick={() => doSearch(page + 20)}
                disabled={page + 20 >= total || searching}
                className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-30"
              >
                הבא
              </button>
            </div>
          )}
        </div>
      )}

      {/* Import result */}
      {importResult && (
        <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
          <h3 className="font-semibold text-gray-800 mb-2">תוצאות ייבוא</h3>
          <div className="grid grid-cols-3 gap-3">
            <div className="bg-green-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-green-700">{importResult.imported}</div>
              <div className="text-xs text-gray-500">יובאו</div>
            </div>
            <div className="bg-yellow-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-yellow-700">{importResult.skipped}</div>
              <div className="text-xs text-gray-500">דולגו (כפילויות)</div>
            </div>
            <div className="bg-red-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-red-700">{importResult.errors}</div>
              <div className="text-xs text-gray-500">שגיאות</div>
            </div>
          </div>
          {importResult.error_messages.length > 0 && (
            <div className="mt-2 text-xs text-red-600 font-mono">
              {importResult.error_messages.map((m, i) => <div key={i}>{m}</div>)}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Gov.il Tab: Bulk import ─────────────────────────────────────────────

function GovilTab() {
  const [launching, setLaunching] = useState(false);
  const [importStatus, setImportStatus] = useState<ImportStatus | null>(null);
  const [error, setError] = useState("");
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchStatus = useCallback(async () => {
    try {
      const res = await getImportStatus();
      setImportStatus(res.data);
      if (!res.data.running && pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    fetchStatus();
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [fetchStatus]);

  const handleTrigger = async () => {
    setLaunching(true);
    setError("");
    try {
      await triggerGovilImport();
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(fetchStatus, 2000);
      await fetchStatus();
    } catch (err) {
      const msg = err instanceof Error ? err.message : "";
      setError(msg.includes("409") ? "ייבוא כבר רץ ברקע." : "שגיאה בהפעלת הייבוא");
    } finally {
      setLaunching(false);
    }
  };

  const isRunning = importStatus?.running ?? false;
  const progress = importStatus && importStatus.total > 0
    ? Math.round(((importStatus.imported + importStatus.skipped + importStatus.errors) / importStatus.total) * 100)
    : 0;

  return (
    <div>
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-6">
        <p className="text-sm text-gray-600 mb-4">
          ייבוא הסדרי ניגוד עניינים של שרים ונושאי משרה מאתר Gov.il.
          הייבוא רץ ברקע ומייבא את כל הרשומות. מסמכים קיימים לא ייכפלו.
        </p>
        <button
          onClick={handleTrigger}
          disabled={launching || isRunning}
          className="px-6 py-3 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors font-medium disabled:opacity-50"
        >
          {launching ? "מפעיל..." : isRunning ? "ייבוא פעיל..." : "הפעל ייבוא Gov.il"}
        </button>
        {error && (
          <div className="mt-3 p-3 rounded-lg bg-red-50 border border-red-200 text-sm text-red-700">{error}</div>
        )}
      </div>

      {/* Progress */}
      {importStatus && (importStatus.running || importStatus.finished_at) && (
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-800 mb-3">
            {isRunning ? "ייבוא פעיל" : "תוצאות ייבוא אחרון"}
          </h2>

          {isRunning && importStatus.total > 0 && (
            <div className="mb-4">
              <div className="flex justify-between text-sm text-gray-600 mb-1">
                <span>התקדמות</span>
                <span>{progress}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-3">
                <div className="bg-primary-600 h-3 rounded-full transition-all duration-500" style={{ width: `${progress}%` }} />
              </div>
            </div>
          )}

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-4">
            <div className="bg-gray-50 rounded-lg p-3 text-center">
              <div className="text-2xl font-bold text-gray-900">{importStatus.total}</div>
              <div className="text-xs text-gray-500">נמצאו</div>
            </div>
            <div className="bg-green-50 rounded-lg p-3 text-center">
              <div className="text-2xl font-bold text-green-700">{importStatus.imported}</div>
              <div className="text-xs text-gray-500">יובאו</div>
            </div>
            <div className="bg-yellow-50 rounded-lg p-3 text-center">
              <div className="text-2xl font-bold text-yellow-700">{importStatus.skipped}</div>
              <div className="text-xs text-gray-500">דולגו (כפילויות)</div>
            </div>
            <div className="bg-red-50 rounded-lg p-3 text-center">
              <div className="text-2xl font-bold text-red-700">{importStatus.errors}</div>
              <div className="text-xs text-gray-500">שגיאות</div>
            </div>
          </div>

          <div className="text-sm text-gray-500 space-y-1">
            {importStatus.started_at && (
              <div>התחלה: <span className="font-medium text-gray-700">{new Date(importStatus.started_at).toLocaleString("he-IL")}</span></div>
            )}
            {importStatus.finished_at && (
              <div>סיום: <span className="font-medium text-gray-700">{new Date(importStatus.finished_at).toLocaleString("he-IL")}</span></div>
            )}
          </div>

          {importStatus.error_messages.length > 0 && (
            <div className="mt-4">
              <h3 className="text-sm font-semibold text-red-700 mb-2">שגיאות ({importStatus.error_messages.length})</h3>
              <div className="bg-red-50 rounded-lg p-3 max-h-40 overflow-y-auto">
                {importStatus.error_messages.map((msg, i) => (
                  <div key={i} className="text-xs text-red-600 mb-1 font-mono">{msg}</div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
