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

// ── Gov.il Tab: Automated Import ────────────────────────────────────

function GovilTab() {
  const [status, setStatus] = useState<ImportStatus | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [error, setError] = useState("");
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  }, []);

  const pollStatus = useCallback(async () => {
    try {
      const res = await getImportStatus();
      setStatus(res.data);
      if (!res.data.running) {
        stopPolling();
      }
    } catch {
      // ignore polling errors
    }
  }, [stopPolling]);

  const startPolling = useCallback(() => {
    stopPolling();
    pollStatus();
    pollingRef.current = setInterval(pollStatus, 2000);
  }, [stopPolling, pollStatus]);

  // Check status on mount (in case import is already running)
  useEffect(() => {
    pollStatus();
    return stopPolling;
  }, [pollStatus, stopPolling]);

  const handleTrigger = async () => {
    setTriggering(true);
    setError("");
    try {
      await triggerGovilImport(0);
      startPolling();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה בהפעלת הייבוא");
    } finally {
      setTriggering(false);
    }
  };

  const isRunning = status?.running === true;
  const isFinished = status?.finished_at && !isRunning;
  const progressPct = status && status.total > 0
    ? Math.round(((status.imported + status.skipped + status.errors) / status.total) * 100)
    : 0;

  return (
    <div>
      {/* Trigger section */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-4">
        <h2 className="text-lg font-semibold text-gray-800 mb-2">ייבוא מ-Gov.il</h2>
        <p className="text-sm text-gray-600 mb-4">
          ייבוא אוטומטי של הסדרי ניגוד עניינים של שרים וסגני שרים מאתר Gov.il.
          <br />
          <span className="text-xs text-gray-400">
            המערכת סורקת את כל הדפים ומייבאת מסמכי PDF חדשים. מסמכים שכבר יובאו ידולגו.
          </span>
        </p>

        <button
          onClick={handleTrigger}
          disabled={triggering || isRunning}
          className="px-6 py-3 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium disabled:opacity-50"
        >
          {triggering ? "מפעיל..." : isRunning ? "ייבוא פעיל..." : "התחל ייבוא"}
        </button>

        {error && (
          <div className="mt-3 text-sm text-red-600">{error}</div>
        )}
      </div>

      {/* Progress section */}
      {isRunning && status && (
        <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold text-gray-800">ייבוא פעיל — Gov.il</h3>
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
              <span className="text-xs text-gray-500">פעיל</span>
            </div>
          </div>

          {/* Progress bar */}
          {status.total > 0 && (
            <div className="mb-3">
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span>{status.imported + status.skipped + status.errors} / {status.total}</span>
                <span>{progressPct}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2.5">
                <div
                  className="bg-primary-700 h-2.5 rounded-full transition-all duration-500"
                  style={{ width: `${progressPct}%` }}
                />
              </div>
            </div>
          )}

          {/* Live stats */}
          <div className="grid grid-cols-3 gap-3">
            <div className="bg-green-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-green-700">{status.imported}</div>
              <div className="text-xs text-gray-500">יובאו</div>
            </div>
            <div className="bg-yellow-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-yellow-700">{status.skipped}</div>
              <div className="text-xs text-gray-500">דולגו</div>
            </div>
            <div className="bg-red-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-red-700">{status.errors}</div>
              <div className="text-xs text-gray-500">שגיאות</div>
            </div>
          </div>

          {status.started_at && (
            <div className="mt-2 text-xs text-gray-400">
              התחיל: {new Date(status.started_at).toLocaleString("he-IL")}
            </div>
          )}
        </div>
      )}

      {/* Finished section */}
      {isFinished && status && (
        <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
          <h3 className="font-semibold text-gray-800 mb-3">תוצאות ייבוא — Gov.il</h3>

          <div className="grid grid-cols-3 gap-3 mb-3">
            <div className="bg-green-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-green-700">{status.imported}</div>
              <div className="text-xs text-gray-500">יובאו</div>
            </div>
            <div className="bg-yellow-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-yellow-700">{status.skipped}</div>
              <div className="text-xs text-gray-500">דולגו (כפילויות)</div>
            </div>
            <div className="bg-red-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-red-700">{status.errors}</div>
              <div className="text-xs text-gray-500">שגיאות</div>
            </div>
          </div>

          <div className="text-xs text-gray-400 space-y-0.5">
            {status.started_at && (
              <div>התחיל: {new Date(status.started_at).toLocaleString("he-IL")}</div>
            )}
            {status.finished_at && (
              <div>הסתיים: {new Date(status.finished_at).toLocaleString("he-IL")}</div>
            )}
          </div>

          {status.error_messages.length > 0 && (
            <div className="mt-3 p-2 bg-red-50 rounded-lg">
              <div className="text-xs font-medium text-red-700 mb-1">שגיאות:</div>
              <div className="text-xs text-red-600 font-mono space-y-0.5 max-h-32 overflow-y-auto">
                {status.error_messages.map((m, i) => <div key={i}>{m}</div>)}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
