"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import {
  searchCkan,
  importCkanDatasets,
  triggerGovilImport,
  getImportStatus,
  getExtractionPrompt,
  updateExtractionPrompt,
  triggerExtraction,
  getExtractionStatus,
  type CkanSearchResult,
  type ImportStatus,
  type ImportStats,
  type ExtractionStatus,
} from "@/lib/admin-api";

type Tab = "ckan" | "govil" | "extraction";

export default function ImportPage() {
  const [tab, setTab] = useState<Tab>("ckan");

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">ייבוא מסמכים</h1>

      {/* Tab buttons */}
      <div className="flex gap-2 mb-6">
        {([
          ["ckan", "CKAN (odata.org.il)"],
          ["govil", "Gov.il"],
          ["extraction", "חילוץ ישויות"],
        ] as [Tab, string][]).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`px-5 py-2.5 rounded-lg font-medium text-sm transition-colors ${
              tab === key ? "bg-primary-700 text-white" : "bg-white border border-gray-200 text-gray-700 hover:bg-gray-50"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === "ckan" && <CkanTab />}
      {tab === "govil" && <GovilTab />}
      {tab === "extraction" && <ExtractionTab />}
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
  const [hideImported, setHideImported] = useState(true);

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

  const filteredResults = hideImported
    ? results.filter((r) => !(r.already_imported === r.num_documents && r.num_documents > 0))
    : results;
  const hiddenCount = results.length - filteredResults.length;

  const selectAll = () => {
    if (selected.size === filteredResults.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(filteredResults.map((r) => r.id)));
    }
  };

  const doImport = async () => {
    if (selected.size === 0) return;
    setImporting(true);
    setImportResult(null);
    try {
      const res = await importCkanDatasets(Array.from(selected));
      setImportResult(res.data);
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
          <div className="flex items-center justify-between mt-2">
            <div className="text-xs text-gray-500">{total} תוצאות נמצאו</div>
            <label className="flex items-center gap-2 text-xs text-gray-500 cursor-pointer">
              <input
                type="checkbox"
                checked={hideImported}
                onChange={(e) => setHideImported(e.target.checked)}
                className="rounded"
              />
              הסתר פריטים שיובאו
              {hiddenCount > 0 && <span className="text-yellow-600">({hiddenCount} מוסתרים)</span>}
            </label>
          </div>
        )}
      </div>

      {/* Results */}
      {filteredResults.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 mb-4">
          <div className="flex items-center justify-between p-3 border-b border-gray-100">
            <div className="flex items-center gap-3">
              <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
                <input
                  type="checkbox"
                  checked={selected.size === filteredResults.length && filteredResults.length > 0}
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

          {filteredResults.map((ds) => (
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
      if (!res.data.running) stopPolling();
    } catch { /* ignore */ }
  }, [stopPolling]);

  const startPolling = useCallback(() => {
    stopPolling();
    pollStatus();
    pollingRef.current = setInterval(pollStatus, 2000);
  }, [stopPolling, pollStatus]);

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
    ? Math.round(((status.imported + status.errors) / status.total) * 100)
    : 0;

  return (
    <div>
      {/* Trigger */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-4">
        <h2 className="text-lg font-semibold text-gray-800 mb-2">ייבוא מ-Gov.il</h2>
        <p className="text-sm text-gray-600 mb-4">
          ייבוא אוטומטי של הסדרי ניגוד עניינים של שרים וסגני שרים מאתר Gov.il.
          <br />
          <span className="text-xs text-gray-400">
            המערכת סורקת את כל הדפים, מזהה מסמכים חדשים, ומייבאת רק מה שעדיין לא קיים במערכת.
          </span>
        </p>
        <button
          onClick={handleTrigger}
          disabled={triggering || isRunning}
          className="px-6 py-3 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium disabled:opacity-50"
        >
          {triggering ? "מפעיל..." : isRunning ? "ייבוא פעיל..." : "התחל ייבוא"}
        </button>
        {error && <div className="mt-3 text-sm text-red-600">{error}</div>}
      </div>

      {/* Summary: what's on the website vs DB */}
      {status && (status.total_on_website > 0 || isRunning) && (
        <div className="bg-blue-50 rounded-lg border border-blue-100 p-4 mb-4">
          <div className="grid grid-cols-3 gap-3 text-center">
            <div>
              <div className="text-xl font-bold text-blue-700">{status.total_on_website}</div>
              <div className="text-xs text-gray-500">באתר Gov.il</div>
            </div>
            <div>
              <div className="text-xl font-bold text-gray-600">{status.already_in_db}</div>
              <div className="text-xs text-gray-500">כבר במערכת</div>
            </div>
            <div>
              <div className="text-xl font-bold text-primary-700">{status.new_to_import}</div>
              <div className="text-xs text-gray-500">חדשים לייבוא</div>
            </div>
          </div>
        </div>
      )}

      {/* Progress */}
      {isRunning && status && status.total > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold text-gray-800">מייבא מסמכים חדשים...</h3>
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
              <span className="text-xs text-gray-500">פעיל</span>
            </div>
          </div>
          <div className="mb-3">
            <div className="flex justify-between text-xs text-gray-500 mb-1">
              <span>{status.imported + status.errors} / {status.total}</span>
              <span>{progressPct}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2.5">
              <div className="bg-primary-700 h-2.5 rounded-full transition-all duration-500" style={{ width: `${progressPct}%` }} />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-green-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-green-700">{status.imported}</div>
              <div className="text-xs text-gray-500">יובאו</div>
            </div>
            <div className="bg-red-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-red-700">{status.errors}</div>
              <div className="text-xs text-gray-500">שגיאות</div>
            </div>
          </div>
        </div>
      )}

      {/* Finished */}
      {isFinished && status && (
        <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
          <h3 className="font-semibold text-gray-800 mb-3">תוצאות ייבוא — Gov.il</h3>
          <div className="grid grid-cols-3 gap-3 mb-3">
            <div className="bg-green-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-green-700">{status.imported}</div>
              <div className="text-xs text-gray-500">יובאו</div>
            </div>
            <div className="bg-yellow-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-yellow-700">{status.already_in_db}</div>
              <div className="text-xs text-gray-500">כבר במערכת</div>
            </div>
            <div className="bg-red-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-red-700">{status.errors}</div>
              <div className="text-xs text-gray-500">שגיאות</div>
            </div>
          </div>
          <div className="text-xs text-gray-400 space-y-0.5">
            {status.started_at && <div>התחיל: {new Date(status.started_at).toLocaleString("he-IL")}</div>}
            {status.finished_at && <div>הסתיים: {new Date(status.finished_at).toLocaleString("he-IL")}</div>}
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

// ── Extraction Tab: DeepSeek Entity Extraction ──────────────────────────

function ExtractionTab() {
  const [systemPrompt, setSystemPrompt] = useState("");
  const [userPrompt, setUserPrompt] = useState("");
  const [loadingPrompt, setLoadingPrompt] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [status, setStatus] = useState<ExtractionStatus | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [error, setError] = useState("");
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Load prompt on mount
  useEffect(() => {
    getExtractionPrompt()
      .then((res) => {
        setSystemPrompt(res.data.system_prompt);
        setUserPrompt(res.data.user_prompt);
      })
      .finally(() => setLoadingPrompt(false));
  }, []);

  const handleSavePrompt = async () => {
    setSaving(true);
    setSaved(false);
    try {
      await updateExtractionPrompt(systemPrompt, userPrompt);
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch {
      setError("שגיאה בשמירת הפרומפט");
    } finally {
      setSaving(false);
    }
  };

  // Polling
  const stopPolling = useCallback(() => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  }, []);

  const pollStatus = useCallback(async () => {
    try {
      const res = await getExtractionStatus();
      setStatus(res.data);
      if (!res.data.running) stopPolling();
    } catch { /* ignore */ }
  }, [stopPolling]);

  const startPolling = useCallback(() => {
    stopPolling();
    pollStatus();
    pollingRef.current = setInterval(pollStatus, 2000);
  }, [stopPolling, pollStatus]);

  useEffect(() => {
    pollStatus();
    return stopPolling;
  }, [pollStatus, stopPolling]);

  const handleTrigger = async () => {
    setTriggering(true);
    setError("");
    try {
      await triggerExtraction();
      startPolling();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה בהפעלת החילוץ");
    } finally {
      setTriggering(false);
    }
  };

  const isRunning = status?.running === true;
  const isFinished = status?.finished_at && !isRunning;
  const progressPct = status && status.total > 0
    ? Math.round((status.processed / status.total) * 100)
    : 0;

  return (
    <div>
      {/* Prompt editor */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-4">
        <h2 className="text-lg font-semibold text-gray-800 mb-2">פרומפט חילוץ ישויות</h2>
        <p className="text-xs text-gray-500 mb-4">
          ערוך את ההנחיות שנשלחות ל-DeepSeek לצורך חילוץ ישויות ויחסים מכל מסמך.
        </p>

        {loadingPrompt ? (
          <div className="text-gray-400 text-sm">טוען...</div>
        ) : (
          <>
            <label className="block text-sm font-medium text-gray-700 mb-1">System Prompt:</label>
            <textarea
              value={systemPrompt}
              onChange={(e) => setSystemPrompt(e.target.value)}
              className="w-full h-24 px-3 py-2 border border-gray-300 rounded-lg text-xs font-mono focus:outline-none focus:border-primary-500 resize-y mb-3"
              dir="rtl"
            />

            <label className="block text-sm font-medium text-gray-700 mb-1">
              User Prompt Template:
              <span className="text-xs text-gray-400 font-normal mr-2">(השתמש ב-{"{document_text}"} למיקום תוכן המסמך)</span>
            </label>
            <textarea
              value={userPrompt}
              onChange={(e) => setUserPrompt(e.target.value)}
              className="w-full h-48 px-3 py-2 border border-gray-300 rounded-lg text-xs font-mono focus:outline-none focus:border-primary-500 resize-y mb-3"
              dir="ltr"
            />

            <button
              onClick={handleSavePrompt}
              disabled={saving}
              className="px-4 py-2 bg-gray-800 text-white rounded-lg hover:bg-gray-900 transition-colors text-sm font-medium disabled:opacity-50"
            >
              {saving ? "שומר..." : saved ? "נשמר!" : "שמור פרומפט"}
            </button>
          </>
        )}
      </div>

      {/* Trigger extraction */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-4">
        <h2 className="text-lg font-semibold text-gray-800 mb-2">הפעלת חילוץ</h2>
        <p className="text-sm text-gray-600 mb-4">
          הפעלת חילוץ ישויות על כל המסמכים שממתינים לעיבוד.
          <br />
          <span className="text-xs text-gray-400">
            לכל מסמך: הורדת PDF, המרה לטקסט, שליחה ל-DeepSeek, שמירת ישויות ויחסים.
          </span>
        </p>
        <button
          onClick={handleTrigger}
          disabled={triggering || isRunning}
          className="px-6 py-3 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium disabled:opacity-50"
        >
          {triggering ? "מפעיל..." : isRunning ? "חילוץ פעיל..." : "התחל חילוץ"}
        </button>
        {error && <div className="mt-3 text-sm text-red-600">{error}</div>}
      </div>

      {/* Progress */}
      {isRunning && status && (
        <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold text-gray-800">מחלץ ישויות...</h3>
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
              <span className="text-xs text-gray-500">פעיל</span>
            </div>
          </div>
          {status.total > 0 && (
            <div className="mb-3">
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span>{status.processed} / {status.total} מסמכים</span>
                <span>{progressPct}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2.5">
                <div className="bg-primary-700 h-2.5 rounded-full transition-all duration-500" style={{ width: `${progressPct}%` }} />
              </div>
            </div>
          )}
          <div className="grid grid-cols-3 gap-3">
            <div className="bg-green-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-green-700">{status.entities_found}</div>
              <div className="text-xs text-gray-500">ישויות</div>
            </div>
            <div className="bg-blue-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-blue-700">{status.relationships_found}</div>
              <div className="text-xs text-gray-500">יחסים</div>
            </div>
            <div className="bg-red-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-red-700">{status.errors}</div>
              <div className="text-xs text-gray-500">שגיאות</div>
            </div>
          </div>
        </div>
      )}

      {/* Finished */}
      {isFinished && status && (
        <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
          <h3 className="font-semibold text-gray-800 mb-3">תוצאות חילוץ</h3>
          <div className="grid grid-cols-4 gap-3 mb-3">
            <div className="bg-gray-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-gray-700">{status.processed}</div>
              <div className="text-xs text-gray-500">מסמכים</div>
            </div>
            <div className="bg-green-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-green-700">{status.entities_found}</div>
              <div className="text-xs text-gray-500">ישויות</div>
            </div>
            <div className="bg-blue-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-blue-700">{status.relationships_found}</div>
              <div className="text-xs text-gray-500">יחסים</div>
            </div>
            <div className="bg-red-50 rounded-lg p-2 text-center">
              <div className="text-xl font-bold text-red-700">{status.errors}</div>
              <div className="text-xs text-gray-500">שגיאות</div>
            </div>
          </div>
          <div className="text-xs text-gray-400 space-y-0.5">
            {status.started_at && <div>התחיל: {new Date(status.started_at).toLocaleString("he-IL")}</div>}
            {status.finished_at && <div>הסתיים: {new Date(status.finished_at).toLocaleString("he-IL")}</div>}
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
