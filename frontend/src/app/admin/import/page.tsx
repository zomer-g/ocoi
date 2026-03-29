"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import {
  searchCkan,
  importCkanResources,
  bulkImportCkan,
  ignoreResources,
  unignoreResources,
  triggerGovilImport,
  submitGovilRecords,
  fetchGovilFromBrowser,
  getGovilCachedRecords,
  getImportStatus,
  resetImportState,
  getExtractionPrompt,
  updateExtractionPrompt,
  triggerExtraction,
  getExtractionStatus,
  resetExtraction,
  reconvertAllDocuments,
  type CkanSearchResult,
  type CkanResource,
  type CkanResourceImport,
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

// ── CKAN Tab: Search + Resource-level Select + Import ─────────────────

function formatResourceSize(size: number | null) {
  if (!size) return "";
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(0)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

const FORMAT_COLORS: Record<string, string> = {
  pdf: "bg-red-50 text-red-700 border-red-200",
  docx: "bg-blue-50 text-blue-700 border-blue-200",
  doc: "bg-blue-50 text-blue-700 border-blue-200",
  xlsx: "bg-green-50 text-green-700 border-green-200",
  csv: "bg-green-50 text-green-700 border-green-200",
  jpeg: "bg-purple-50 text-purple-700 border-purple-200",
  jpg: "bg-purple-50 text-purple-700 border-purple-200",
  png: "bg-purple-50 text-purple-700 border-purple-200",
};

// Unique key for a resource: dataset_id + url
function resourceKey(datasetId: string, url: string) {
  return `${datasetId}::${url}`;
}

function CkanTab() {
  const [query, setQuery] = useState("ניגוד עניינים");
  const [results, setResults] = useState<CkanSearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(0);
  const [searching, setSearching] = useState(false);
  // Selected resources (key = datasetId::url)
  const [selected, setSelected] = useState<Set<string>>(new Set());
  // Expanded datasets (show resources)
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState<ImportStats | null>(null);
  const [hideImported, setHideImported] = useState(true);
  const [hideOcr, setHideOcr] = useState(true);
  const [hideIgnored, setHideIgnored] = useState(true);
  // Bulk import
  const [bulkRunning, setBulkRunning] = useState(false);
  const [bulkStatus, setBulkStatus] = useState<ImportStatus | null>(null);
  const bulkPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  // Shift-click range selection
  const lastClickedRef = useRef<string | null>(null);

  const doSearch = async (start = 0) => {
    if (!query.trim()) return;
    setSearching(true);
    setImportResult(null);
    try {
      const res = await searchCkan(query.trim(), 50, start);
      setResults(res.data.results);
      setTotal(res.data.total);
      setPage(start);
      setSelected(new Set());
      setExpanded(new Set());
    } catch {
      // ignore
    } finally {
      setSearching(false);
    }
  };

  const toggleExpand = (id: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const isOcrFile = (title: string) => title.includes("לאחר עיבוד OCR");

  // Get visible resources for a dataset (respecting filters)
  const getVisibleResources = (ds: CkanSearchResult) => {
    return ds.resources.filter((r) => {
      if (hideImported && r.already_imported) return false;
      if (hideIgnored && r.ignored) return false;
      if (hideOcr && isOcrFile(r.title)) return false;
      return true;
    });
  };

  // Filter out datasets where ALL visible resources are hidden by active filters
  const filteredResults = results.filter((ds) => {
    const visible = getVisibleResources(ds);
    return visible.length > 0;
  });
  const hiddenCount = results.length - filteredResults.length;

  // Build a flat list of all visible resource keys (for shift-click)
  const allVisibleKeys = filteredResults.flatMap((ds) =>
    getVisibleResources(ds).filter((r) => !r.already_imported && !r.ignored).map((r) => resourceKey(ds.id, r.url))
  );

  const toggleResource = (datasetId: string, url: string, shiftKey = false) => {
    const key = resourceKey(datasetId, url);

    if (shiftKey && lastClickedRef.current && lastClickedRef.current !== key) {
      // Shift-click: select range
      const fromIdx = allVisibleKeys.indexOf(lastClickedRef.current);
      const toIdx = allVisibleKeys.indexOf(key);
      if (fromIdx >= 0 && toIdx >= 0) {
        const start = Math.min(fromIdx, toIdx);
        const end = Math.max(fromIdx, toIdx);
        const rangeKeys = allVisibleKeys.slice(start, end + 1);
        setSelected((prev) => {
          const next = new Set(prev);
          rangeKeys.forEach((k) => next.add(k));
          return next;
        });
        lastClickedRef.current = key;
        return;
      }
    }

    lastClickedRef.current = key;
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const toggleAllInDataset = (ds: CkanSearchResult) => {
    const availableResources = getVisibleResources(ds).filter((r) => !r.already_imported);
    const allKeys = availableResources.map((r) => resourceKey(ds.id, r.url));
    const allSelected = allKeys.length > 0 && allKeys.every((k) => selected.has(k));
    setSelected((prev) => {
      const next = new Set(prev);
      if (allSelected) {
        allKeys.forEach((k) => next.delete(k));
      } else {
        allKeys.forEach((k) => next.add(k));
      }
      return next;
    });
  };

  // Count selected resources
  const selectedCount = selected.size;

  // Build resource import list from selection
  const buildImportList = (): CkanResourceImport[] => {
    const list: CkanResourceImport[] = [];
    for (const ds of results) {
      for (const res of ds.resources) {
        if (selected.has(resourceKey(ds.id, res.url))) {
          list.push({
            dataset_id: ds.id,
            url: res.url,
            title: res.title,
            format: res.format,
            size: res.size,
            resource_id: res.resource_id,
          });
        }
      }
    }
    return list;
  };

  const doImport = async () => {
    const resources = buildImportList();
    if (resources.length === 0) return;
    setImporting(true);
    setImportResult(null);
    try {
      const res = await importCkanResources(resources);
      setImportResult(res.data);
      await doSearch(page);
    } catch {
      setImportResult({ imported: 0, skipped: 0, errors: 1, error_messages: ["שגיאה בייבוא"] });
    } finally {
      setImporting(false);
    }
  };

  const doIgnore = async () => {
    const list: { url: string; title: string }[] = [];
    for (const ds of results) {
      for (const res of ds.resources) {
        if (selected.has(resourceKey(ds.id, res.url))) {
          list.push({ url: res.url, title: res.title });
        }
      }
    }
    if (list.length === 0) return;
    try {
      await ignoreResources(list);
      setSelected(new Set());
      await doSearch(page);
    } catch {
      // ignore
    }
  };

  const doBulkImport = async () => {
    if (!query.trim()) return;
    try {
      await bulkImportCkan(query.trim());
      setBulkRunning(true);
      // Start polling
      bulkPollRef.current = setInterval(async () => {
        try {
          const res = await getImportStatus();
          setBulkStatus(res.data);
          if (!res.data.running) {
            if (bulkPollRef.current) clearInterval(bulkPollRef.current);
            bulkPollRef.current = null;
            setBulkRunning(false);
          }
        } catch {}
      }, 2000);
    } catch (e: unknown) {
      alert((e as Error).message);
    }
  };

  // Cleanup bulk poll on unmount
  useEffect(() => {
    return () => {
      if (bulkPollRef.current) clearInterval(bulkPollRef.current);
    };
  }, []);

  const totalPages = Math.ceil(total / 50);
  const currentPage = Math.floor(page / 50) + 1;

  // Auto-advance to next page when all results on current page are filtered out
  useEffect(() => {
    if (
      results.length > 0 &&
      filteredResults.length === 0 &&
      !searching &&
      page + 50 < total
    ) {
      doSearch(page + 50);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [results, filteredResults.length, searching, page, total]);

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
          <button
            onClick={doBulkImport}
            disabled={!query.trim() || bulkRunning || importing}
            className="px-5 py-2.5 bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 transition-colors text-sm font-medium disabled:opacity-50 whitespace-nowrap"
            title="ייבא את כל המסמכים התואמים לחיפוש (ללא הגבלת כמות)"
          >
            {bulkRunning ? "מייבא..." : "ייבא הכל"}
          </button>
        </div>
        {total > 0 && (
          <div className="flex items-center justify-between mt-2">
            <div className="text-xs text-gray-500">{total} תוצאות נמצאו</div>
            <div className="flex items-center gap-4">
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
              <label className="flex items-center gap-2 text-xs text-gray-500 cursor-pointer">
                <input
                  type="checkbox"
                  checked={hideIgnored}
                  onChange={(e) => setHideIgnored(e.target.checked)}
                  className="rounded"
                />
                הסתר פריטים מותעלמים
              </label>
              <label className="flex items-center gap-2 text-xs text-gray-500 cursor-pointer">
                <input
                  type="checkbox"
                  checked={hideOcr}
                  onChange={(e) => setHideOcr(e.target.checked)}
                  className="rounded"
                />
                הסתר עותקי OCR
              </label>
            </div>
          </div>
        )}
      </div>

      {/* Bulk import progress */}
      {(bulkRunning || (bulkStatus && bulkStatus.finished_at)) && bulkStatus && (
        <div className={`rounded-lg border p-4 mb-4 ${bulkRunning ? "bg-blue-50 border-blue-200" : "bg-green-50 border-green-200"}`}>
          <div className="flex items-center justify-between text-sm mb-2">
            <span className={bulkRunning ? "text-blue-700 font-medium" : "text-green-700 font-medium"}>
              {bulkRunning ? "מייבא את כל המסמכים..." : "ייבוא הושלם"}
            </span>
            <span className={bulkRunning ? "text-blue-600" : "text-green-600"}>
              {bulkStatus.imported} יובאו · {bulkStatus.skipped} דולגו · {bulkStatus.errors} שגיאות
            </span>
          </div>
          {bulkRunning && bulkStatus.total > 0 && (
            <div className="w-full bg-blue-200 rounded-full h-2 mb-2">
              <div
                className="bg-blue-600 h-2 rounded-full transition-all"
                style={{ width: `${((bulkStatus.imported + bulkStatus.skipped + bulkStatus.errors) / bulkStatus.total) * 100}%` }}
              />
            </div>
          )}
          {bulkStatus.total_on_website > 0 && (
            <div className="text-xs text-gray-500">
              {bulkStatus.total_on_website} מערכי נתונים · {bulkStatus.new_to_import} חדשים · {bulkStatus.already_in_db} כבר קיימים
            </div>
          )}
          {!bulkRunning && bulkStatus.error_messages.length > 0 && (
            <div className="mt-2 text-xs text-red-600 font-mono max-h-32 overflow-auto">
              {bulkStatus.error_messages.map((m, i) => <div key={i}>{m}</div>)}
            </div>
          )}
          {!bulkRunning && (
            <button
              onClick={() => setBulkStatus(null)}
              className="mt-2 text-xs text-gray-500 hover:text-gray-700"
            >
              סגור
            </button>
          )}
        </div>
      )}

      {/* Results */}
      {filteredResults.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 mb-4">
          {/* Import bar */}
          <div className="flex items-center justify-between p-3 border-b border-gray-100">
            <div className="flex items-center gap-3">
              {selectedCount > 0 && (
                <span className="text-xs text-primary-700 font-medium">{selectedCount} משאבים נבחרו</span>
              )}
            </div>
            <div className="flex items-center gap-2">
              {selectedCount > 0 && (
                <button
                  onClick={doIgnore}
                  className="px-3 py-2 border border-gray-300 text-gray-600 rounded-lg hover:bg-gray-50 transition-colors text-sm"
                >
                  התעלם ({selectedCount})
                </button>
              )}
              <button
                onClick={doImport}
                disabled={selectedCount === 0 || importing}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors text-sm font-medium disabled:opacity-50"
              >
                {importing ? "מייבא..." : `ייבא ${selectedCount > 0 ? `(${selectedCount})` : ""}`}
              </button>
            </div>
          </div>

          {/* Select all on page */}
          <div className="px-4 py-2 border-b border-gray-100">
            <label className="flex items-center gap-2 text-xs text-gray-500 cursor-pointer">
              <input
                type="checkbox"
                checked={allVisibleKeys.length > 0 && allVisibleKeys.every((k) => selected.has(k))}
                onChange={() => {
                  const allSelected = allVisibleKeys.length > 0 && allVisibleKeys.every((k) => selected.has(k));
                  setSelected((prev) => {
                    const next = new Set(prev);
                    if (allSelected) {
                      allVisibleKeys.forEach((k) => next.delete(k));
                    } else {
                      allVisibleKeys.forEach((k) => next.add(k));
                    }
                    return next;
                  });
                }}
                className="rounded"
              />
              בחר הכל בעמוד ({allVisibleKeys.length})
            </label>
          </div>

          {/* Flat resource list */}
          {filteredResults.map((ds) => {
            const visibleResources = getVisibleResources(ds);
            if (visibleResources.length === 0) return null;

            return (
              <div key={ds.id}>
                {/* Dataset name as group header */}
                <div className="px-4 py-1.5 bg-gray-50 border-b border-gray-100">
                  <span className="text-xs font-medium text-gray-500">{ds.title}</span>
                </div>
                {visibleResources.map((res) => {
                  const key = resourceKey(ds.id, res.url);
                  const fmtColor = FORMAT_COLORS[res.format] || "bg-gray-50 text-gray-600 border-gray-200";
                  return (
                    <label
                      key={key}
                      className={`flex items-center gap-3 px-4 py-2 border-b border-gray-50 hover:bg-gray-50 transition-colors cursor-pointer ${
                        res.already_imported || res.ignored ? "opacity-50" : ""
                      } ${selected.has(key) ? "bg-primary-50/50" : ""}`}
                      onClick={(e) => {
                        if (!res.already_imported && !res.ignored) {
                          e.preventDefault();
                          toggleResource(ds.id, res.url, e.shiftKey);
                        }
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={selected.has(key)}
                        onChange={() => {}}
                        disabled={res.already_imported || res.ignored}
                        className="rounded shrink-0"
                      />
                      <span className={`text-[10px] px-1.5 py-0.5 rounded border uppercase font-medium shrink-0 ${fmtColor}`}>
                        {res.format}
                      </span>
                      <span className="text-sm text-gray-700 truncate flex-1 min-w-0" title={res.title}>
                        {res.title}
                      </span>
                      {(res.size ?? 0) > 0 && (
                        <span className="text-xs text-gray-400 shrink-0">{formatResourceSize(res.size)}</span>
                      )}
                      {res.already_imported && (
                        <span className="text-xs text-yellow-600 shrink-0">יובא</span>
                      )}
                      {res.ignored && !res.already_imported && (
                        <span className="text-xs text-gray-400 shrink-0">מותעלם</span>
                      )}
                    </label>
                  );
                })}
              </div>
            );
          })}

          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 p-3">
              <button
                onClick={() => doSearch(page - 50)}
                disabled={page === 0 || searching}
                className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-30"
              >
                הקודם
              </button>
              <span className="text-sm text-gray-600">
                עמוד {currentPage} מתוך {totalPages}
              </span>
              <button
                onClick={() => doSearch(page + 50)}
                disabled={page + 50 >= total || searching}
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

// ── Gov.il Tab: URL-based scraper + manual fallback ──────────────────

const GOVIL_PRESETS = [
  {
    label: "הסדרי ניגוד עניינים — שרים וסגני שרים",
    url: "https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict",
  },
];

const GOVIL_CONSOLE_SCRIPT = `(async () => {
  const all = [];
  let skip = 0;
  console.log('Starting Gov.il data fetch...');
  while (true) {
    const r = await fetch('/he/api/DynamicCollector', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        DynamicTemplateID: 'c6e0f53e-02c0-4db1-ae89-76590f0f502e',
        QueryFilters: {},
        From: skip,
        Quantity: 20
      })
    });
    const d = await r.json();
    if (!d.Results || !d.Results.length) break;
    all.push(...d.Results);
    skip += d.Results.length;
    console.log('Fetched ' + all.length + ' / ' + (d.TotalResults || '?') + ' records...');
  }
  copy(JSON.stringify(all));
  console.log('Done! ' + all.length + ' records copied to clipboard.');
  console.log('Go back to the import page and paste (Ctrl+V).');
})();`;

function tryCountRecords(json: string): string {
  try {
    const arr = JSON.parse(json);
    return Array.isArray(arr) ? String(arr.length) : "?";
  } catch {
    return "?";
  }
}

function isValidGovilUrl(url: string): boolean {
  try {
    const u = new URL(url);
    return u.hostname === "www.gov.il" || u.hostname === "gov.il";
  } catch {
    return false;
  }
}

function GovilTab() {
  const [status, setStatus] = useState<ImportStatus | null>(null);
  const [phase, setPhase] = useState<"idle" | "fetching" | "manual" | "processing">("idle");
  const [fetchProgress, setFetchProgress] = useState({ fetched: 0, total: 0 });
  const [error, setError] = useState("");
  const [pastedData, setPastedData] = useState("");
  const [scriptCopied, setScriptCopied] = useState(false);
  const [govilUrl, setGovilUrl] = useState(GOVIL_PRESETS[0].url);
  const [customUrl, setCustomUrl] = useState("");
  const [useCustomUrl, setUseCustomUrl] = useState(false);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const activeUrl = useCustomUrl ? customUrl : govilUrl;

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
      if (!res.data.running) { stopPolling(); setPhase("idle"); }
    } catch { /* ignore */ }
  }, [stopPolling]);

  const startPolling = useCallback(() => {
    stopPolling();
    setPhase("processing");
    pollStatus();
    pollingRef.current = setInterval(pollStatus, 2000);
  }, [stopPolling, pollStatus]);

  useEffect(() => {
    pollStatus();
    return stopPolling;
  }, [pollStatus, stopPolling]);

  // Server-side scraper (new: uses cloudscraper + Playwright fallback)
  const handleServerScrape = async () => {
    if (useCustomUrl && !isValidGovilUrl(customUrl)) {
      setError("כתובת URL לא תקינה. יש להזין כתובת מאתר gov.il.");
      return;
    }
    setPhase("fetching");
    setError("");
    try {
      await triggerGovilImport(0, activeUrl);
      startPolling();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה בהפעלת הסקריפט");
      setPhase("idle");
    }
  };

  // Browser-side fetch via proxy (original flow)
  const handleBrowserFetch = async () => {
    setPhase("fetching");
    setError("");
    setFetchProgress({ fetched: 0, total: 0 });
    try {
      const records = await fetchGovilFromBrowser((fetched, total) => {
        setFetchProgress({ fetched, total });
      });

      if (records.length === 0) {
        setPhase("manual");
        return;
      }

      await submitGovilRecords(records);
      startPolling();
    } catch {
      setPhase("manual");
    }
  };

  // Use pre-fetched cached records
  const handleUseCached = async () => {
    setPhase("fetching");
    setError("");
    try {
      const res = await getGovilCachedRecords();
      const records = res.data.records;
      if (!records || records.length === 0) {
        setError("אין רשומות שמורות בשרת.");
        setPhase("idle");
        return;
      }
      await submitGovilRecords(records);
      startPolling();
    } catch {
      setError("לא נמצאו רשומות שמורות. נסה ייבוא ידני.");
      setPhase("idle");
    }
  };

  const handleCopyScript = () => {
    navigator.clipboard.writeText(GOVIL_CONSOLE_SCRIPT);
    setScriptCopied(true);
    setTimeout(() => setScriptCopied(false), 2000);
  };

  const handleManualImport = async () => {
    if (!pastedData.trim()) return;
    setError("");
    try {
      const records = JSON.parse(pastedData);
      if (!Array.isArray(records) || records.length === 0) {
        setError("הנתונים לא בפורמט תקין. ודא שהעתקת את כל הפלט מהסקריפט.");
        return;
      }
      await submitGovilRecords(records);
      setPastedData("");
      startPolling();
    } catch {
      setError("שגיאה בפענוח הנתונים. ודא שהעתקת את כל הטקסט מהקונסול.");
    }
  };

  const isRunning = phase === "processing" || status?.running === true;
  const isFinished = status?.finished_at && !isRunning && phase === "idle";
  const processed = status ? status.imported + status.skipped + status.errors : 0;
  const progressPct = status && status.total > 0
    ? Math.round((processed / status.total) * 100)
    : 0;

  return (
    <div>
      {/* URL selection */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-4">
        <h2 className="text-lg font-semibold text-gray-800 mb-2">ייבוא מ-Gov.il</h2>
        <p className="text-sm text-gray-600 mb-4">
          בחר עמוד אוסף מאתר Gov.il לייבוא, או הזן כתובת URL מותאמת אישית.
        </p>

        {/* Preset URLs */}
        <div className="space-y-2 mb-4">
          {GOVIL_PRESETS.map((preset) => (
            <label
              key={preset.url}
              className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                !useCustomUrl && govilUrl === preset.url
                  ? "border-primary-300 bg-primary-50"
                  : "border-gray-200 hover:bg-gray-50"
              }`}
            >
              <input
                type="radio"
                name="govil-url"
                checked={!useCustomUrl && govilUrl === preset.url}
                onChange={() => { setGovilUrl(preset.url); setUseCustomUrl(false); }}
                className="accent-primary-700"
              />
              <div className="flex-1 min-w-0">
                <div className="text-sm font-medium text-gray-800">{preset.label}</div>
                <div className="text-xs text-gray-400 truncate" dir="ltr">{preset.url}</div>
              </div>
            </label>
          ))}

          {/* Custom URL */}
          <label
            className={`flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
              useCustomUrl
                ? "border-primary-300 bg-primary-50"
                : "border-gray-200 hover:bg-gray-50"
            }`}
          >
            <input
              type="radio"
              name="govil-url"
              checked={useCustomUrl}
              onChange={() => setUseCustomUrl(true)}
              className="accent-primary-700 mt-1"
            />
            <div className="flex-1 min-w-0">
              <div className="text-sm font-medium text-gray-800 mb-1">כתובת URL מותאמת אישית</div>
              <input
                type="url"
                value={customUrl}
                onChange={(e) => { setCustomUrl(e.target.value); setUseCustomUrl(true); }}
                placeholder="https://www.gov.il/he/departments/dynamiccollectors/..."
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:border-primary-500"
                dir="ltr"
              />
            </div>
          </label>
        </div>

        {/* Action buttons */}
        <div className="flex gap-3 flex-wrap">
          <button
            onClick={handleServerScrape}
            disabled={phase !== "idle" || isRunning || (useCustomUrl && !customUrl.trim())}
            className="px-6 py-3 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium disabled:opacity-50"
          >
            {phase === "fetching" || isRunning ? "מעבד..." : "ייבוא אוטומטי (שרת)"}
          </button>
          <button
            onClick={handleBrowserFetch}
            disabled={phase !== "idle" || isRunning}
            className="px-6 py-3 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors text-sm font-medium disabled:opacity-50"
          >
            ייבוא דרך הדפדפן (פרוקסי)
          </button>
          <button
            onClick={handleUseCached}
            disabled={phase !== "idle" || isRunning}
            className="px-6 py-3 border border-green-300 text-green-700 rounded-lg hover:bg-green-50 transition-colors text-sm font-medium disabled:opacity-50"
          >
            ייבוא מנתונים שמורים
          </button>
          <button
            onClick={() => { setPhase("manual"); setError(""); }}
            disabled={phase !== "idle" || isRunning}
            className="px-6 py-3 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors text-sm font-medium disabled:opacity-50"
          >
            ייבוא ידני (מעקף Cloudflare)
          </button>
        </div>
        {isRunning && (
          <button
            onClick={async () => { await resetImportState(); setPhase("idle"); setStatus(null); }}
            className="px-4 py-2 border border-red-300 text-red-600 rounded-lg hover:bg-red-50 transition-colors text-sm"
          >
            אפס תהליך תקוע
          </button>
        )}
        {error && phase !== "manual" && <div className="mt-3 text-sm text-red-600">{error}</div>}
      </div>

      {/* Browser fetch progress */}
      {phase === "fetching" && !status?.running && (
        <div className="bg-blue-50 rounded-lg border border-blue-100 p-4 mb-4">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 bg-blue-500 rounded-full animate-pulse" />
              <span className="text-sm font-medium text-blue-800">מנסה להתחבר לשרת Gov.il...</span>
            </div>
            <button
              onClick={() => setPhase("manual")}
              className="text-xs text-blue-600 hover:text-blue-800 underline"
            >
              דלג לייבוא ידני
            </button>
          </div>
          {fetchProgress.total > 0 && (
            <div className="text-lg font-bold text-blue-700">
              {fetchProgress.fetched} / {fetchProgress.total} רשומות
            </div>
          )}
        </div>
      )}

      {/* Manual import instructions */}
      {phase === "manual" && (
        <div className="bg-amber-50 rounded-lg border border-amber-200 p-6 mb-4">
          <h3 className="font-semibold text-amber-800 mb-2">ייבוא ידני — מעקף Cloudflare</h3>
          <p className="text-sm text-amber-700 mb-4">
            Gov.il חוסם גישה אוטומטית מהשרת. ניתן לשלוף את הנתונים ישירות מהדפדפן שלך:
          </p>
          <ol className="text-sm text-gray-700 space-y-2 mb-4 list-decimal list-inside" dir="rtl">
            <li>
              פתח את{" "}
              <a
                href="https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict"
                target="_blank"
                rel="noopener noreferrer"
                className="text-primary-700 underline font-medium"
              >
                עמוד ניגוד עניינים ב-Gov.il
              </a>
              {" "}בלשונית חדשה
            </li>
            <li>לחץ <kbd className="px-1.5 py-0.5 bg-gray-200 rounded text-xs font-mono">F12</kbd> לפתיחת כלי המפתחים &larr; לשונית <strong>Console</strong></li>
            <li>לחץ &quot;העתק סקריפט&quot; למטה, הדבק בקונסול ולחץ Enter</li>
            <li>המתן לסיום (תראה הודעת Done!) — הנתונים יועתקו ללוח</li>
            <li>חזור לכאן והדבק <kbd className="px-1.5 py-0.5 bg-gray-200 rounded text-xs font-mono">Ctrl+V</kbd> בתיבה למטה</li>
          </ol>

          {/* Script copy button */}
          <div className="mb-4">
            <button
              onClick={handleCopyScript}
              className="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 transition-colors text-sm font-medium"
            >
              {scriptCopied ? "הועתק!" : "העתק סקריפט"}
            </button>
          </div>

          {/* Paste area */}
          <textarea
            value={pastedData}
            onChange={(e) => setPastedData(e.target.value)}
            placeholder={'הדבק כאן את הנתונים שהועתקו מהקונסול (Ctrl+V)...'}
            className="w-full h-28 px-3 py-2 border border-amber-300 rounded-lg text-xs font-mono focus:outline-none focus:border-primary-500 resize-y mb-3 bg-white"
            dir="ltr"
          />

          <div className="flex gap-2 items-center">
            <button
              onClick={handleManualImport}
              disabled={!pastedData.trim()}
              className="px-6 py-2.5 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium disabled:opacity-50"
            >
              {pastedData.trim()
                ? `ייבא (${tryCountRecords(pastedData)} רשומות)`
                : "ייבא"}
            </button>
            <button
              onClick={() => { setPhase("idle"); setError(""); setPastedData(""); }}
              className="px-4 py-2.5 text-gray-600 hover:text-gray-800 text-sm"
            >
              חזרה
            </button>
          </div>
          {error && <div className="mt-3 text-sm text-red-600">{error}</div>}
        </div>
      )}

      {/* Summary: what's on the website vs DB */}
      {status && (status.total_on_website > 0 || isRunning) && phase !== "fetching" && phase !== "manual" && (
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
            <h3 className="font-semibold text-gray-800">מוריד ומעבד מסמכים...</h3>
            <div className="flex items-center gap-2">
              <button
                onClick={async () => { await resetImportState(); setPhase("idle"); setStatus(null); }}
                className="text-xs text-red-500 hover:text-red-700 underline"
              >
                אפס ייבוא
              </button>
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
              <span className="text-xs text-gray-500">פעיל</span>
            </div>
          </div>
          <div className="mb-3">
            <div className="flex justify-between text-xs text-gray-500 mb-1">
              <span>{status.imported + status.skipped + status.errors} / {status.total}</span>
              <span>{progressPct}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2.5">
              <div className="bg-primary-700 h-2.5 rounded-full transition-all duration-500" style={{ width: `${progressPct}%` }} />
            </div>
          </div>
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
  const [resetting, setResetting] = useState(false);
  const [resetResult, setResetResult] = useState<Record<string, number> | null>(null);
  const [confirmReset, setConfirmReset] = useState(false);
  const [reconverting, setReconverting] = useState(false);
  const [reconvertResult, setReconvertResult] = useState<{ updated: number; skipped: number; errors: string[] } | null>(null);
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

      {/* Reconvert all PDFs */}
      <div className="bg-white rounded-lg border border-blue-200 p-6 mb-4">
        <h2 className="text-lg font-semibold text-blue-800 mb-2">המרה מחדש של כל המסמכים</h2>
        <p className="text-sm text-gray-600 mb-4">
          המרה מחדש של כל קבצי ה-PDF לטקסט באמצעות המרה מתוקנת (תיקון עברית הפוכה RTL).
        </p>
        <button
          onClick={async () => {
            setReconverting(true);
            setReconvertResult(null);
            setError("");
            try {
              const res = await reconvertAllDocuments();
              setReconvertResult(res.data);
            } catch (e) {
              setError(e instanceof Error ? e.message : "שגיאה בהמרה מחדש");
            } finally {
              setReconverting(false);
            }
          }}
          disabled={reconverting}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-sm font-medium disabled:opacity-50"
        >
          {reconverting ? "ממיר..." : "המר מחדש את כל המסמכים"}
        </button>
        {reconvertResult && (
          <div className="mt-3 p-3 bg-green-50 rounded-lg text-sm text-green-800">
            הומרו: {reconvertResult.updated} מסמכים, דילוג: {reconvertResult.skipped}.
            {reconvertResult.errors.length > 0 && (
              <div className="mt-1 text-red-700">שגיאות: {reconvertResult.errors.join(", ")}</div>
            )}
          </div>
        )}
      </div>

      {/* Reset extraction data */}
      <div className="bg-white rounded-lg border border-red-200 p-6 mb-4">
        <h2 className="text-lg font-semibold text-red-800 mb-2">איפוס נתוני חילוץ</h2>
        <p className="text-sm text-gray-600 mb-4">
          מחיקת כל הישויות, הקשרים, והרצות החילוץ. המסמכים יישארו אך יחזרו לסטטוס &quot;ממתין&quot;.
        </p>
        {!confirmReset ? (
          <button
            onClick={() => setConfirmReset(true)}
            disabled={resetting}
            className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors text-sm font-medium disabled:opacity-50"
          >
            מחק הכל והתחל מחדש
          </button>
        ) : (
          <div className="flex items-center gap-3">
            <span className="text-sm text-red-700 font-medium">בטוח? פעולה זו בלתי הפיכה!</span>
            <button
              onClick={async () => {
                setResetting(true);
                setResetResult(null);
                setError("");
                try {
                  const res = await resetExtraction();
                  setResetResult(res.deleted);
                } catch (e) {
                  setError(e instanceof Error ? e.message : "שגיאה באיפוס");
                } finally {
                  setResetting(false);
                  setConfirmReset(false);
                }
              }}
              disabled={resetting}
              className="px-4 py-2 bg-red-700 text-white rounded-lg hover:bg-red-800 transition-colors text-sm font-medium disabled:opacity-50"
            >
              {resetting ? "מוחק..." : "כן, מחק הכל"}
            </button>
            <button
              onClick={() => setConfirmReset(false)}
              className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors text-sm font-medium"
            >
              ביטול
            </button>
          </div>
        )}
        {resetResult && (
          <div className="mt-3 p-3 bg-green-50 rounded-lg text-sm text-green-800">
            נמחקו: {resetResult.persons || 0} אנשים, {resetResult.companies || 0} חברות, {resetResult.associations || 0} עמותות, {resetResult.domains || 0} תחומים, {resetResult.relationships || 0} קשרים, {resetResult.extraction_runs || 0} הרצות חילוץ.
            <br />כל המסמכים חזרו לסטטוס &quot;ממתין לחילוץ&quot;.
          </div>
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
