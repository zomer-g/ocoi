"use client";

import { useState } from "react";
import {
  searchCkan,
  importCkanDatasets,
  importGovilRecords,
  type CkanSearchResult,
  type GovilRecord,
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

// ── Gov.il Tab: Browser extraction + Import ───────────────────────────

const GOVIL_EXTRACT_SCRIPT = `(async()=>{const a=[];const tp=document.querySelector('[ng-repeat]');let s=angular.element(tp).scope();while(s&&!s.dynamicCtrl)s=s.$parent;const ctrl=s.dynamicCtrl;const totalPages=ctrl.ViewModel.totalPages;function extract(){const links=document.querySelectorAll('a[href$=".pdf"]');const urls=Array.from(links).map(l=>l.href);return ctrl.ViewModel.dataResults.map((r,i)=>{const d=r.Data;const f=d.file&&d.file[0]?d.file[0]:{};return{name:d.function||'',position_type_id:d.list?d.list[0]:'',ministry_id:d.government_ministry?d.government_ministry[0]:'',date:d.date||'',pdf_url:urls[i]||'',pdf_display:f.DisplayName||'',pdf_size:parseInt(f.FileSize||'0')}})}a.push(...extract());for(let p=2;p<=totalPages;p++){ctrl.Events.goToPage(p);s.$apply();await new Promise(r=>setTimeout(r,2000));a.push(...extract())}const j=JSON.stringify(a);await navigator.clipboard.writeText(j);alert('הועתקו '+a.length+' רשומות ללוח!')})();`;

function GovilTab() {
  const [pastedData, setPastedData] = useState("");
  const [records, setRecords] = useState<GovilRecord[]>([]);
  const [parseError, setParseError] = useState("");
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState<ImportStats | null>(null);
  const [copied, setCopied] = useState(false);

  const handleCopyScript = async () => {
    try {
      await navigator.clipboard.writeText(GOVIL_EXTRACT_SCRIPT);
      setCopied(true);
      setTimeout(() => setCopied(false), 3000);
    } catch {
      // Fallback: select the text
      const ta = document.createElement("textarea");
      ta.value = GOVIL_EXTRACT_SCRIPT;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
      setCopied(true);
      setTimeout(() => setCopied(false), 3000);
    }
  };

  const handlePaste = (text: string) => {
    setPastedData(text);
    setParseError("");
    setImportResult(null);
    if (!text.trim()) {
      setRecords([]);
      return;
    }
    try {
      const parsed = JSON.parse(text);
      if (!Array.isArray(parsed)) throw new Error("Expected array");
      if (parsed.length === 0) throw new Error("Empty array");
      // Validate first record has expected fields
      const first = parsed[0];
      if (!first.pdf_url && !first.name) throw new Error("Missing fields");
      setRecords(parsed);
    } catch (e) {
      setParseError(`שגיאה בפענוח JSON: ${e instanceof Error ? e.message : "unknown"}`);
      setRecords([]);
    }
  };

  const doImport = async () => {
    if (records.length === 0) return;
    setImporting(true);
    setImportResult(null);
    try {
      const res = await importGovilRecords(records);
      setImportResult(res.data);
    } catch {
      setImportResult({ imported: 0, skipped: 0, errors: 1, error_messages: ["שגיאה בייבוא"] });
    } finally {
      setImporting(false);
    }
  };

  return (
    <div>
      {/* Instructions */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-4">
        <h2 className="text-lg font-semibold text-gray-800 mb-3">ייבוא מ-Gov.il</h2>
        <p className="text-sm text-gray-600 mb-4">
          אתר Gov.il חוסם גישה אוטומטית. כדי לייבא, צריך לחלץ את הנתונים מהדפדפן:
        </p>
        <ol className="text-sm text-gray-700 space-y-2 mb-4 list-decimal list-inside" dir="rtl">
          <li>
            פתח את{" "}
            <a
              href="https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict?skip=0"
              target="_blank"
              rel="noopener noreferrer"
              className="text-primary-700 underline hover:text-primary-900"
            >
              דף הסדרי ניגוד עניינים ב-Gov.il
            </a>
            {" "}בטאב חדש
          </li>
          <li>פתח את כלי המפתח (F12) ועבור ללשונית Console</li>
          <li>
            <button
              onClick={handleCopyScript}
              className="inline-flex items-center gap-1 px-3 py-1 bg-gray-100 border border-gray-300 rounded text-xs font-mono hover:bg-gray-200 transition-colors"
            >
              {copied ? "✓ הועתק!" : "העתק סקריפט"}
            </button>
            {" "}— הדבק ב-Console ולחץ Enter. הסקריפט עובר על כל הדפים ומעתיק את הנתונים ללוח.
          </li>
          <li>חזור לכאן והדבק (Ctrl+V) את הנתונים בתיבה למטה</li>
        </ol>
      </div>

      {/* Paste area */}
      <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-2">הדבק נתונים (JSON):</label>
        <textarea
          value={pastedData}
          onChange={(e) => handlePaste(e.target.value)}
          placeholder='הדבק כאן את ה-JSON שחולץ מ-Gov.il...'
          className="w-full h-32 px-3 py-2 border border-gray-300 rounded-lg text-xs font-mono focus:outline-none focus:border-primary-500 resize-y"
          dir="ltr"
        />
        {parseError && (
          <div className="mt-2 text-sm text-red-600">{parseError}</div>
        )}
        {records.length > 0 && (
          <div className="mt-2 flex items-center justify-between">
            <span className="text-sm text-green-700 font-medium">
              ✓ {records.length} רשומות זוהו
            </span>
            <button
              onClick={doImport}
              disabled={importing}
              className="px-6 py-2.5 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors text-sm font-medium disabled:opacity-50"
            >
              {importing ? "מייבא..." : `ייבא ${records.length} רשומות`}
            </button>
          </div>
        )}
      </div>

      {/* Preview first few records */}
      {records.length > 0 && !importResult && (
        <div className="bg-white rounded-lg border border-gray-200 mb-4">
          <div className="p-3 border-b border-gray-100">
            <span className="text-sm font-medium text-gray-700">תצוגה מקדימה ({Math.min(5, records.length)} מתוך {records.length})</span>
          </div>
          {records.slice(0, 5).map((r, i) => (
            <div key={i} className="p-3 border-b border-gray-50 text-sm">
              <div className="font-medium text-gray-900">{r.pdf_display || r.name}</div>
              <div className="text-xs text-gray-400 mt-0.5">
                {r.date && <span>{new Date(r.date).toLocaleDateString("he-IL")} · </span>}
                <span className="font-mono">{r.pdf_url.split("/").pop()}</span>
              </div>
            </div>
          ))}
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
