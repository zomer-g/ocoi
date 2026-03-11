"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import { triggerImport, getImportStatus, type ImportStatus } from "@/lib/admin-api";

const SOURCES = [
  { value: "ckan", label: "CKAN (odata.org.il)", description: "נתוני ניגוד עניינים מפורטל הנתונים הפתוחים" },
  { value: "govil", label: "Gov.il", description: "הסדרי ניגוד עניינים של שרים ונושאי משרה" },
  { value: "all", label: "הכל", description: "ייבוא משני המקורות" },
];

export default function ImportPage() {
  const [selectedSource, setSelectedSource] = useState("all");
  const [launching, setLaunching] = useState(false);
  const [importStatus, setImportStatus] = useState<ImportStatus | null>(null);
  const [error, setError] = useState("");
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchStatus = useCallback(async () => {
    try {
      const res = await getImportStatus();
      setImportStatus(res.data);
      // Stop polling when import finishes
      if (!res.data.running && pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    } catch {
      // ignore polling errors
    }
  }, []);

  // Fetch initial status on mount
  useEffect(() => {
    fetchStatus();
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [fetchStatus]);

  const handleTrigger = async () => {
    setLaunching(true);
    setError("");
    try {
      await triggerImport(selectedSource);
      // Start polling every 2 seconds
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(fetchStatus, 2000);
      // Immediately fetch status
      await fetchStatus();
    } catch (err) {
      const msg = err instanceof Error ? err.message : "שגיאה בהפעלת הייבוא";
      if (msg.includes("409")) {
        setError("ייבוא כבר רץ ברקע. המתן לסיומו.");
      } else {
        setError(msg);
      }
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
      <h1 className="text-2xl font-bold text-gray-900 mb-6">ייבוא מסמכים</h1>

      {/* Source selection */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-6">
        <h2 className="text-lg font-semibold text-gray-800 mb-3">בחירת מקור</h2>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-4">
          {SOURCES.map((src) => (
            <button
              key={src.value}
              onClick={() => setSelectedSource(src.value)}
              disabled={isRunning}
              className={`p-4 rounded-lg border-2 text-start transition-colors ${
                selectedSource === src.value
                  ? "border-primary-600 bg-primary-50"
                  : "border-gray-200 hover:border-primary-300"
              } disabled:opacity-50`}
            >
              <div className="font-medium text-gray-900">{src.label}</div>
              <div className="text-xs text-gray-500 mt-1">{src.description}</div>
            </button>
          ))}
        </div>

        <button
          onClick={handleTrigger}
          disabled={launching || isRunning}
          className="px-6 py-3 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors font-medium disabled:opacity-50"
        >
          {launching ? "מפעיל..." : isRunning ? "ייבוא פעיל..." : "הפעל ייבוא"}
        </button>

        {error && (
          <div className="mt-3 p-3 rounded-lg bg-red-50 border border-red-200 text-sm text-red-700">
            {error}
          </div>
        )}
      </div>

      {/* Progress */}
      {importStatus && (importStatus.running || importStatus.finished_at) && (
        <div className="bg-white rounded-lg border border-gray-200 p-6 mb-6">
          <h2 className="text-lg font-semibold text-gray-800 mb-3">
            {isRunning ? "ייבוא פעיל" : "תוצאות ייבוא אחרון"}
          </h2>

          {/* Progress bar */}
          {isRunning && importStatus.total > 0 && (
            <div className="mb-4">
              <div className="flex justify-between text-sm text-gray-600 mb-1">
                <span>התקדמות</span>
                <span>{progress}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-3">
                <div
                  className="bg-primary-600 h-3 rounded-full transition-all duration-500"
                  style={{ width: `${progress}%` }}
                />
              </div>
            </div>
          )}

          {/* Stats grid */}
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

          {/* Timestamps */}
          <div className="text-sm text-gray-500 space-y-1">
            {importStatus.source && <div>מקור: <span className="font-medium text-gray-700">{importStatus.source}</span></div>}
            {importStatus.started_at && (
              <div>התחלה: <span className="font-medium text-gray-700">{new Date(importStatus.started_at).toLocaleString("he-IL")}</span></div>
            )}
            {importStatus.finished_at && (
              <div>סיום: <span className="font-medium text-gray-700">{new Date(importStatus.finished_at).toLocaleString("he-IL")}</span></div>
            )}
          </div>

          {/* Errors */}
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

      {/* Info */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-sm text-blue-800">
        <strong>מידע:</strong> הייבוא רץ ברקע. ניתן לעזוב את הדף ולחזור — הסטטוס יישמר.
        מסמכים שכבר יובאו לא ייכפלו (זיהוי כפילויות לפי כתובת URL).
      </div>
    </div>
  );
}
