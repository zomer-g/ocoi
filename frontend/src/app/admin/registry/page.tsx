"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  getRegistrySources,
  getRegistrySyncStatus,
  getRegistryMatchStatus,
  triggerRegistrySync,
  triggerRegistryMatchAll,
  getRegistryRecords,
  type RegistrySource,
  type RegistrySyncState,
  type RegistryMatchState,
} from "@/lib/admin-api";

const STATUS_BADGES: Record<string, { label: string; color: string }> = {
  never: { label: "לא סונכרן", color: "bg-gray-100 text-gray-600" },
  syncing: { label: "מסנכרן...", color: "bg-blue-100 text-blue-700" },
  completed: { label: "מסונכרן", color: "bg-green-100 text-green-700" },
  failed: { label: "נכשל", color: "bg-red-100 text-red-700" },
};

function formatNumber(n: number) {
  return n.toLocaleString("he-IL");
}

function timeAgo(iso: string | null): string {
  if (!iso) return "—";
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "הרגע";
  if (mins < 60) return `לפני ${mins} דקות`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `לפני ${hours} שעות`;
  const days = Math.floor(hours / 24);
  return `לפני ${days} ימים`;
}

export default function RegistryPage() {
  const [sources, setSources] = useState<RegistrySource[]>([]);
  const [syncState, setSyncState] = useState<RegistrySyncState | null>(null);
  const [matchState, setMatchState] = useState<RegistryMatchState | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Records browser
  const [browseSource, setBrowseSource] = useState<string | null>(null);
  const [browseSearch, setBrowseSearch] = useState("");
  const [browseRecords, setBrowseRecords] = useState<
    { id: string; name: string; registration_number: string | null; source_type: string; status: string | null }[]
  >([]);
  const [browseMeta, setBrowseMeta] = useState<{ total: number; page: number; limit: number } | null>(null);

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadSources = useCallback(async () => {
    try {
      const res = await getRegistrySources();
      setSources(res.data);
    } catch (e: unknown) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSources();
  }, [loadSources]);

  // Poll sync status when syncing
  useEffect(() => {
    if (syncState?.running) {
      pollRef.current = setInterval(async () => {
        try {
          const res = await getRegistrySyncStatus();
          setSyncState(res.data);
          if (!res.data.running) {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = null;
            loadSources();
          }
        } catch {}
      }, 2000);
      return () => {
        if (pollRef.current) clearInterval(pollRef.current);
      };
    }
  }, [syncState?.running, loadSources]);

  // Poll match status when matching
  useEffect(() => {
    if (matchState?.running) {
      const id = setInterval(async () => {
        try {
          const res = await getRegistryMatchStatus();
          setMatchState(res.data);
          if (!res.data.running) {
            clearInterval(id);
          }
        } catch {}
      }, 2000);
      return () => clearInterval(id);
    }
  }, [matchState?.running]);

  const handleSync = async (sourceKey: string) => {
    try {
      setError(null);
      await triggerRegistrySync(sourceKey);
      const res = await getRegistrySyncStatus();
      setSyncState(res.data);
    } catch (e: unknown) {
      setError((e as Error).message);
    }
  };

  const handleMatchAll = async () => {
    try {
      setError(null);
      await triggerRegistryMatchAll();
      const res = await getRegistryMatchStatus();
      setMatchState(res.data);
    } catch (e: unknown) {
      setError((e as Error).message);
    }
  };

  const handleBrowse = async (sourceKey: string | null, search = "", page = 1) => {
    try {
      const res = await getRegistryRecords(sourceKey || undefined, search || undefined, page);
      setBrowseRecords(res.data);
      setBrowseMeta(res.meta);
    } catch {}
  };

  useEffect(() => {
    if (browseSource !== null) {
      handleBrowse(browseSource, browseSearch);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [browseSource]);

  if (loading) {
    return <div className="text-gray-400 text-center py-12">טוען...</div>;
  }

  return (
    <div className="space-y-6" dir="rtl">
      <h1 className="text-2xl font-bold text-gray-900">מרשמים חיצוניים</h1>
      <p className="text-sm text-gray-500">
        סנכרון מרשמים ממאגרי מידע ממשלתיים (DATAGOV) והתאמה אוטומטית של ישויות שחולצו.
      </p>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 rounded-lg px-4 py-3 text-sm">
          {error}
        </div>
      )}

      {/* Sync progress bar */}
      {syncState?.running && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <div className="flex items-center justify-between text-sm text-blue-700 mb-2">
            <span>
              מסנכרן {sources.find((s) => s.key === syncState.source)?.label || syncState.source}...
            </span>
            <span>
              {formatNumber(syncState.fetched)} / {formatNumber(syncState.total_remote)}
            </span>
          </div>
          <div className="w-full bg-blue-200 rounded-full h-2">
            <div
              className="bg-blue-600 h-2 rounded-full transition-all"
              style={{
                width: `${syncState.total_remote > 0 ? (syncState.fetched / syncState.total_remote) * 100 : 0}%`,
              }}
            />
          </div>
        </div>
      )}

      {/* Source cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {sources.map((src) => {
          const badge = STATUS_BADGES[src.sync_status] || STATUS_BADGES.never;
          const isSyncing = syncState?.running && syncState.source === src.key;
          return (
            <div key={src.key} className="bg-white rounded-xl border border-gray-200 p-5 space-y-3">
              <div className="flex items-center justify-between">
                <h3 className="font-semibold text-gray-900">{src.label}</h3>
                <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${badge.color}`}>
                  {badge.label}
                </span>
              </div>

              <div className="text-sm text-gray-500 space-y-1">
                <div className="flex justify-between">
                  <span>רשומות:</span>
                  <span className="font-medium text-gray-700">
                    {formatNumber(src.record_count)}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>סנכרון אחרון:</span>
                  <span className="text-gray-700">{timeAgo(src.last_synced_at)}</span>
                </div>
                <div className="flex justify-between">
                  <span>סוג ישות:</span>
                  <span className="text-gray-700">
                    {src.entity_type === "company" ? "חברה" : "עמותה"}
                  </span>
                </div>
              </div>

              {src.error_message && (
                <div className="text-xs text-red-600 bg-red-50 rounded px-2 py-1 truncate">
                  {src.error_message}
                </div>
              )}

              <div className="flex gap-2">
                <button
                  onClick={() => handleSync(src.key)}
                  disabled={!!syncState?.running}
                  className="flex-1 text-sm bg-primary-600 text-white rounded-lg px-3 py-2 hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  {isSyncing ? "מסנכרן..." : "סנכרן"}
                </button>
                <button
                  onClick={() => {
                    setBrowseSource(src.key);
                    setBrowseSearch("");
                  }}
                  disabled={src.record_count === 0}
                  className="text-sm border border-gray-300 text-gray-700 rounded-lg px-3 py-2 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  עיין
                </button>
              </div>
            </div>
          );
        })}
      </div>

      {/* Match all section */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 space-y-3">
        <h2 className="font-semibold text-gray-900">התאמת ישויות למרשמים</h2>
        <p className="text-sm text-gray-500">
          התאם את כל הישויות (חברות ועמותות) שעדיין ללא מספר רישום מול המרשמים החיצוניים.
        </p>

        {matchState?.running && (
          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm text-blue-700">
              <span>מתאים ישויות...</span>
              <span>
                {formatNumber(matchState.processed)} / {formatNumber(matchState.total)}
                {matchState.matched > 0 && ` (${formatNumber(matchState.matched)} הותאמו)`}
              </span>
            </div>
            <div className="w-full bg-blue-200 rounded-full h-2">
              <div
                className="bg-blue-600 h-2 rounded-full transition-all"
                style={{
                  width: `${matchState.total > 0 ? (matchState.processed / matchState.total) * 100 : 0}%`,
                }}
              />
            </div>
          </div>
        )}

        {matchState && !matchState.running && matchState.finished_at && (
          <div className="text-sm text-green-700 bg-green-50 rounded-lg px-3 py-2">
            התאמה הושלמה: {formatNumber(matchState.matched)} ישויות הותאמו מתוך {formatNumber(matchState.total)}
            {matchState.errors > 0 && ` (${matchState.errors} שגיאות)`}
          </div>
        )}

        <button
          onClick={handleMatchAll}
          disabled={matchState?.running}
          className="text-sm bg-emerald-600 text-white rounded-lg px-4 py-2 hover:bg-emerald-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {matchState?.running ? "מתאים..." : "התאם את כל הישויות"}
        </button>
      </div>

      {/* Records browser */}
      {browseSource && (
        <div className="bg-white rounded-xl border border-gray-200 p-5 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="font-semibold text-gray-900">
              רשומות: {sources.find((s) => s.key === browseSource)?.label}
            </h2>
            <button
              onClick={() => {
                setBrowseSource(null);
                setBrowseRecords([]);
                setBrowseMeta(null);
              }}
              className="text-sm text-gray-500 hover:text-gray-700"
            >
              סגור
            </button>
          </div>

          <div className="flex gap-2">
            <input
              type="text"
              value={browseSearch}
              onChange={(e) => setBrowseSearch(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") handleBrowse(browseSource, browseSearch);
              }}
              placeholder="חפש לפי שם..."
              className="flex-1 border border-gray-300 rounded-lg px-3 py-2 text-sm"
            />
            <button
              onClick={() => handleBrowse(browseSource, browseSearch)}
              className="text-sm bg-gray-100 text-gray-700 rounded-lg px-4 py-2 hover:bg-gray-200 transition-colors"
            >
              חפש
            </button>
          </div>

          {browseMeta && (
            <div className="text-xs text-gray-500">
              {formatNumber(browseMeta.total)} רשומות | עמוד {browseMeta.page}
            </div>
          )}

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-gray-600">
                <tr>
                  <th className="text-right px-3 py-2 font-medium">שם</th>
                  <th className="text-right px-3 py-2 font-medium">מספר רישום</th>
                  <th className="text-right px-3 py-2 font-medium">סטטוס</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {browseRecords.map((rec) => (
                  <tr key={rec.id} className="hover:bg-gray-50">
                    <td className="px-3 py-2 text-gray-900">{rec.name}</td>
                    <td className="px-3 py-2 text-gray-600 font-mono text-xs">
                      {rec.registration_number || "—"}
                    </td>
                    <td className="px-3 py-2 text-gray-500">{rec.status || "—"}</td>
                  </tr>
                ))}
                {browseRecords.length === 0 && (
                  <tr>
                    <td colSpan={3} className="px-3 py-8 text-center text-gray-400">
                      אין רשומות
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {browseMeta && browseMeta.total > browseMeta.limit && (
            <div className="flex gap-2 justify-center">
              <button
                onClick={() => handleBrowse(browseSource, browseSearch, browseMeta!.page - 1)}
                disabled={browseMeta.page <= 1}
                className="text-sm px-3 py-1 border border-gray-300 rounded disabled:opacity-50"
              >
                הקודם
              </button>
              <span className="text-sm text-gray-500 py-1">
                עמוד {browseMeta.page} מתוך {Math.ceil(browseMeta.total / browseMeta.limit)}
              </span>
              <button
                onClick={() => handleBrowse(browseSource, browseSearch, browseMeta!.page + 1)}
                disabled={browseMeta.page >= Math.ceil(browseMeta.total / browseMeta.limit)}
                className="text-sm px-3 py-1 border border-gray-300 rounded disabled:opacity-50"
              >
                הבא
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
