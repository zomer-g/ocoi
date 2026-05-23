"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";

interface EntitySummary {
  id: string;
  type: string;
  name: string;
  aliases?: string[];
  title?: string | null;
  position?: string | null;
  ministry?: string | null;
  kind?: string;
}

interface MatchProposal {
  id: string;
  proposal_kind: "duplicate" | "registry_match" | string;
  status: "pending" | "approved" | "rejected" | "dismissed" | string;
  score: number;
  reasons: string[];
  left: EntitySummary;
  right: EntitySummary;
  reviewed_by_name?: string | null;
  reviewed_at?: string | null;
  created_at?: string | null;
}

interface ScanStatus {
  running: boolean;
  phase: string | null;
  scanned_entities: number;
  total_entities: number;
  candidate_pairs: number;
  proposals_written: number;
  duplicates_skipped: number;
  errors: number;
  started_at: string | null;
  finished_at: string | null;
  current_kind: string | null;
}

const STATUS_OPTIONS: { value: string; label: string; cls: string }[] = [
  { value: "pending", label: "ממתינות", cls: "bg-amber-100 text-amber-800 border-amber-300" },
  { value: "approved", label: "מאושרות", cls: "bg-emerald-100 text-emerald-800 border-emerald-300" },
  { value: "rejected", label: "נדחו", cls: "bg-red-100 text-red-800 border-red-300" },
  { value: "dismissed", label: "מוסתרות", cls: "bg-gray-100 text-gray-700 border-gray-300" },
  { value: "all", label: "הכל", cls: "bg-white text-gray-700 border-gray-300" },
];

const KIND_FILTERS: { value: string; label: string }[] = [
  { value: "", label: "כל הסוגים" },
  { value: "person", label: "אנשים" },
  { value: "company", label: "חברות" },
  { value: "association", label: "עמותות" },
];

const KIND_LABEL: Record<string, string> = {
  person: "אדם",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

const KIND_BADGE: Record<string, string> = {
  person: "bg-blue-100 text-blue-700",
  company: "bg-amber-100 text-amber-700",
  association: "bg-green-100 text-green-700",
  domain: "bg-purple-100 text-purple-700",
};

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("he-IL", {
      day: "numeric", month: "numeric", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch { return iso; }
}

function EntityCard({ e, side }: { e: EntitySummary; side: "left" | "right" }) {
  const sideBorder = side === "left" ? "border-l-2 border-l-primary-300" : "border-r-2 border-r-emerald-300";
  return (
    <div className={`flex-1 min-w-0 p-3 bg-gray-50 rounded-lg ${sideBorder}`}>
      <div className="flex items-center gap-2 mb-1">
        <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${KIND_BADGE[e.type] || "bg-gray-100 text-gray-700"}`}>
          {KIND_LABEL[e.type] || e.type}
        </span>
        <span className="font-semibold text-gray-900 truncate">{e.name || "(ללא שם)"}</span>
      </div>
      {(e.title || e.position || e.ministry) && (
        <div className="text-xs text-gray-500 truncate">
          {[e.title, e.position, e.ministry].filter(Boolean).join(" · ")}
        </div>
      )}
      {Array.isArray(e.aliases) && e.aliases.length > 0 && (
        <div className="mt-1 text-xs text-gray-400">
          כינויים: {e.aliases.slice(0, 3).join(", ")}
          {e.aliases.length > 3 && ` (+${e.aliases.length - 3})`}
        </div>
      )}
      <div className="mt-2">
        <Link
          href={
            e.type === "person"
              ? `/admin/entities/detail?type=persons&id=${e.id}`
              : e.type === "company"
                ? `/admin/entities/detail?type=companies&id=${e.id}`
                : e.type === "association"
                  ? `/admin/entities/detail?type=associations&id=${e.id}`
                  : `/admin/entities/detail?type=${e.type}&id=${e.id}`
          }
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-primary-600 hover:underline"
        >
          פתח ↗
        </Link>
      </div>
    </div>
  );
}

export default function AdminMatchesPage() {
  const [items, setItems] = useState<MatchProposal[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<string>("pending");
  const [kindFilter, setKindFilter] = useState<string>("");
  const [savingId, setSavingId] = useState<string | null>(null);
  const [scan, setScan] = useState<ScanStatus | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        proposal_kind: "duplicate",
        status: statusFilter,
        limit: "100",
      });
      if (kindFilter) params.set("entity_type", kindFilter);
      const res = await fetch(`/api/v1/admin/matches?${params}`, { credentials: "include" });
      if (!res.ok) throw new Error(`שגיאה בטעינה (${res.status})`);
      const data = await res.json();
      const rows: MatchProposal[] = Array.isArray(data.data) ? data.data : [];
      setItems(rows);
      setTotal(data.meta?.total || rows.length);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
      setItems([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }, [statusFilter, kindFilter]);

  const refreshScan = useCallback(async () => {
    try {
      const res = await fetch(`/api/v1/admin/matches/scan-status`, { credentials: "include" });
      if (!res.ok) return;
      const data = await res.json();
      setScan(data.data || null);
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    refresh();
    refreshScan();
  }, [refresh, refreshScan]);

  // Poll scan status while it's running so the user sees progress.
  useEffect(() => {
    if (!scan?.running) return;
    const t = setInterval(refreshScan, 2000);
    return () => clearInterval(t);
  }, [scan?.running, refreshScan]);

  const triggerScan = async () => {
    setError(null);
    try {
      const res = await fetch(`/api/v1/admin/matches/scan-duplicates`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `שגיאה (${res.status})`);
      }
      const data = await res.json();
      setScan(data.state || null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה בהפעלת הסריקה");
    }
  };

  const resetScan = async () => {
    try {
      await fetch(`/api/v1/admin/matches/scan-reset`, {
        method: "POST",
        credentials: "include",
      });
      await refreshScan();
    } catch {
      // ignore
    }
  };

  const act = async (id: string, action: "approve" | "reject" | "dismiss") => {
    setSavingId(id);
    setError(null);
    try {
      const res = await fetch(`/api/v1/admin/matches/${id}/${action}`, {
        method: "POST",
        credentials: "include",
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `שגיאה (${res.status})`);
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setSavingId(null);
    }
  };

  const scanRunning = !!scan?.running;
  const scanPhaseLabel: Record<string, string> = {
    loading: "טוען ישויות",
    comparing: "משווה זוגות",
    writing: "כותב הצעות",
  };

  return (
    <div>
      <div className="flex items-start justify-between gap-4 mb-4">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">תור התאמות כפילויות</h1>
          <p className="text-sm text-gray-500 mt-1 max-w-3xl">
            סריקה אוטומטית מזהה זוגות של ישויות שנראות כמו אותה ישות (לדוגמה "משה כחלון" ו"כחלון משה").
            כל הצעה ממתינה לאישור ידני — באישור הקשרים מועברים אוטומטית לישות שתישאר, והשנייה נמחקת.
          </p>
        </div>
        <div className="shrink-0 flex items-center gap-2">
          <button
            onClick={triggerScan}
            disabled={scanRunning}
            className="px-4 py-2 rounded-lg bg-primary-700 text-white text-sm font-medium hover:bg-primary-800 disabled:opacity-50 transition-colors"
          >
            {scanRunning ? "סורק..." : "סרוק כפילויות"}
          </button>
          {scan && !scan.running && (scan.errors > 0 || scan.proposals_written > 0 || scan.scanned_entities > 0) && (
            <button
              onClick={resetScan}
              className="px-3 py-2 rounded-lg border border-gray-300 text-sm text-gray-700 hover:bg-gray-50"
              title="איפוס מצב הסריקה (לא משנה הצעות שכבר נכתבו)"
            >
              איפוס מצב
            </button>
          )}
        </div>
      </div>

      {/* Scan progress block */}
      {scan && (scan.running || scan.started_at) && (
        <div className={`mb-4 p-3 rounded-lg border text-sm ${scan.running ? "bg-amber-50 border-amber-200 text-amber-900" : "bg-gray-50 border-gray-200 text-gray-700"}`}>
          <div className="flex items-center gap-3 mb-1">
            <span className="font-medium">
              {scan.running ? "סריקה פעילה" : "סריקה הושלמה"}
            </span>
            {scan.phase && <span className="text-xs">· {scanPhaseLabel[scan.phase] || scan.phase}</span>}
            {scan.current_kind && (
              <span className="text-xs">· {KIND_LABEL[scan.current_kind] || scan.current_kind}</span>
            )}
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 text-xs">
            <div>
              <div className="text-gray-500">ישויות נסקרו</div>
              <div className="font-semibold">{scan.scanned_entities.toLocaleString()} / {scan.total_entities.toLocaleString()}</div>
            </div>
            <div>
              <div className="text-gray-500">זוגות נבדקו</div>
              <div className="font-semibold">{scan.candidate_pairs.toLocaleString()}</div>
            </div>
            <div>
              <div className="text-gray-500">הצעות חדשות</div>
              <div className="font-semibold text-emerald-700">{scan.proposals_written.toLocaleString()}</div>
            </div>
            <div>
              <div className="text-gray-500">דולגו (קיימות)</div>
              <div className="font-semibold">{scan.duplicates_skipped.toLocaleString()}</div>
            </div>
            <div>
              <div className="text-gray-500">שגיאות</div>
              <div className={`font-semibold ${scan.errors > 0 ? "text-red-700" : ""}`}>{scan.errors}</div>
            </div>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2 mb-3">
        <span className="text-xs text-gray-500">סטטוס:</span>
        {STATUS_OPTIONS.map((s) => (
          <button
            key={s.value}
            onClick={() => setStatusFilter(s.value)}
            className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-colors ${
              statusFilter === s.value ? s.cls : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
            }`}
          >
            {s.label}
          </button>
        ))}
      </div>
      <div className="flex flex-wrap items-center gap-2 mb-4">
        <span className="text-xs text-gray-500">סוג ישות:</span>
        {KIND_FILTERS.map((k) => (
          <button
            key={k.value || "all-kind"}
            onClick={() => setKindFilter(k.value)}
            className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-colors ${
              kindFilter === k.value
                ? "bg-primary-700 text-white border-primary-700"
                : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
            }`}
          >
            {k.label}
          </button>
        ))}
      </div>

      {error && (
        <div className="mb-4 p-3 rounded-lg bg-red-50 text-red-700 text-sm">{error}</div>
      )}

      <p className="text-xs text-gray-400 mb-3">{total.toLocaleString()} הצעות</p>

      {loading ? (
        <div className="text-center py-12 text-gray-400">טוען...</div>
      ) : items.length === 0 ? (
        <div className="text-center py-12 text-gray-400">
          אין הצעות בתצוגה הזו. {statusFilter === "pending" ? "הפעל סריקה כדי לחפש כפילויות חדשות." : ""}
        </div>
      ) : (
        <div className="space-y-3">
          {items.map((p) => {
            const status = STATUS_OPTIONS.find((s) => s.value === p.status);
            const scorePct = Math.round((p.score || 0) * 100);
            return (
              <div key={p.id} className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
                <div className="flex items-center justify-between mb-2 flex-wrap gap-2">
                  <div className="flex items-center gap-2 flex-wrap">
                    {status && (
                      <span className={`text-xs px-2 py-0.5 rounded-full border ${status.cls}`}>
                        {status.label}
                      </span>
                    )}
                    <span
                      className={`text-xs px-2 py-0.5 rounded-full font-semibold ${
                        scorePct >= 95 ? "bg-emerald-100 text-emerald-800"
                        : scorePct >= 90 ? "bg-amber-100 text-amber-800"
                        : "bg-gray-100 text-gray-700"
                      }`}
                      title="רמת ביטחון משוקללת על בסיס token-sort + Jaccard + בונוס היפוך מילים"
                    >
                      {scorePct}% ביטחון
                    </span>
                    {p.reasons.map((r, i) => (
                      <span key={i} className="text-[10px] px-1.5 py-0.5 rounded-full bg-gray-100 text-gray-600 border border-gray-200">
                        {r}
                      </span>
                    ))}
                  </div>
                  <span className="text-xs text-gray-400">
                    {formatDate(p.created_at)}
                    {p.reviewed_by_name && ` · נבדק על ידי ${p.reviewed_by_name}`}
                  </span>
                </div>

                <div className="flex flex-col md:flex-row gap-3 mb-3">
                  <EntityCard e={p.left} side="left" />
                  <div className="self-center text-gray-400 text-xs hidden md:block">←</div>
                  <EntityCard e={p.right} side="right" />
                </div>

                {p.status === "pending" && (
                  <div className="flex flex-wrap gap-2 pt-2 border-t border-gray-100">
                    <button
                      onClick={() => {
                        if (!window.confirm(`לאשר ולמזג?\n\nכל הקשרים של "${p.right.name}" יועברו ל-"${p.left.name}", ושמה של ${p.right.name} יתווסף ככינוי. הפעולה אינה הפיכה.`)) return;
                        act(p.id, "approve");
                      }}
                      disabled={savingId === p.id}
                      className="px-3 py-1.5 text-sm rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50"
                    >
                      ✓ אשר ומזג ({p.left.name} ← {p.right.name})
                    </button>
                    <button
                      onClick={() => act(p.id, "reject")}
                      disabled={savingId === p.id}
                      className="px-3 py-1.5 text-sm rounded-lg border border-red-300 text-red-700 hover:bg-red-50 disabled:opacity-50"
                    >
                      דחה (לא אותה ישות)
                    </button>
                    <button
                      onClick={() => act(p.id, "dismiss")}
                      disabled={savingId === p.id}
                      className="px-3 py-1.5 text-sm rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50"
                    >
                      הסתר
                    </button>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
