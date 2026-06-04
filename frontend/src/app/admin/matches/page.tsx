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

interface ClusterMember extends EntitySummary {
  connection_count: number;
}

interface DuplicateCluster {
  entity_type: string;
  size: number;
  canonical_id: string;
  members: ClusterMember[];
  proposals: { id: string; left_id: string; right_id: string; score: number; reasons: string[] }[];
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
  const [clusters, setClusters] = useState<DuplicateCluster[]>([]);
  const [clustersLoading, setClustersLoading] = useState(false);
  const [clustersLimited, setClustersLimited] = useState(false);
  const [mergingCluster, setMergingCluster] = useState<string | null>(null);
  // Per-cluster override: when the admin wants to keep a member other
  // than the auto-recommended canonical, they pick from a dropdown and
  // we remember it for that cluster only.
  const [canonicalOverride, setCanonicalOverride] = useState<Record<string, string>>({});
  // Bulk-merge selection. Keyed on the cluster's *initial* canonical_id
  // (stable for the cluster's lifetime even if the admin overrides who
  // gets kept). Set semantics keep toggling O(1).
  const [selectedClusters, setSelectedClusters] = useState<Set<string>>(new Set());
  const [bulkMerging, setBulkMerging] = useState(false);

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

  const refreshClusters = useCallback(async () => {
    // Clusters depend on PENDING proposals; only fetch them when the user
    // is on the "ממתינות" tab, otherwise the API would return [] anyway.
    if (statusFilter !== "pending") {
      setClusters([]);
      return;
    }
    setClustersLoading(true);
    try {
      // Default to 30 — the server can hydrate ~500 IDs per request in
      // under 3s on Render free. Bigger N hits the proxy timeout when
      // mega-clusters (50-100 members) are in the top slice.
      const params = new URLSearchParams({ limit: "30" });
      if (kindFilter) params.set("entity_type", kindFilter);
      const res = await fetch(
        `/api/v1/admin/matches/clusters?${params}`,
        { credentials: "include" },
      );
      if (!res.ok) throw new Error(`שגיאה בטעינת אשכולות (${res.status})`);
      const data = await res.json();
      setClusters(Array.isArray(data.data) ? data.data : []);
      setClustersLimited(Boolean(data.meta?.limited));
    } catch (e) {
      // Don't blow up the whole page on cluster-fetch failure.
      // eslint-disable-next-line no-console
      console.error(e);
      setClusters([]);
      setClustersLimited(false);
    } finally {
      setClustersLoading(false);
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
    refreshClusters();
  }, [refresh, refreshScan, refreshClusters]);

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

  const [cleaning, setCleaning] = useState(false);

  // Data-quality sweep: first does a dry-run to count the junk, shows
  // the admin exactly what will be removed, then runs for real on
  // confirm. Junk = placeholder-name entities ("null"/"***"/"----") +
  // orphan relationships (edges pointing at non-existent entities).
  const runCleanup = async () => {
    setError(null);
    setCleaning(true);
    try {
      const dry = await fetch(`/api/v1/admin/audit/cleanup`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dry_run: true }),
      });
      if (!dry.ok) throw new Error(await dry.text());
      const dj = (await dry.json()).data;
      const garbageTotal = (Object.values(dj.garbage_entities || {}) as Array<{ entities_removed?: number }>)
        .reduce((s, v) => s + (v.entities_removed || 0), 0);
      const orphanTotal = dj.orphan_relationships?.total || 0;
      if (garbageTotal === 0 && orphanTotal === 0) {
        setError("לא נמצאו ישויות זבל או קשרים יתומים — המאגר נקי.");
        return;
      }
      const ok = window.confirm(
        `נמצאו:\n` +
        `• ${garbageTotal} ישויות עם שם פסול (null / *** / ---- וכו')\n` +
        `• ${orphanTotal} קשרים יתומים (מצביעים על ישות שלא קיימת)\n\n` +
        `למחוק את כולם? הפעולה אינה הפיכה.`,
      );
      if (!ok) return;
      const real = await fetch(`/api/v1/admin/audit/cleanup`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      if (!real.ok) throw new Error(await real.text());
      const rj = (await real.json()).data;
      const removed = (Object.values(rj.garbage_entities || {}) as Array<{ entities_removed?: number }>)
        .reduce((s, v) => s + (v.entities_removed || 0), 0);
      setError(
        `נוקו ${removed} ישויות זבל ו-${rj.orphan_relationships?.total || 0} קשרים יתומים. ✓`,
      );
      await Promise.all([refresh(), refreshClusters()]);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה בניקוי");
    } finally {
      setCleaning(false);
    }
  };

  const toggleClusterSelected = (canonicalId: string) => {
    setSelectedClusters((prev) => {
      const next = new Set(prev);
      if (next.has(canonicalId)) next.delete(canonicalId);
      else next.add(canonicalId);
      return next;
    });
  };

  const selectAllClusters = () => {
    setSelectedClusters(new Set(clusters.map((c) => c.canonical_id)));
  };

  const clearClusterSelection = () => {
    setSelectedClusters(new Set());
  };

  // Pre-compute the impact of the current selection so the action bar can
  // show "מזג N אשכולות (X ישויות יימחקו)".
  const selectedClusterList = clusters.filter((c) =>
    selectedClusters.has(c.canonical_id),
  );
  const selectedEntitiesToDelete = selectedClusterList.reduce((sum, c) => {
    // Each cluster of size N collapses into 1 → N-1 entities removed.
    return sum + Math.max(0, c.size - 1);
  }, 0);

  const bulkMergeSelected = async () => {
    if (selectedClusterList.length === 0) return;
    const confirmText =
      `למזג ${selectedClusterList.length} אשכולות בבת אחת?\n\n` +
      `סה"כ ${selectedEntitiesToDelete} ישויות יימחקו (הקשרים שלהן יועברו לישות הקנונית של כל אשכול).\n\n` +
      `הפעולה אינה הפיכה.`;
    if (!window.confirm(confirmText)) return;

    setBulkMerging(true);
    setError(null);
    try {
      const operations = selectedClusterList.map((c) => {
        const canonicalId = canonicalOverride[c.canonical_id] || c.canonical_id;
        return {
          entity_type: c.entity_type,
          canonical_id: canonicalId,
          member_ids: c.members.map((m) => m.id),
        };
      });
      const res = await fetch(`/api/v1/admin/matches/clusters/merge-batch`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ operations }),
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `שגיאה (${res.status})`);
      }
      const data = await res.json();
      const summary = data.data || {};
      const failed = summary.failed || 0;
      if (failed > 0) {
        setError(
          `${summary.succeeded || 0} אשכולות אוחדו, ${failed} נכשלו. רענון התצוגה...`,
        );
      }
      // Reset selection + refresh both feeds.
      setSelectedClusters(new Set());
      await Promise.all([refresh(), refreshClusters()]);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה במיזוג מרובה");
    } finally {
      setBulkMerging(false);
    }
  };

  const mergeCluster = async (cluster: DuplicateCluster) => {
    const canonicalId =
      canonicalOverride[cluster.canonical_id] || cluster.canonical_id;
    const canonical = cluster.members.find((m) => m.id === canonicalId);
    const others = cluster.members.filter((m) => m.id !== canonicalId);
    if (!canonical || others.length === 0) return;
    const confirmText =
      `למזג ${cluster.size} ישויות לאחת?\n\n` +
      `הישות שתישאר: "${canonical.name}" (${canonical.connection_count} קשרים)\n` +
      `הישויות שיימחקו (${others.length}): ${others.map((m) => `"${m.name}"`).join(", ")}\n\n` +
      `הקשרים של כל ה-${others.length} יועברו לישות שנשארת. הפעולה אינה הפיכה.`;
    if (!window.confirm(confirmText)) return;
    setMergingCluster(cluster.canonical_id);
    setError(null);
    try {
      const res = await fetch(`/api/v1/admin/matches/clusters/merge`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          entity_type: cluster.entity_type,
          canonical_id: canonicalId,
          member_ids: cluster.members.map((m) => m.id),
        }),
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `שגיאה (${res.status})`);
      }
      await Promise.all([refresh(), refreshClusters()]);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה במיזוג אשכול");
    } finally {
      setMergingCluster(null);
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
          <button
            onClick={runCleanup}
            disabled={cleaning}
            className="px-3 py-2 rounded-lg border border-red-300 text-sm text-red-700 hover:bg-red-50 disabled:opacity-50"
            title="מחיקת ישויות עם שם פסול (null/***/----) וקשרים יתומים"
          >
            {cleaning ? "מנקה..." : "נקה זבל"}
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

      {/* ── Clusters block — only on the "pending" tab. Most cleanup wins
            happen here: one click collapses 30 הצלחה rows in a single shot.
            The per-pair list below stays available for low-confidence
            cases where a human still wants to inspect each edge. */}
      {statusFilter === "pending" && (
        <section className="mb-6">
          <div className="flex items-center justify-between mb-2 gap-2 flex-wrap">
            <h2 className="text-base font-semibold text-gray-800">
              אשכולות כפילויות
              {clusters.length > 0 && (
                <span className="text-xs font-normal text-gray-500 mr-2">
                  ({clusters.length} {clustersLimited ? "אשכולות (הגדולים ביותר) · " : "אשכולות · "}{clusters.reduce((s, c) => s + c.size, 0)} ישויות)
                </span>
              )}
            </h2>
            <span className="text-xs text-gray-400">
              {clustersLimited
                ? "מציג את 30 הגדולים. לאחר מיזוג רענן לראות את הבאים."
                : "סמן כמה שתרצה ולחץ \"מזג בחירה\". הקנונית נבחרת אוטומטית לפי מס' הקשרים."}
            </span>
          </div>

          {/* Sticky bulk-action bar. Shows the running total of impact
              ("X אשכולות · Y ישויות יימחקו") so the admin can see what
              they're about to do before clicking. Stays glued to the
              top of the section while scrolling through hundreds of
              clusters. */}
          {clusters.length > 0 && (
            <div className="sticky top-14 sm:top-16 z-30 -mx-4 sm:-mx-6 lg:-mx-8 px-4 sm:px-6 lg:px-8 py-2 bg-amber-100/95 backdrop-blur border-y border-amber-300 mb-3 flex items-center justify-between gap-3 flex-wrap">
              <div className="flex items-center gap-3 flex-wrap text-sm">
                <button
                  type="button"
                  onClick={
                    selectedClusters.size === clusters.length
                      ? clearClusterSelection
                      : selectAllClusters
                  }
                  className="px-3 py-1.5 text-xs rounded-lg border border-amber-400 bg-white text-amber-900 hover:bg-amber-50 font-medium"
                >
                  {selectedClusters.size === clusters.length
                    ? "בטל בחירה"
                    : `סמן את כל ${clusters.length}`}
                </button>
                {selectedClusters.size > 0 && (
                  <button
                    type="button"
                    onClick={clearClusterSelection}
                    className="text-xs text-amber-800 hover:underline"
                  >
                    נקה בחירה
                  </button>
                )}
                <span className="text-amber-900">
                  <strong>{selectedClusters.size}</strong> אשכולות נבחרו
                  {selectedClusters.size > 0 && (
                    <span className="text-amber-800">
                      {" "}· <strong>{selectedEntitiesToDelete}</strong> ישויות יימחקו
                    </span>
                  )}
                </span>
              </div>
              <button
                type="button"
                onClick={bulkMergeSelected}
                disabled={selectedClusters.size === 0 || bulkMerging}
                className="px-4 py-1.5 text-sm rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50 disabled:cursor-not-allowed font-medium"
              >
                {bulkMerging
                  ? `ממזג ${selectedClusters.size} אשכולות...`
                  : `מזג בחירה (${selectedClusters.size})`}
              </button>
            </div>
          )}

          {clustersLoading ? (
            <div className="text-center py-8 text-gray-400 text-sm">טוען אשכולות...</div>
          ) : clusters.length === 0 ? (
            <div className="text-center py-6 text-gray-400 text-sm bg-gray-50 border border-gray-200 rounded-lg">
              אין כרגע אשכולות לפתרון. {items.length === 0 && "הפעל סריקה כדי להתחיל."}
            </div>
          ) : (
            <div className="space-y-3">
              {clusters.map((cluster) => {
                const overrideId = canonicalOverride[cluster.canonical_id];
                const canonicalId = overrideId || cluster.canonical_id;
                const totalConnections = cluster.members.reduce(
                  (s, m) => s + (m.connection_count || 0),
                  0,
                );
                const isMerging = mergingCluster === cluster.canonical_id;
                return (
                  <div
                    key={`${cluster.entity_type}-${cluster.canonical_id}`}
                    className={`border rounded-lg p-4 transition-colors ${
                      selectedClusters.has(cluster.canonical_id)
                        ? "bg-amber-100 border-amber-400 ring-2 ring-amber-300"
                        : "bg-amber-50 border-amber-200"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2 flex-wrap mb-3">
                      <label className="flex items-center gap-2 flex-wrap cursor-pointer">
                        <input
                          type="checkbox"
                          checked={selectedClusters.has(cluster.canonical_id)}
                          onChange={() => toggleClusterSelected(cluster.canonical_id)}
                          disabled={bulkMerging || isMerging}
                          className="w-4 h-4 shrink-0 accent-amber-600"
                          title="סמן לאיחוד מרובה"
                        />
                        <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${KIND_BADGE[cluster.entity_type] || "bg-gray-100 text-gray-700"}`}>
                          {KIND_LABEL[cluster.entity_type] || cluster.entity_type}
                        </span>
                        <span className="text-sm font-semibold text-amber-900">
                          {cluster.size} ישויות
                        </span>
                        <span className="text-xs text-amber-800">
                          · {totalConnections.toLocaleString()} קשרים סך הכל
                        </span>
                      </label>
                      <button
                        onClick={() => mergeCluster(cluster)}
                        disabled={isMerging || bulkMerging}
                        className="px-3 py-1.5 text-sm rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50 font-medium"
                      >
                        {isMerging ? "ממזג..." : `מזג את כל ${cluster.size} ל"${cluster.members.find((m) => m.id === canonicalId)?.name || "?"}"`}
                      </button>
                    </div>

                    {/* Member list — first row is recommended canonical
                        (or whatever the user picked via the radio).
                        Each row shows its connection count + a radio
                        button to override the canonical selection. */}
                    <div className="bg-white border border-amber-100 rounded-lg divide-y divide-gray-100">
                      {cluster.members.map((m) => {
                        const isCanonical = m.id === canonicalId;
                        return (
                          <label
                            key={m.id}
                            className={`flex items-center justify-between gap-3 px-3 py-2 cursor-pointer ${
                              isCanonical ? "bg-emerald-50" : "hover:bg-gray-50"
                            }`}
                          >
                            <div className="flex items-center gap-2 min-w-0 flex-1">
                              <input
                                type="radio"
                                name={`cluster-${cluster.canonical_id}`}
                                checked={isCanonical}
                                onChange={() =>
                                  setCanonicalOverride((prev) => ({
                                    ...prev,
                                    [cluster.canonical_id]: m.id,
                                  }))
                                }
                                disabled={isMerging}
                                className="shrink-0"
                              />
                              <span className={`text-sm truncate ${isCanonical ? "font-semibold text-gray-900" : "text-gray-700"}`}>
                                {m.name || "(ללא שם)"}
                              </span>
                              {(m.title || m.position || m.ministry) && (
                                <span className="text-xs text-gray-400 truncate">
                                  · {[m.title, m.position, m.ministry].filter(Boolean).join(" · ")}
                                </span>
                              )}
                              {Array.isArray(m.aliases) && m.aliases.length > 0 && (
                                <span className="text-xs text-gray-400 truncate hidden sm:inline">
                                  · כינויים: {m.aliases.slice(0, 2).join(", ")}
                                  {m.aliases.length > 2 && ` (+${m.aliases.length - 2})`}
                                </span>
                              )}
                            </div>
                            <div className="flex items-center gap-2 shrink-0">
                              <span className={`text-xs ${isCanonical ? "text-emerald-700 font-semibold" : "text-gray-500"}`}>
                                {m.connection_count} קשרים
                              </span>
                              {isCanonical && (
                                <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-emerald-200 text-emerald-900 font-medium">
                                  נשמרת
                                </span>
                              )}
                              <Link
                                href={
                                  cluster.entity_type === "person"
                                    ? `/admin/entities/detail?type=persons&id=${m.id}`
                                    : cluster.entity_type === "company"
                                      ? `/admin/entities/detail?type=companies&id=${m.id}`
                                      : `/admin/entities/detail?type=associations&id=${m.id}`
                                }
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-xs text-primary-600 hover:underline"
                              >
                                ↗
                              </Link>
                            </div>
                          </label>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </section>
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
