"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";

interface Suggestion {
  id: string;
  target_kind: string;
  target_id: string;
  field_name: string;
  current_value: string | null;
  proposed_value: string | null;
  comment: string | null;
  submitter_email: string | null;
  document_id: string | null;
  status: "pending" | "approved" | "rejected";
  admin_notes: string | null;
  resolved_at: string | null;
  created_at: string | null;
}

const STATUS_LABEL: Record<string, string> = {
  pending: "ממתין",
  approved: "אושר",
  rejected: "נדחה",
};

const STATUS_COLOR: Record<string, string> = {
  pending: "bg-amber-100 text-amber-700 border-amber-200",
  approved: "bg-green-100 text-green-700 border-green-200",
  rejected: "bg-gray-200 text-gray-600 border-gray-300",
};

const KIND_LABEL: Record<string, string> = {
  document: "מסמך",
  person: "אדם",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
  relationship: "קשר",
};

const FIELD_LABEL: Record<string, string> = {
  title: "כותרת",
  name_hebrew: "שם",
  position: "תפקיד",
  ministry: "משרד",
  details: "פרטים",
  relationship_type: "סוג קשר",
  general: "הערה כללית",
};

const PAGE_SIZE = 50;

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("he-IL", {
      day: "numeric", month: "numeric", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch { return iso; }
}

export default function AdminSuggestionsPage() {
  const [items, setItems] = useState<Suggestion[]>([]);
  const [total, setTotal] = useState(0);
  const [pages, setPages] = useState(0);
  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState<string>("pending");
  const [kindFilter, setKindFilter] = useState<string>("");
  const [loading, setLoading] = useState(true);
  const [savingId, setSavingId] = useState<string | null>(null);
  const [editing, setEditing] = useState<{ id: string; notes: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fetchItems = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        page: String(page),
        limit: String(PAGE_SIZE),
      });
      if (statusFilter) params.set("status", statusFilter);
      if (kindFilter) params.set("target_kind", kindFilter);
      const res = await fetch(`/api/v1/admin/suggestions?${params}`, { credentials: "include" });
      const data = await res.json();
      setItems(data.data || []);
      setTotal(data.meta?.total || 0);
      setPages(data.meta?.pages || 0);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה בטעינה");
    } finally {
      setLoading(false);
    }
  }, [page, statusFilter, kindFilter]);

  useEffect(() => {
    fetchItems();
  }, [fetchItems]);

  // Reset page when filters change
  useEffect(() => {
    setPage(1);
  }, [statusFilter, kindFilter]);

  const updateStatus = async (id: string, newStatus: "approved" | "rejected" | "pending", notes?: string) => {
    setSavingId(id);
    try {
      const body: Record<string, unknown> = { status: newStatus };
      if (notes !== undefined) body.admin_notes = notes;
      const res = await fetch(`/api/v1/admin/suggestions/${id}`, {
        method: "PATCH",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error("שגיאה בעדכון");
      await fetchItems();
      setEditing(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה בעדכון");
    } finally {
      setSavingId(null);
    }
  };

  const remove = async (id: string) => {
    if (!confirm("למחוק את ההצעה לצמיתות?")) return;
    setSavingId(id);
    try {
      const res = await fetch(`/api/v1/admin/suggestions/${id}`, {
        method: "DELETE",
        credentials: "include",
      });
      if (!res.ok) throw new Error("שגיאה במחיקה");
      await fetchItems();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה במחיקה");
    } finally {
      setSavingId(null);
    }
  };

  const targetLink = (s: Suggestion): { href: string; label: string } => {
    if (s.target_kind === "document") {
      return { href: `/admin/documents/detail?id=${s.target_id}`, label: "המסמך" };
    }
    if (["person", "company", "association", "domain"].includes(s.target_kind)) {
      const tab = s.target_kind === "person" ? "persons"
        : s.target_kind === "company" ? "companies"
        : s.target_kind === "association" ? "associations" : "domains";
      return {
        href: `/admin/entities/detail?type=${tab}&id=${s.target_id}`,
        label: KIND_LABEL[s.target_kind] || s.target_kind,
      };
    }
    return { href: "#", label: KIND_LABEL[s.target_kind] || s.target_kind };
  };

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-2">הצעות לתיקון</h1>
      <p className="text-sm text-gray-500 mb-5">
        הצעות שהוגשו על ידי משתמשים מהדפים הציבוריים. אישור / דחייה משנים את הסטטוס בלבד — את הנתונים יש לעדכן ידנית בעמודי הניהול הרלוונטיים.
      </p>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2 mb-5">
        <div className="flex items-center gap-1">
          <span className="text-xs text-gray-500">סטטוס:</span>
          {[
            { v: "pending", l: "ממתין" },
            { v: "approved", l: "אושר" },
            { v: "rejected", l: "נדחה" },
            { v: "", l: "הכל" },
          ].map((opt) => (
            <button
              key={opt.v || "all"}
              onClick={() => setStatusFilter(opt.v)}
              className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                statusFilter === opt.v
                  ? "bg-primary-700 text-white border-primary-700"
                  : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
              }`}
            >
              {opt.l}
            </button>
          ))}
        </div>
        <span className="text-gray-300">|</span>
        <div className="flex items-center gap-1">
          <span className="text-xs text-gray-500">סוג:</span>
          {[
            { v: "", l: "הכל" },
            { v: "document", l: "מסמך" },
            { v: "person", l: "אדם" },
            { v: "company", l: "חברה" },
            { v: "association", l: "עמותה" },
            { v: "domain", l: "תחום" },
          ].map((opt) => (
            <button
              key={opt.v || "all-kind"}
              onClick={() => setKindFilter(opt.v)}
              className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                kindFilter === opt.v
                  ? "bg-gray-800 text-white border-gray-800"
                  : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
              }`}
            >
              {opt.l}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <div className="mb-4 p-3 rounded-lg bg-red-50 text-red-700 text-sm">{error}</div>
      )}

      <p className="text-xs text-gray-400 mb-3">{total.toLocaleString()} הצעות</p>

      {loading ? (
        <div className="text-center py-12 text-gray-400">טוען...</div>
      ) : items.length === 0 ? (
        <div className="text-center py-12 text-gray-400">אין הצעות בתצוגה הזו</div>
      ) : (
        <div className="space-y-3">
          {items.map((s) => {
            const link = targetLink(s);
            const fieldLabel = FIELD_LABEL[s.field_name] || s.field_name;
            return (
              <div
                key={s.id}
                className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm"
              >
                <div className="flex items-start justify-between gap-3 mb-2">
                  <div className="flex flex-wrap items-center gap-2 text-sm">
                    <span className={`text-xs px-2 py-0.5 rounded-full border ${STATUS_COLOR[s.status]}`}>
                      {STATUS_LABEL[s.status] || s.status}
                    </span>
                    <span className="text-xs text-gray-500">
                      {KIND_LABEL[s.target_kind] || s.target_kind} · {fieldLabel}
                    </span>
                    <span className="text-xs text-gray-300">·</span>
                    <Link
                      href={link.href}
                      className="text-xs text-primary-600 hover:underline"
                    >
                      פתח {link.label}
                    </Link>
                    {s.document_id && s.target_kind !== "document" && (
                      <>
                        <span className="text-xs text-gray-300">·</span>
                        <Link
                          href={`/admin/documents/detail?id=${s.document_id}`}
                          className="text-xs text-primary-600 hover:underline"
                        >
                          מסמך מקור
                        </Link>
                      </>
                    )}
                  </div>
                  <span className="text-xs text-gray-400 shrink-0">{formatDate(s.created_at)}</span>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-2">
                  <div>
                    <div className="text-xs text-gray-500 mb-1">ערך נוכחי</div>
                    <div className="px-3 py-2 bg-gray-50 border border-gray-200 rounded text-sm text-gray-700 whitespace-pre-wrap break-words min-h-[2.5rem]">
                      {s.current_value || <span className="text-gray-400">—</span>}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-500 mb-1">ערך מוצע</div>
                    <div className="px-3 py-2 bg-primary-50 border border-primary-200 rounded text-sm text-gray-900 font-medium whitespace-pre-wrap break-words min-h-[2.5rem]">
                      {s.proposed_value || <span className="text-gray-400">—</span>}
                    </div>
                  </div>
                </div>

                {s.comment && (
                  <div className="mt-3">
                    <div className="text-xs text-gray-500 mb-1">הערה</div>
                    <div className="px-3 py-2 bg-amber-50 border border-amber-200 rounded text-sm text-gray-800 whitespace-pre-wrap">
                      {s.comment}
                    </div>
                  </div>
                )}

                {s.submitter_email && (
                  <div className="mt-2 text-xs text-gray-500">
                    נשלח על ידי: <a href={`mailto:${s.submitter_email}`} className="text-primary-600 hover:underline" dir="ltr">{s.submitter_email}</a>
                  </div>
                )}

                {/* Admin notes view / edit */}
                <div className="mt-3 pt-3 border-t border-gray-100">
                  {editing?.id === s.id ? (
                    <div className="flex gap-2 items-start">
                      <textarea
                        value={editing.notes}
                        onChange={(e) => setEditing({ id: s.id, notes: e.target.value })}
                        rows={2}
                        className="flex-1 px-2 py-1.5 text-sm border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-primary-500"
                        placeholder="הערות פנימיות..."
                      />
                      <button
                        onClick={() => updateStatus(s.id, s.status, editing.notes)}
                        disabled={savingId === s.id}
                        className="px-3 py-1.5 text-sm rounded bg-primary-700 text-white hover:bg-primary-800 disabled:opacity-50"
                      >
                        שמור
                      </button>
                      <button
                        onClick={() => setEditing(null)}
                        className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800"
                      >
                        ביטול
                      </button>
                    </div>
                  ) : (
                    <div className="flex items-center justify-between">
                      <div className="text-xs text-gray-500">
                        {s.admin_notes ? (
                          <>
                            <span className="font-medium">הערה פנימית: </span>
                            <span>{s.admin_notes}</span>
                          </>
                        ) : (
                          <span className="text-gray-300">אין הערות פנימיות</span>
                        )}
                      </div>
                      <button
                        onClick={() => setEditing({ id: s.id, notes: s.admin_notes || "" })}
                        className="text-xs text-primary-600 hover:underline"
                      >
                        ערוך הערה
                      </button>
                    </div>
                  )}
                </div>

                {/* Action buttons */}
                <div className="mt-3 flex flex-wrap gap-2">
                  {s.status !== "approved" && (
                    <button
                      onClick={() => updateStatus(s.id, "approved")}
                      disabled={savingId === s.id}
                      className="px-3 py-1.5 text-sm rounded bg-green-600 text-white hover:bg-green-700 disabled:opacity-50"
                    >
                      סמן כאושר
                    </button>
                  )}
                  {s.status !== "rejected" && (
                    <button
                      onClick={() => updateStatus(s.id, "rejected")}
                      disabled={savingId === s.id}
                      className="px-3 py-1.5 text-sm rounded bg-gray-600 text-white hover:bg-gray-700 disabled:opacity-50"
                    >
                      דחה
                    </button>
                  )}
                  {s.status !== "pending" && (
                    <button
                      onClick={() => updateStatus(s.id, "pending")}
                      disabled={savingId === s.id}
                      className="px-3 py-1.5 text-sm rounded border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50"
                    >
                      החזר ל"ממתין"
                    </button>
                  )}
                  <button
                    onClick={() => remove(s.id)}
                    disabled={savingId === s.id}
                    className="px-3 py-1.5 text-sm rounded text-red-600 hover:bg-red-50 disabled:opacity-50 mr-auto"
                  >
                    מחק
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {pages > 1 && (
        <div className="flex items-center justify-center gap-2 mt-6">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            className="px-3 py-1.5 rounded border border-gray-300 text-sm disabled:opacity-40"
          >
            הקודם
          </button>
          <span className="text-sm text-gray-500">
            עמוד {page} מתוך {pages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(pages, p + 1))}
            disabled={page >= pages}
            className="px-3 py-1.5 rounded border border-gray-300 text-sm disabled:opacity-40"
          >
            הבא
          </button>
        </div>
      )}
    </div>
  );
}
