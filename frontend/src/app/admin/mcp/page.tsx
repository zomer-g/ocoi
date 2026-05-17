"use client";

import { useCallback, useEffect, useState } from "react";

interface McpStats {
  total_mcp_users: number;
  active_users_30d: number;
  calls_30d: number;
  metered_users: number;
}

interface McpUser {
  id: string;
  email: string;
  name: string;
  role: string;
  last_login_at: string | null;
  calls_30d: number;
  bytes_out_30d: number;
  plan: "free" | "metered";
  monthly_quota: number | null;
  stripe_customer_id: string | null;
}

interface McpClient {
  id: string;
  client_id: string;
  client_name: string | null;
  is_public: boolean;
  redirect_uris: string[];
  created_at: string | null;
  revoked_at: string | null;
}

interface McpEvent {
  id: string;
  tool: string;
  client_id: string | null;
  started_at: string | null;
  duration_ms: number | null;
  bytes_out: number | null;
  status: string;
  error_message: string | null;
  stripe_pushed_at: string | null;
}

function fmtDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("he-IL", {
      day: "numeric", month: "numeric", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch { return iso; }
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

export default function AdminMcpPage() {
  const [stats, setStats] = useState<McpStats | null>(null);
  const [users, setUsers] = useState<McpUser[]>([]);
  const [clients, setClients] = useState<McpClient[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedUser, setExpandedUser] = useState<string | null>(null);
  const [events, setEvents] = useState<Record<string, McpEvent[]>>({});
  const [savingId, setSavingId] = useState<string | null>(null);
  const [showInvite, setShowInvite] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteName, setInviteName] = useState("");
  const [inviteQuota, setInviteQuota] = useState("");
  const [inviting, setInviting] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [statsRes, usersRes, clientsRes] = await Promise.all([
        fetch("/api/v1/admin/mcp/stats", { credentials: "include" }),
        fetch("/api/v1/admin/mcp/users", { credentials: "include" }),
        fetch("/api/v1/admin/mcp/clients", { credentials: "include" }),
      ]);
      if (statsRes.status === 401 || statsRes.status === 403) {
        throw new Error("נדרשת הרשאת מנהל מערכת.");
      }
      if (!statsRes.ok || !usersRes.ok || !clientsRes.ok) {
        throw new Error("שגיאה בטעינת נתוני MCP");
      }
      const sj = await statsRes.json();
      const uj = await usersRes.json();
      const cj = await clientsRes.json();
      setStats(sj.data ?? null);
      setUsers(Array.isArray(uj.data) ? uj.data : []);
      setClients(Array.isArray(cj.data) ? cj.data : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  const expandUser = async (id: string) => {
    if (expandedUser === id) {
      setExpandedUser(null);
      return;
    }
    setExpandedUser(id);
    if (!events[id]) {
      const r = await fetch(`/api/v1/admin/mcp/users/${id}/events`, { credentials: "include" });
      if (r.ok) {
        const j = await r.json();
        setEvents((prev) => ({ ...prev, [id]: Array.isArray(j.data) ? j.data : [] }));
      }
    }
  };

  const updateUserBilling = async (id: string, patch: Partial<McpUser>) => {
    setSavingId(id);
    try {
      const r = await fetch(`/api/v1/admin/mcp/users/${id}`, {
        method: "PATCH",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      });
      if (!r.ok) throw new Error(await r.text());
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setSavingId(null);
    }
  };

  const inviteUser = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!inviteEmail.includes("@")) {
      setError('דוא"ל לא תקין');
      return;
    }
    setInviting(true);
    try {
      const body: Record<string, unknown> = {
        email: inviteEmail.trim().toLowerCase(),
        name: inviteName.trim() || inviteEmail.trim(),
      };
      if (inviteQuota.trim()) {
        const q = parseInt(inviteQuota, 10);
        if (!Number.isNaN(q) && q >= 0) body.monthly_quota = q;
      }
      const r = await fetch("/api/v1/admin/mcp/invites", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error(await r.text());
      setInviteEmail("");
      setInviteName("");
      setInviteQuota("");
      setShowInvite(false);
      await refresh();
    } catch (e2) {
      setError(e2 instanceof Error ? e2.message : "שגיאה");
    } finally {
      setInviting(false);
    }
  };

  const removeUser = async (id: string, email: string) => {
    if (!confirm(`לבטל את הגישה של ${email}?`)) return;
    setSavingId(id);
    try {
      const r = await fetch(`/api/v1/admin/mcp/users/${id}`, {
        method: "DELETE",
        credentials: "include",
      });
      if (!r.ok) throw new Error(await r.text());
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setSavingId(null);
    }
  };

  const revokeClient = async (clientPk: string, name: string) => {
    if (!confirm(`לבטל את הלקוח ${name}? לקוח שבוטל לא יוכל להתחבר.`)) return;
    const r = await fetch(`/api/v1/admin/mcp/clients/${clientPk}`, {
      method: "DELETE",
      credentials: "include",
    });
    if (r.ok) refresh();
  };

  return (
    <div>
      <div className="mb-4">
        <h1 className="text-2xl font-bold text-gray-900">שרת MCP</h1>
        <p className="text-sm text-gray-500 mt-1">
          ניהול גישה לדאטה דרך Model Context Protocol — שימוש פר-יוזר ובילינג.
        </p>
      </div>

      {error && (
        <div className="mb-4 p-3 rounded-lg bg-red-50 text-red-700 text-sm">{error}</div>
      )}

      {/* Stats cards */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
          <StatCard label="משתמשי MCP" value={stats.total_mcp_users} />
          <StatCard label="פעילים (30 יום)" value={stats.active_users_30d} />
          <StatCard label="קריאות (30 יום)" value={stats.calls_30d} />
          <StatCard label="חשבונות חיוב פעילים" value={stats.metered_users} />
        </div>
      )}

      {/* Users */}
      <section className="mb-8">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-gray-800">משתמשים מוזמנים</h2>
          <button
            onClick={() => setShowInvite((v) => !v)}
            className="px-3 py-1.5 text-sm rounded-lg bg-primary-700 text-white font-medium hover:bg-primary-800"
          >
            {showInvite ? "ביטול" : "+ הזמן משתמש"}
          </button>
        </div>

        {showInvite && (
          <form
            onSubmit={inviteUser}
            className="mb-4 p-4 bg-white border border-gray-200 rounded-xl space-y-3"
          >
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div>
                <label className="block text-xs text-gray-500 mb-1">דוא"ל (Google)</label>
                <input
                  type="email"
                  value={inviteEmail}
                  onChange={(e) => setInviteEmail(e.target.value)}
                  placeholder="user@example.com"
                  dir="ltr"
                  required
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">שם להצגה</label>
                <input
                  type="text"
                  value={inviteName}
                  onChange={(e) => setInviteName(e.target.value)}
                  placeholder="(אופציונלי)"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">מכסה חודשית (לא חובה)</label>
                <input
                  type="number"
                  min={0}
                  value={inviteQuota}
                  onChange={(e) => setInviteQuota(e.target.value)}
                  placeholder="ללא הגבלה"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm"
                />
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setShowInvite(false)}
                className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800"
              >
                ביטול
              </button>
              <button
                type="submit"
                disabled={inviting}
                className="px-4 py-1.5 text-sm rounded-lg bg-primary-700 text-white font-medium hover:bg-primary-800 disabled:opacity-50"
              >
                {inviting ? "מזמין..." : "שלח הזמנה"}
              </button>
            </div>
            <p className="text-xs text-gray-500">
              המשתמש יוכל להיכנס דרך Claude/Cursor עם חשבון Google של אותה כתובת. אין צורך לשלוח לו דבר ידנית — תן לו פשוט את כתובת ה-MCP.
            </p>
          </form>
        )}
        {loading ? (
          <div className="text-center py-8 text-gray-400">טוען...</div>
        ) : users.length === 0 ? (
          <div className="text-center py-8 text-gray-400 bg-white border border-gray-200 rounded-lg">
            אין עדיין משתמשי MCP. כשמישהו יחבר Claude/Cursor הוא יופיע כאן.
          </div>
        ) : (
          <div className="space-y-2">
            {users.map((u) => {
              const isOpen = expandedUser === u.id;
              return (
                <div key={u.id} className="bg-white border border-gray-200 rounded-lg p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-semibold text-gray-900">{u.name || u.email}</span>
                        <PlanBadge plan={u.plan} />
                        {u.monthly_quota !== null && (
                          <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-700 border border-gray-200">
                            מכסה: {u.monthly_quota}/חודש
                          </span>
                        )}
                      </div>
                      <div className="text-sm text-gray-500 truncate mt-0.5" dir="ltr">{u.email}</div>
                      <div className="text-xs text-gray-400 mt-1">
                        קריאות 30 ימים: {u.calls_30d} · נתונים: {fmtBytes(u.bytes_out_30d)} · התחברות אחרונה: {fmtDate(u.last_login_at)}
                      </div>
                    </div>
                    <div className="shrink-0 flex items-center gap-2">
                      <button
                        onClick={() => expandUser(u.id)}
                        className="text-xs text-primary-600 hover:underline"
                      >
                        {isOpen ? "סגור" : "פרטים"}
                      </button>
                      <button
                        onClick={() => removeUser(u.id, u.email)}
                        disabled={savingId === u.id}
                        className="text-xs text-red-500 hover:text-red-700 disabled:opacity-50"
                      >
                        בטל
                      </button>
                    </div>
                  </div>

                  {isOpen && (
                    <div className="mt-3 pt-3 border-t border-gray-200 space-y-3">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-xs text-gray-500">תכנית:</span>
                        {(["free", "metered"] as const).map((p) => (
                          <button
                            key={p}
                            disabled={savingId === u.id || u.plan === p}
                            onClick={() => updateUserBilling(u.id, { plan: p })}
                            className={`text-xs px-3 py-1 rounded-full border transition-colors ${
                              u.plan === p
                                ? "bg-primary-700 text-white border-primary-700"
                                : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
                            } disabled:opacity-50`}
                          >
                            {p === "free" ? "חינם" : "חיוב לפי שימוש"}
                          </button>
                        ))}
                      </div>
                      <div className="flex items-center gap-2">
                        <label className="text-xs text-gray-500">מכסה חודשית (השאר ריק = ללא הגבלה):</label>
                        <input
                          type="number"
                          defaultValue={u.monthly_quota ?? ""}
                          min={0}
                          className="px-2 py-1 border border-gray-300 rounded text-sm w-24"
                          onBlur={(e) => {
                            const v = e.target.value;
                            const q = v === "" ? null : parseInt(v, 10);
                            if (q === u.monthly_quota) return;
                            updateUserBilling(u.id, { monthly_quota: q });
                          }}
                        />
                      </div>
                      {u.stripe_customer_id && (
                        <div className="text-xs text-gray-500" dir="ltr">
                          Stripe customer: <span className="font-mono">{u.stripe_customer_id}</span>
                        </div>
                      )}

                      <div>
                        <div className="text-xs text-gray-500 mb-1">קריאות אחרונות:</div>
                        <div className="overflow-x-auto">
                          <table className="min-w-full text-xs">
                            <thead>
                              <tr className="text-gray-400 border-b border-gray-200">
                                <th className="text-start py-1 px-2">זמן</th>
                                <th className="text-start py-1 px-2">כלי</th>
                                <th className="text-start py-1 px-2">משך</th>
                                <th className="text-start py-1 px-2">סטטוס</th>
                                <th className="text-start py-1 px-2">Stripe</th>
                              </tr>
                            </thead>
                            <tbody>
                              {(events[u.id] || []).slice(0, 30).map((ev) => (
                                <tr key={ev.id} className="border-b border-gray-100">
                                  <td className="py-1 px-2 text-gray-700">{fmtDate(ev.started_at)}</td>
                                  <td className="py-1 px-2 font-mono">{ev.tool}</td>
                                  <td className="py-1 px-2 text-gray-600">{ev.duration_ms ?? "—"}ms</td>
                                  <td className={`py-1 px-2 ${ev.status === "ok" ? "text-emerald-700" : "text-red-700"}`}>
                                    {ev.status}
                                  </td>
                                  <td className="py-1 px-2 text-gray-500">
                                    {ev.stripe_pushed_at ? "✓" : "—"}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                          {!events[u.id]?.length && (
                            <div className="text-center text-gray-400 py-2">אין קריאות עדיין</div>
                          )}
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </section>

      {/* OAuth clients */}
      <section>
        <h2 className="text-lg font-semibold text-gray-800 mb-3">לקוחות OAuth רשומים</h2>
        {clients.length === 0 ? (
          <div className="text-center py-8 text-gray-400 bg-white border border-gray-200 rounded-lg">
            אין עדיין לקוחות רשומים.
          </div>
        ) : (
          <div className="space-y-2">
            {clients.map((c) => (
              <div key={c.id} className="bg-white border border-gray-200 rounded-lg p-3 flex items-center justify-between">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <span className="font-medium text-gray-900">{c.client_name || "(ללא שם)"}</span>
                    {c.is_public && (
                      <span className="text-xs px-2 py-0.5 rounded-full bg-blue-50 text-blue-700 border border-blue-200">public (PKCE)</span>
                    )}
                    {c.revoked_at && (
                      <span className="text-xs px-2 py-0.5 rounded-full bg-red-50 text-red-700 border border-red-200">בוטל</span>
                    )}
                  </div>
                  <div className="text-xs text-gray-500 mt-0.5 font-mono" dir="ltr">{c.client_id}</div>
                  <div className="text-xs text-gray-400 mt-0.5">
                    נרשם: {fmtDate(c.created_at)}
                  </div>
                </div>
                {!c.revoked_at && (
                  <button
                    onClick={() => revokeClient(c.id, c.client_name || c.client_id)}
                    className="text-xs text-red-600 hover:text-red-800"
                  >
                    בטל
                  </button>
                )}
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <div className="text-xs text-gray-500">{label}</div>
      <div className="text-2xl font-bold text-gray-900 mt-1">{value.toLocaleString("he-IL")}</div>
    </div>
  );
}

function PlanBadge({ plan }: { plan: "free" | "metered" }) {
  const cls = plan === "metered"
    ? "bg-emerald-50 text-emerald-700 border-emerald-200"
    : "bg-gray-100 text-gray-700 border-gray-200";
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full border ${cls}`}>
      {plan === "metered" ? "חיוב לפי שימוש" : "חינם"}
    </span>
  );
}
