"use client";

import { useEffect, useState, useCallback } from "react";
import { getMe, type AdminUser } from "@/lib/auth";

interface AdminUserRow {
  id: string;
  email: string;
  name: string;
  role: "admin" | "content_manager";
  permissions: string[];
  last_login_at: string | null;
  created_at: string | null;
}

interface PermissionDef {
  key: string;
  label: string;
}

const ROLE_LABEL: Record<string, string> = {
  admin: "מנהל מערכת",
  content_manager: "מנהל/ת תוכן",
};

const ROLE_BADGE: Record<string, string> = {
  admin: "bg-amber-100 text-amber-800 border-amber-200",
  content_manager: "bg-primary-100 text-primary-800 border-primary-200",
};

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("he-IL", {
      day: "numeric", month: "numeric", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch { return iso; }
}

export default function AdminUsersPage() {
  const [me, setMe] = useState<AdminUser | null>(null);
  const [users, setUsers] = useState<AdminUserRow[]>([]);
  const [permissionCatalog, setPermissionCatalog] = useState<PermissionDef[]>([]);
  const [defaultPerms, setDefaultPerms] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [showAddForm, setShowAddForm] = useState(false);
  const [savingId, setSavingId] = useState<string | null>(null);

  // Add-form state
  const [newEmail, setNewEmail] = useState("");
  const [newName, setNewName] = useState("");
  const [newRole, setNewRole] = useState<"admin" | "content_manager">("content_manager");
  const [newPerms, setNewPerms] = useState<string[]>([]);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [usersRes, catRes] = await Promise.all([
        fetch("/api/v1/admin/users", { credentials: "include" }),
        fetch("/api/v1/admin/permissions/catalog", { credentials: "include" }),
      ]);
      if (!usersRes.ok) throw new Error("שגיאה בטעינת המשתמשים");
      if (!catRes.ok) throw new Error("שגיאה בטעינת קטלוג ההרשאות");
      const usersData = await usersRes.json();
      const catData = await catRes.json();
      setUsers(usersData.data || []);
      setPermissionCatalog(catData.data?.permissions || []);
      setDefaultPerms(catData.data?.default_content_manager || []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    getMe().then(setMe);
    refresh();
  }, [refresh]);

  // Seed add-form perms with the recommended default whenever the
  // catalogue (re)loads.
  useEffect(() => {
    setNewPerms(defaultPerms);
  }, [defaultPerms]);

  const addUser = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    if (!newEmail.includes("@")) {
      setError("דוא\"ל לא תקין");
      return;
    }
    try {
      const res = await fetch("/api/v1/admin/users", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: newEmail.trim().toLowerCase(),
          name: newName.trim() || newEmail.trim(),
          role: newRole,
          permissions: newRole === "admin" ? [] : newPerms,
        }),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || "שגיאה ביצירת משתמש");
      }
      setNewEmail("");
      setNewName("");
      setNewRole("content_manager");
      setNewPerms(defaultPerms);
      setShowAddForm(false);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
    }
  };

  const updateUser = async (id: string, patch: Partial<AdminUserRow>) => {
    setSavingId(id);
    setError(null);
    try {
      const res = await fetch(`/api/v1/admin/users/${id}`, {
        method: "PATCH",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || "שגיאה בעדכון");
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setSavingId(null);
    }
  };

  const deleteUser = async (id: string, email: string) => {
    if (!confirm(`למחוק את המשתמש ${email}?`)) return;
    setSavingId(id);
    try {
      const res = await fetch(`/api/v1/admin/users/${id}`, {
        method: "DELETE",
        credentials: "include",
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || "שגיאה במחיקה");
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "שגיאה");
    } finally {
      setSavingId(null);
    }
  };

  const togglePermission = (user: AdminUserRow, key: string) => {
    const next = user.permissions.includes(key)
      ? user.permissions.filter((p) => p !== key)
      : [...user.permissions, key];
    updateUser(user.id, { permissions: next });
  };

  const setRole = (user: AdminUserRow, role: "admin" | "content_manager") => {
    updateUser(user.id, { role });
  };

  return (
    <div>
      <div className="flex items-start justify-between mb-4">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">ניהול משתמשים</h1>
          <p className="text-sm text-gray-500 mt-1">
            הגדרת מי יכול להיכנס לפאנל הניהול ואיזה חלקים פתוחים לכל אחד.
            מנהלי מערכת רואים הכל; מנהלי תוכן רואים רק את ההרשאות המסומנות.
          </p>
        </div>
        <button
          onClick={() => setShowAddForm((v) => !v)}
          className="shrink-0 px-4 py-2 bg-primary-700 text-white text-sm font-medium rounded-lg hover:bg-primary-800 transition-colors"
        >
          {showAddForm ? "ביטול" : "+ הוסף משתמש"}
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 rounded-lg bg-red-50 text-red-700 text-sm">{error}</div>
      )}

      {/* Add user form */}
      {showAddForm && (
        <form onSubmit={addUser} className="mb-6 p-5 bg-white border border-gray-200 rounded-xl space-y-3">
          <h2 className="text-sm font-semibold text-gray-700">משתמש חדש</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-gray-500 mb-1">דוא"ל (Google)</label>
              <input
                type="email"
                value={newEmail}
                onChange={(e) => setNewEmail(e.target.value)}
                placeholder="user@example.com"
                dir="ltr"
                required
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
              />
              <p className="text-xs text-gray-400 mt-1">
                המשתמש יתחבר עם חשבון Google של אותה כתובת.
              </p>
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">שם להצגה</label>
              <input
                type="text"
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="(אופציונלי — יילקח מ-Google אם ריק)"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
              />
            </div>
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">תפקיד</label>
            <div className="flex gap-2">
              {(["content_manager", "admin"] as const).map((r) => (
                <button
                  key={r}
                  type="button"
                  onClick={() => setNewRole(r)}
                  className={`px-3 py-1.5 text-sm rounded-lg border transition-colors ${
                    newRole === r
                      ? "bg-primary-700 text-white border-primary-700"
                      : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
                  }`}
                >
                  {ROLE_LABEL[r]}
                </button>
              ))}
            </div>
          </div>
          {newRole === "content_manager" && (
            <div>
              <label className="block text-xs text-gray-500 mb-1">הרשאות</label>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                {permissionCatalog.map((p) => (
                  <label key={p.key} className="flex items-center gap-2 text-sm text-gray-700">
                    <input
                      type="checkbox"
                      checked={newPerms.includes(p.key)}
                      onChange={() =>
                        setNewPerms((cur) =>
                          cur.includes(p.key)
                            ? cur.filter((x) => x !== p.key)
                            : [...cur, p.key]
                        )
                      }
                      className="rounded border-gray-300"
                    />
                    {p.label}
                  </label>
                ))}
              </div>
            </div>
          )}
          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={() => setShowAddForm(false)}
              className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800"
            >
              ביטול
            </button>
            <button
              type="submit"
              className="px-4 py-1.5 text-sm rounded-lg bg-primary-700 text-white font-medium hover:bg-primary-800 transition-colors"
            >
              צור משתמש
            </button>
          </div>
        </form>
      )}

      {/* Users table */}
      {loading ? (
        <div className="text-center py-12 text-gray-400">טוען...</div>
      ) : users.length === 0 ? (
        <div className="text-center py-12 text-gray-400">אין משתמשים עדיין</div>
      ) : (
        <div className="space-y-3">
          {users.map((u) => {
            const isMe = me?.email === u.email;
            const isExpanded = editingId === u.id;
            return (
              <div key={u.id} className="bg-white border border-gray-200 rounded-lg p-4">
                <div className="flex items-start justify-between gap-3 mb-1">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="font-semibold text-gray-900">{u.name || u.email}</span>
                      <span className={`text-xs px-2 py-0.5 rounded-full border ${ROLE_BADGE[u.role]}`}>
                        {ROLE_LABEL[u.role] || u.role}
                      </span>
                      {isMe && (
                        <span className="text-xs text-gray-400">(אתה)</span>
                      )}
                    </div>
                    <div className="text-sm text-gray-500 truncate mt-0.5" dir="ltr">{u.email}</div>
                    <div className="text-xs text-gray-400 mt-1">
                      התחברות אחרונה: {formatDate(u.last_login_at)} · נוצר: {formatDate(u.created_at)}
                    </div>
                  </div>
                  <div className="shrink-0 flex items-center gap-2">
                    <button
                      onClick={() => setEditingId(isExpanded ? null : u.id)}
                      className="text-xs text-primary-600 hover:underline"
                    >
                      {isExpanded ? "סגור" : "ערוך הרשאות"}
                    </button>
                    {!isMe && (
                      <button
                        onClick={() => deleteUser(u.id, u.email)}
                        disabled={savingId === u.id}
                        className="text-xs text-red-500 hover:text-red-700 disabled:opacity-50"
                      >
                        מחק
                      </button>
                    )}
                  </div>
                </div>

                {/* Permissions chips (preview when collapsed, editable when expanded) */}
                {u.role === "admin" ? (
                  <div className="mt-2 text-xs text-gray-500">
                    גישה מלאה לכל החלקים, כולל ניהול משתמשים.
                  </div>
                ) : (
                  <div className="mt-3">
                    {!isExpanded ? (
                      <div className="flex flex-wrap gap-1.5">
                        {u.permissions.length === 0 ? (
                          <span className="text-xs text-gray-400">אין הרשאות פעילות</span>
                        ) : (
                          permissionCatalog
                            .filter((p) => u.permissions.includes(p.key))
                            .map((p) => (
                              <span key={p.key} className="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-700 border border-gray-200">
                                {p.label}
                              </span>
                            ))
                        )}
                      </div>
                    ) : (
                      <div className="mt-2 p-3 bg-gray-50 rounded-lg border border-gray-200">
                        <div className="text-xs text-gray-500 mb-2">הרשאות:</div>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-2 mb-3">
                          {permissionCatalog.map((p) => (
                            <label key={p.key} className="flex items-center gap-2 text-sm text-gray-700">
                              <input
                                type="checkbox"
                                checked={u.permissions.includes(p.key)}
                                onChange={() => togglePermission(u, p.key)}
                                disabled={savingId === u.id}
                                className="rounded border-gray-300"
                              />
                              {p.label}
                            </label>
                          ))}
                        </div>
                        {!isMe && (
                          <div className="flex items-center gap-2 pt-2 border-t border-gray-200">
                            <span className="text-xs text-gray-500">קדם לתפקיד:</span>
                            <button
                              onClick={() => setRole(u, "admin")}
                              disabled={savingId === u.id}
                              className="text-xs px-3 py-1 rounded-full bg-amber-50 text-amber-700 border border-amber-200 hover:bg-amber-100 disabled:opacity-50"
                            >
                              הפוך למנהל מערכת
                            </button>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}

                {/* Demote admin → content_manager */}
                {u.role === "admin" && isExpanded && !isMe && (
                  <div className="mt-3 p-3 bg-gray-50 rounded-lg border border-gray-200 flex items-center justify-between">
                    <span className="text-xs text-gray-500">להוריד למנהל/ת תוכן עם הרשאות מותאמות?</span>
                    <button
                      onClick={() => setRole(u, "content_manager")}
                      disabled={savingId === u.id}
                      className="text-xs px-3 py-1 rounded-full bg-white text-gray-700 border border-gray-300 hover:bg-gray-50 disabled:opacity-50"
                    >
                      הפוך למנהל/ת תוכן
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
