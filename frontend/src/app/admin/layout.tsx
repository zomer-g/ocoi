"use client";

import { useEffect, useState } from "react";
import { getMe, hasPermission, isAdmin, logout, type AdminUser } from "@/lib/auth";

// Each nav item lists the permission key (or `adminOnly: true`) that the
// backend section requires. Matches `SECTION_PERMISSIONS` in
// `packages/ocoi-common/src/ocoi_common/permissions.py` so the UI never
// offers a link that the API would reject.
type NavItem = {
  href: string;
  label: string;
  perm?: string;
  adminOnly?: boolean;
  external?: boolean;
};

const NAV_ITEMS: NavItem[] = [
  { href: "/admin", label: "לוח בקרה", perm: "view_dashboard" },
  { href: "/admin/entities", label: "ישויות", perm: "manage_entities" },
  { href: "/admin/relationships", label: "קשרים", perm: "manage_relationships" },
  { href: "/admin/documents", label: "מסמכים", perm: "manage_documents" },
  { href: "/admin/import", label: "ייבוא", perm: "manage_import" },
  { href: "/admin/registry", label: "מרשמים", perm: "manage_registry" },
  { href: "/admin/suggestions", label: "הצעות תיקון", perm: "manage_suggestions" },
  { href: "/admin/site-content", label: "תוכן האתר", perm: "manage_site_content" },
  { href: "/api/admin-docs", label: "API", external: true, adminOnly: true },
  { href: "/admin/users", label: "משתמשים", adminOnly: true },
  { href: "/admin/settings", label: "הגדרות", adminOnly: true },
];

function visibleNavFor(user: AdminUser | null): NavItem[] {
  if (!user) return [];
  return NAV_ITEMS.filter((item) => {
    if (item.adminOnly) return isAdmin(user);
    if (item.perm) return hasPermission(user, item.perm);
    return true;
  });
}

export default function AdminLayout({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AdminUser | null>(null);
  const [loading, setLoading] = useState(true);
  const [isLoginPage, setIsLoginPage] = useState(false);

  useEffect(() => {
    // Skip auth guard on the login page itself to avoid redirect loop
    if (window.location.pathname.startsWith("/admin/login")) {
      setIsLoginPage(true);
      setLoading(false);
      return;
    }
    getMe().then((u) => {
      if (!u) {
        window.location.href = "/admin/login";
      } else {
        setUser(u);
        setLoading(false);
      }
    });
  }, []);

  if (loading) {
    return <div className="min-h-screen flex items-center justify-center text-gray-400">טוען...</div>;
  }

  // Render login page without the sidebar layout
  if (isLoginPage) {
    return <>{children}</>;
  }

  return (
    <div className="flex h-[calc(100vh-56px)] sm:h-[calc(100vh-64px)]">
      {/* Sidebar */}
      <aside className="w-56 bg-primary-900 text-white flex flex-col shrink-0">
        <div className="p-4 border-b border-primary-800">
          <div className="text-sm font-bold">פאנל ניהול</div>
        </div>
        <nav className="flex-1 p-2 space-y-1">
          {visibleNavFor(user).map((item) => (
            <a
              key={item.href}
              href={item.href}
              {...(item.external ? { target: "_blank", rel: "noopener" } : {})}
              className="block px-3 py-2 rounded-lg text-sm text-primary-100 hover:bg-primary-800 transition-colors"
            >
              {item.label}
              {item.external && <span className="text-xs mr-1">↗</span>}
            </a>
          ))}
        </nav>
        <div className="p-3 border-t border-primary-800">
          <div className="text-xs text-primary-200 truncate">{user?.email}</div>
          {user?.role && (
            <div className="text-[10px] text-primary-300 mb-2">
              {user.role === "admin" ? "מנהל מערכת" : "מנהל/ת תוכן"}
            </div>
          )}
          <button
            onClick={logout}
            className="w-full text-xs text-primary-200 hover:text-white px-2 py-1 rounded hover:bg-primary-800 transition-colors text-start"
          >
            יציאה
          </button>
        </div>
      </aside>

      {/* Main content */}
      <div className="flex-1 overflow-auto bg-[#F6F6F6] p-6">
        {children}
      </div>
    </div>
  );
}
