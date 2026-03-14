"use client";

import { useEffect, useState } from "react";
import { getMe, logout, type AdminUser } from "@/lib/auth";

const NAV_ITEMS = [
  { href: "/admin", label: "לוח בקרה" },
  { href: "/admin/entities", label: "ישויות" },
  { href: "/admin/relationships", label: "קשרים" },
  { href: "/admin/documents", label: "מסמכים" },
  { href: "/admin/import", label: "ייבוא" },
  { href: "/admin/registry", label: "מרשמים" },
  { href: "/admin/settings", label: "הגדרות" },
];

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
          {NAV_ITEMS.map((item) => (
            <a
              key={item.href}
              href={item.href}
              className="block px-3 py-2 rounded-lg text-sm text-primary-100 hover:bg-primary-800 transition-colors"
            >
              {item.label}
            </a>
          ))}
        </nav>
        <div className="p-3 border-t border-primary-800">
          <div className="text-xs text-primary-200 truncate mb-2">{user?.email}</div>
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
