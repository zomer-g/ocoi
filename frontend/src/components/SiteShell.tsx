"use client";

import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { OriginFilterProvider } from "@/lib/originFilter";

interface NavLink {
  href: string;
  label: string;
}

const DEFAULT_NAV: NavLink[] = [
  { href: "/", label: "חיפוש" },
  { href: "/graph", label: "מפת קשרים" },
  { href: "/documents", label: "מסמכים" },
  { href: "/api-docs", label: "API ציבורי" },
  { href: "/about", label: "אודות" },
];

const DEFAULT_FOOTER = "ניגוד עניינים לעם — שקיפות ניגודי עניינים של בעלי תפקידים ציבוריים בישראל";

export default function SiteShell({ children }: { children: React.ReactNode }) {
  const [navLinks, setNavLinks] = useState<NavLink[]>(DEFAULT_NAV);
  const [footerText, setFooterText] = useState(DEFAULT_FOOTER);
  // Active-link state is family-canonical (see DESIGN_SYSTEM.md →
  // "Home Hero / Navigation"). usePathname is null on the very first
  // server render; we treat null as "no link active" so SSR + hydrate
  // produce identical markup.
  const pathname = usePathname();
  // Mobile drawer state. Closed on every route change so clicking a
  // link in the open drawer doesn't leave it stuck open after navigation.
  const [mobileOpen, setMobileOpen] = useState(false);
  useEffect(() => {
    setMobileOpen(false);
  }, [pathname]);

  useEffect(() => {
    fetch("/api/v1/site/content/header_links")
      .then((r) => r.json())
      .then((d) => {
        const val = d?.data?.value;
        if (val) {
          try {
            const parsed = JSON.parse(val);
            if (Array.isArray(parsed) && parsed.length > 0) {
              // Merge while preserving the canonical order in DEFAULT_NAV:
              // walk DEFAULT_NAV first and replace each entry's label with
              // the CMS label (if any). Then append any CMS-only entries
              // (admin extras) at the end. This keeps "מסמכים" pinned in
              // its expected slot between "מפת קשרים" and "API ציבורי"
              // even when the CMS list still uses the old order.
              const cmsByHref = new Map<string, NavLink>();
              for (const l of parsed as NavLink[]) {
                if (l && l.href) cmsByHref.set(l.href, l);
              }
              const defaultHrefs = new Set(DEFAULT_NAV.map((l) => l.href));
              const merged: NavLink[] = [
                ...DEFAULT_NAV.map((l) => cmsByHref.get(l.href) ?? l),
                ...(parsed as NavLink[]).filter(
                  (l) => l && l.href && !defaultHrefs.has(l.href),
                ),
              ];
              setNavLinks(merged);
            }
          } catch { /* use default */ }
        }
      })
      .catch(() => {});

    fetch("/api/v1/site/content/footer_text")
      .then((r) => r.json())
      .then((d) => {
        const val = d?.data?.value;
        if (val) setFooterText(val);
      })
      .catch(() => {});
  }, []);

  // Active-state helper — shared by desktop nav + mobile drawer so both
  // surfaces stay visually in sync.
  const isActiveLink = (href: string): boolean => {
    if (pathname == null) return false;
    return href === "/"
      ? pathname === "/"
      : pathname === href || pathname.startsWith(href + "/");
  };

  return (
    <OriginFilterProvider>
      <header className="bg-primary-800 sticky top-0 z-50">
        <nav className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="h-14 sm:h-16 flex items-center justify-between gap-2">
            <a href="/" className="flex items-center gap-2 text-white min-w-0">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="w-7 h-7 sm:w-8 sm:h-8 shrink-0"
                viewBox="0 0 24 24" fill="none" stroke="currentColor"
                strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
              >
                <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
              </svg>
              <span className="text-base sm:text-xl font-bold truncate">
                ניגוד עניינים לעם
              </span>
            </a>

            {/* Desktop nav — visible at sm and up */}
            <div className="hidden sm:flex gap-1">
              {navLinks.map((item) => {
                const isActive = isActiveLink(item.href);
                return (
                  <a
                    key={item.href}
                    href={item.href}
                    aria-current={isActive ? "page" : undefined}
                    className={`px-3 sm:px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                      isActive
                        ? "bg-white/15 text-white"
                        : "text-primary-100 hover:bg-white/10 hover:text-white"
                    }`}
                  >
                    {item.label}
                  </a>
                );
              })}
            </div>

            {/* Mobile hamburger — sub-sm only */}
            <button
              type="button"
              onClick={() => setMobileOpen((v) => !v)}
              aria-expanded={mobileOpen}
              aria-controls="mobile-nav"
              aria-label={mobileOpen ? "סגור תפריט ניווט" : "פתח תפריט ניווט"}
              className="sm:hidden p-2 rounded-lg text-primary-100 hover:bg-white/10 hover:text-white shrink-0"
            >
              {mobileOpen ? (
                <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M18 6L6 18M6 6l12 12" />
                </svg>
              ) : (
                <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M3 6h18M3 12h18M3 18h18" />
                </svg>
              )}
            </button>
          </div>

          {/* Mobile dropdown — full-width panel below the bar.
              `-mx-*` matches the header's responsive horizontal padding
              so the panel reaches the screen edges; inner `px-4` keeps
              the link rows aligned with the brand. */}
          {mobileOpen && (
            <div
              id="mobile-nav"
              className="sm:hidden border-t border-primary-600 -mx-4 sm:-mx-6 lg:-mx-8 bg-primary-700"
            >
              <div className="px-4 py-2 space-y-1">
                {navLinks.map((item) => {
                  const isActive = isActiveLink(item.href);
                  return (
                    <a
                      key={item.href}
                      href={item.href}
                      onClick={() => setMobileOpen(false)}
                      aria-current={isActive ? "page" : undefined}
                      className={`block px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                        isActive
                          ? "bg-white/15 text-white"
                          : "text-primary-100 hover:bg-white/10 hover:text-white"
                      }`}
                    >
                      {item.label}
                    </a>
                  );
                })}
              </div>
            </div>
          )}
        </nav>
      </header>

      <main id="main-content" className="flex-1">
        {children}
      </main>

      <footer className="bg-primary-900 text-primary-100 py-6 text-center text-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          {footerText}
        </div>
      </footer>
    </OriginFilterProvider>
  );
}
