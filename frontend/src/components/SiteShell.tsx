"use client";

import { useEffect, useState } from "react";

interface NavLink {
  href: string;
  label: string;
}

const DEFAULT_NAV: NavLink[] = [
  { href: "/", label: "חיפוש" },
  { href: "/graph", label: "מפת קשרים" },
  { href: "/api-docs", label: "API ציבורי" },
  { href: "/about", label: "אודות" },
];

const DEFAULT_FOOTER = "ניגוד עניינים לעם — שקיפות ניגודי עניינים של בעלי תפקידים ציבוריים בישראל";

export default function SiteShell({ children }: { children: React.ReactNode }) {
  const [navLinks, setNavLinks] = useState<NavLink[]>(DEFAULT_NAV);
  const [footerText, setFooterText] = useState(DEFAULT_FOOTER);

  useEffect(() => {
    fetch("/api/v1/site/content/header_links")
      .then((r) => r.json())
      .then((d) => {
        const val = d?.data?.value;
        if (val) {
          try {
            const parsed = JSON.parse(val);
            if (Array.isArray(parsed) && parsed.length > 0) setNavLinks(parsed);
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

  return (
    <>
      <header className="bg-primary-800 sticky top-0 z-50">
        <nav className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-14 sm:h-16 flex items-center justify-between">
          <a href="/" className="flex items-center gap-2 text-white">
            <svg xmlns="http://www.w3.org/2000/svg" className="w-7 h-7 sm:w-8 sm:h-8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
            </svg>
            <span className="text-lg sm:text-xl font-bold">ניגוד עניינים לעם</span>
          </a>
          <div className="flex gap-1">
            {navLinks.map((item) => (
              <a
                key={item.href}
                href={item.href}
                className="px-3 sm:px-4 py-2 rounded-lg text-sm font-medium text-primary-100 hover:bg-white/10 hover:text-white transition-colors"
              >
                {item.label}
              </a>
            ))}
          </div>
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
    </>
  );
}
