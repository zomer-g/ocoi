"use client";

import { useEffect, useState } from "react";
import { getSiteContent, updateSiteContent } from "@/lib/admin-api";

interface NavLink {
  href: string;
  label: string;
}

const DEFAULT_LINKS: NavLink[] = [
  { href: "/", label: "חיפוש" },
  { href: "/graph", label: "מפת קשרים" },
  { href: "/api-docs", label: "API ציבורי" },
  { href: "/about", label: "אודות" },
];

export default function SiteContentPage() {
  const [headerLinks, setHeaderLinks] = useState<NavLink[]>(DEFAULT_LINKS);
  const [footerText, setFooterText] = useState("");
  const [aboutContent, setAboutContent] = useState("");
  const [saving, setSaving] = useState<string | null>(null);
  const [message, setMessage] = useState<{ type: "ok" | "err"; text: string } | null>(null);

  useEffect(() => {
    getSiteContent("header_links").then((r) => {
      if (r.data.value) {
        try {
          const parsed = JSON.parse(r.data.value);
          if (Array.isArray(parsed) && parsed.length > 0) setHeaderLinks(parsed);
        } catch { /* keep defaults */ }
      }
    }).catch(() => {});

    getSiteContent("footer_text").then((r) => {
      if (r.data.value) setFooterText(r.data.value);
    }).catch(() => {});

    getSiteContent("about_content").then((r) => {
      if (r.data.value) setAboutContent(r.data.value);
    }).catch(() => {});
  }, []);

  const flash = (type: "ok" | "err", text: string) => {
    setMessage({ type, text });
    setTimeout(() => setMessage(null), 3000);
  };

  const save = async (key: string, value: string) => {
    setSaving(key);
    try {
      await updateSiteContent(key, value);
      flash("ok", "נשמר בהצלחה");
    } catch (e) {
      flash("err", `שגיאה: ${e}`);
    } finally {
      setSaving(null);
    }
  };

  const updateLink = (index: number, field: keyof NavLink, value: string) => {
    setHeaderLinks((prev) => prev.map((l, i) => (i === index ? { ...l, [field]: value } : l)));
  };

  const addLink = () => {
    setHeaderLinks((prev) => [...prev, { href: "/", label: "" }]);
  };

  const removeLink = (index: number) => {
    setHeaderLinks((prev) => prev.filter((_, i) => i !== index));
  };

  return (
    <div className="space-y-8">
      <h1 className="text-2xl font-bold text-gray-900">תוכן האתר</h1>

      {message && (
        <div className={`p-3 rounded-lg text-sm ${message.type === "ok" ? "bg-green-50 text-green-700" : "bg-red-50 text-red-700"}`}>
          {message.text}
        </div>
      )}

      {/* Header Links */}
      <section className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">קישורי כותרת</h2>
        <p className="text-sm text-gray-500 mb-4">קישורי הניווט שמופיעים בכותרת העליונה של האתר</p>
        <div className="space-y-3">
          {headerLinks.map((link, i) => (
            <div key={i} className="flex items-center gap-3">
              <input
                type="text"
                value={link.label}
                onChange={(e) => updateLink(i, "label", e.target.value)}
                placeholder="טקסט"
                className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm"
              />
              <input
                type="text"
                value={link.href}
                onChange={(e) => updateLink(i, "href", e.target.value)}
                placeholder="כתובת"
                dir="ltr"
                className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm font-mono"
              />
              <button
                onClick={() => removeLink(i)}
                className="text-red-500 hover:text-red-700 px-2 py-1 text-sm"
              >
                מחק
              </button>
            </div>
          ))}
        </div>
        <div className="flex items-center gap-3 mt-4">
          <button
            onClick={addLink}
            className="text-sm text-primary-700 hover:text-primary-900 font-medium"
          >
            + הוסף קישור
          </button>
          <div className="flex-1" />
          <button
            onClick={() => save("header_links", JSON.stringify(headerLinks))}
            disabled={saving === "header_links"}
            className="px-4 py-2 bg-primary-700 text-white rounded-lg text-sm hover:bg-primary-800 disabled:opacity-50"
          >
            {saving === "header_links" ? "שומר..." : "שמור כותרת"}
          </button>
        </div>
      </section>

      {/* Footer Text */}
      <section className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">כותרת תחתונה</h2>
        <p className="text-sm text-gray-500 mb-4">הטקסט שמופיע בתחתית כל עמוד באתר</p>
        <input
          type="text"
          value={footerText}
          onChange={(e) => setFooterText(e.target.value)}
          placeholder="ניגוד עניינים לעם — שקיפות ניגודי עניינים של בעלי תפקידים ציבוריים בישראל"
          className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm"
        />
        <div className="flex justify-end mt-4">
          <button
            onClick={() => save("footer_text", footerText)}
            disabled={saving === "footer_text"}
            className="px-4 py-2 bg-primary-700 text-white rounded-lg text-sm hover:bg-primary-800 disabled:opacity-50"
          >
            {saving === "footer_text" ? "שומר..." : "שמור כותרת תחתונה"}
          </button>
        </div>
      </section>

      {/* About Page Content */}
      <section className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">עמוד אודות</h2>
        <p className="text-sm text-gray-500 mb-4">תוכן עמוד האודות — ניתן לכתוב HTML</p>
        <textarea
          value={aboutContent}
          onChange={(e) => setAboutContent(e.target.value)}
          rows={15}
          placeholder="<h2>אודות הפרויקט</h2><p>תוכן העמוד...</p>"
          className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm font-mono"
          dir="rtl"
        />
        <div className="flex items-center justify-between mt-4">
          {aboutContent && (
            <details className="text-sm">
              <summary className="text-primary-700 cursor-pointer">תצוגה מקדימה</summary>
              <div
                className="mt-2 p-4 border border-gray-200 rounded-lg prose prose-sm max-w-none"
                dir="rtl"
                dangerouslySetInnerHTML={{ __html: aboutContent }}
              />
            </details>
          )}
          <button
            onClick={() => save("about_content", aboutContent)}
            disabled={saving === "about_content"}
            className="px-4 py-2 bg-primary-700 text-white rounded-lg text-sm hover:bg-primary-800 disabled:opacity-50"
          >
            {saving === "about_content" ? "שומר..." : "שמור עמוד אודות"}
          </button>
        </div>
      </section>
    </div>
  );
}
