"use client";

/**
 * MCP onboarding card — the family-canonical block.
 *
 * This file is the reference implementation for Ocal and Ckan-versions.
 * Class names match `DESIGN_SYSTEM.md → API + MCP Card (canonical)`
 * EXACTLY; don't drift without coordinating with the other repos.
 *
 * Props let each sibling reuse the layout while pointing at its own
 * mailto, MCP URL, and product name — the visual structure stays
 * identical so the family looks like one product.
 */

import { useState } from "react";

interface McpCardProps {
  /** Public MCP endpoint, e.g. "https://www.ocoi.org.il/mcp" */
  serverUrl: string;
  /** Mailto for invite requests, e.g. "guy@z-g.co.il" */
  contactEmail: string;
  /**
   * Hebrew product name, used in the email subject + a few labels.
   * Example: "ניגוד עניינים לעם".
   */
  productName: string;
  /**
   * Hebrew tagline shown under the heading. Override to describe
   * what the data is in the sibling project (calendar entries,
   * dataset versions, …).
   */
  tagline?: string;
}

export function McpCard({
  serverUrl,
  contactEmail,
  productName,
  tagline = "גישה מובנית לדאטה דרך Model Context Protocol — ה-LLM יכול לחפש, לטייל בגרף הקשרים ולצטט מסמכי מקור בלי לעבור דרך ה-API למעלה.",
}: McpCardProps) {
  const [copied, setCopied] = useState(false);

  const mailto = `mailto:${contactEmail}?subject=${encodeURIComponent(
    `בקשת גישה ל-MCP של ${productName}`,
  )}`;

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(serverUrl);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // No-op — clipboard API can be blocked. The URL is still visible.
    }
  };

  return (
    // ── Container (canonical) ──
    <div className="bg-amber-50 border border-amber-200 rounded-lg p-6 mb-6">
      {/* Heading row */}
      <div className="flex items-start justify-between gap-3 flex-wrap mb-4">
        <div>
          <h2 className="text-xl font-bold text-amber-900 mb-1">
            MCP — חיבור ישיר ל-Claude / Cursor / סוכני AI
          </h2>
          <p className="text-sm text-amber-800">{tagline}</p>
        </div>
        {/* ── Beta badge (canonical) ── */}
        <span className="shrink-0 bg-yellow-100 text-yellow-800 rounded-full px-2 py-0.5 text-xs font-medium">
          ביתא סגורה
        </span>
      </div>

      {/* ── Sub-card: how to request access ── */}
      <div className="bg-white border border-amber-100 rounded-lg p-4 mb-4">
        <h3 className="text-sm font-semibold text-gray-900 mb-1">
          איך מקבלים גישה?
        </h3>
        <p className="text-sm text-gray-700">
          שלחו אימייל ל-{" "}
          <a
            href={mailto}
            className="font-mono text-primary-700 underline decoration-primary-300 underline-offset-2 hover:decoration-primary-700"
          >
            {contactEmail}
          </a>{" "}
          עם כתובת ה-Google שאתם רוצים לחבר ושורה־שתיים על מטרת השימוש
          (מחקר, עיתונאות, כלי משלכם וכו׳). ההזמנה ניתנת ידנית, בדרך כלל
          באותו יום.
        </p>
      </div>

      {/* ── Sub-card: how to connect after approval ── */}
      <div className="bg-white border border-amber-100 rounded-lg p-4">
        <h3 className="text-sm font-semibold text-gray-900 mb-2">
          אחרי שאישרנו את הכתובת — איך מתחברים מ-Claude
        </h3>
        <ol className="text-sm text-gray-700 space-y-2 list-decimal pr-5 marker:text-amber-700 marker:font-semibold">
          <li>
            ב-Claude (אפליקציית הדסקטופ או claude.ai בדפדפן) פתחו{" "}
            <strong>Settings</strong> → <strong>Connectors</strong>.
          </li>
          <li>
            לחצו על הכפתור <strong>+ Add custom connector</strong> (או
            &quot;Personal plugins&quot; → <strong>+</strong>).
          </li>
          <li>
            בשדה <em>Name</em> כתבו שם שתזהו (למשל &quot;{productName}&quot;).
            <br />
            בשדה <em>URL</em> או <em>Server URL</em> הדביקו את הכתובת:
            <div className="mt-2 flex items-center gap-2 flex-wrap">
              {/* ── URL button (canonical) ── */}
              <code
                className="bg-stone-800 text-white font-mono rounded-md px-4 py-2 text-sm"
                dir="ltr"
              >
                {serverUrl}
              </code>
              {/* ── Copy button (canonical) ── */}
              <button
                type="button"
                onClick={copy}
                className="bg-white text-stone-800 border border-stone-300 rounded-md px-3 py-2 text-sm hover:bg-stone-50"
              >
                {copied ? "הועתק ✓" : "העתק"}
              </button>
            </div>
          </li>
          <li>
            לחצו <strong>Connect</strong>. ייפתח חלון Google — התחברו{" "}
            <strong>עם אותה כתובת מייל</strong> שעליה ביקשתם הזמנה.
          </li>
          <li>
            Google יחזיר אתכם אוטומטית ל-Claude. ה-connector יסומן
            כ-Connected, ובסרגל הכלים של השיחה יופיעו פעולות חדשות
            (חיפוש, מפת קשרים, מסמכי מקור).
          </li>
          <li>
            פתחו שיחה חדשה ושאלו שאלה כמו{" "}
            <em>&quot;חפש ב-{productName} … והצג את הקשרים&quot;</em> —
            Claude יקרא אוטומטית לכלים, יצטט את מסמכי המקור, ויקשר חזרה
            לאתר.
          </li>
        </ol>
        <p className="text-xs text-gray-500 mt-3">
          אם החיבור נכשל עם &quot;Couldn&apos;t reach the MCP server&quot;:
          ודאו ש-URL מתחיל ב-
          <code className="bg-stone-100 px-1 rounded" dir="ltr">
            https://www.
          </code>{" "}
          (עם www) ולא ב-apex בלבד.
        </p>
      </div>
    </div>
  );
}
