"use client";

import { useOriginFilter } from "@/lib/originFilter";

/**
 * Subtle inline toggle for showing MK constituent-outreach expense
 * relationships. Deliberately understated — small text, muted colours —
 * because most visitors don't need it and we don't want it competing
 * with the search bar / showcase headline.
 *
 * Render in the existing layout flow; the component is its own
 * <label> + checkbox unit with no surrounding card.
 */
export function MkExpenseToggle({ className = "" }: { className?: string }) {
  const { showMkExpenses, setShowMkExpenses } = useOriginFilter();
  return (
    <label
      className={`inline-flex items-center gap-2 text-xs text-gray-500 hover:text-gray-700 cursor-pointer select-none ${className}`}
    >
      <input
        type="checkbox"
        checked={showMkExpenses}
        onChange={(e) => setShowMkExpenses(e.target.checked)}
        className="h-3.5 w-3.5 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
      />
      <span>הצג גם הוצאות קשר עם הציבור</span>
    </label>
  );
}
