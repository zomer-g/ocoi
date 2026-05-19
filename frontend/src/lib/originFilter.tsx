"use client";

/**
 * Global toggle for whether MK constituent-outreach payment edges
 * (`origin_kind = "mk_expense"`) are visible across the public site.
 *
 * Default: hidden. The Knesset constituent-outreach data is much denser
 * than COI declarations and visually drowns the more interesting edges
 * for casual visitors. Power users (researchers, journalists) can opt in
 * via the subtle toggle on the home page or the search results section.
 *
 * Persistence: localStorage so the choice survives page navigation +
 * full reloads. SSR-safe — reads default during render, hydrates from
 * storage in a layout effect.
 */

import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";

const STORAGE_KEY = "ocoi:show_mk_expenses";

interface OriginFilterContextValue {
  /** When true, MK-expense edges are shown alongside COI declarations. */
  showMkExpenses: boolean;
  /** Setter — persists to localStorage as a side effect. */
  setShowMkExpenses: (next: boolean) => void;
  /**
   * The query-string value to append to /api/v1/graph/* calls.
   * Empty string when MK expenses are shown; otherwise a
   * `&exclude_origins=mk_expense` snippet ready to splice into a URL.
   */
  excludeOriginsParam: string;
}

const OriginFilterContext = createContext<OriginFilterContextValue | null>(null);

export function OriginFilterProvider({ children }: { children: ReactNode }) {
  // SSR default: hidden. The hydration effect below corrects this to
  // whatever the user previously chose, without an extra render flash for
  // first-time visitors (whose stored value matches the default anyway).
  const [showMkExpenses, setShowMkExpensesState] = useState(false);

  useEffect(() => {
    try {
      const v = window.localStorage.getItem(STORAGE_KEY);
      if (v === "1") setShowMkExpensesState(true);
    } catch {
      // localStorage can throw in incognito on some browsers; default stays.
    }
  }, []);

  const setShowMkExpenses = (next: boolean) => {
    setShowMkExpensesState(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, next ? "1" : "0");
    } catch {
      // ignore — UI state is still correct for the session.
    }
  };

  const excludeOriginsParam = showMkExpenses ? "" : "&exclude_origins=mk_expense";

  return (
    <OriginFilterContext.Provider
      value={{ showMkExpenses, setShowMkExpenses, excludeOriginsParam }}
    >
      {children}
    </OriginFilterContext.Provider>
  );
}

export function useOriginFilter(): OriginFilterContextValue {
  const ctx = useContext(OriginFilterContext);
  if (!ctx) {
    // Safe fallback: defaults to hidden, no persistence. Lets components
    // outside the provider (e.g. error pages) render without crashing.
    return {
      showMkExpenses: false,
      setShowMkExpenses: () => {},
      excludeOriginsParam: "&exclude_origins=mk_expense",
    };
  }
  return ctx;
}
