// Shared display labels and styling tokens for relationship-graph UIs.
// ConnectionMap (cytoscape) and ConnectionTable both consume these so the
// definitions stay in one place.

export const EDGE_LABELS: Record<string, string> = {
  restricted_from: "מוגבל מ-",
  owns: "בעלות",
  manages: "מנהל",
  employed_by: "מועסק ב-",
  related_to: "קשור ל-",
  board_member: "חבר דירקטוריון",
  operates_in: "פועל בתחום",
  family_member: "בן משפחה",
  // MK constituent-outreach expense payments
  mk_expense_payment: "תשלום מתקציב הציבור",
};

export const ORIGIN_LABELS_HE: Record<string, string> = {
  coi_declaration: "הסדר למניעת ניגוד עניינים",
  mk_expense: "הוצאות קשר עם הציבור",
};

// Tailwind utility classes used to render small badges next to edges /
// rows. Keep one entry per origin_kind.
export const ORIGIN_BADGE_CLASSES: Record<string, string> = {
  coi_declaration: "bg-primary-50 text-primary-700 border-primary-200",
  mk_expense: "bg-emerald-50 text-emerald-700 border-emerald-200",
};

export function originLabel(origin_kind?: string | null): string {
  if (!origin_kind) return ORIGIN_LABELS_HE.coi_declaration;
  return ORIGIN_LABELS_HE[origin_kind] || origin_kind;
}

export function originBadgeClass(origin_kind?: string | null): string {
  if (!origin_kind) return ORIGIN_BADGE_CLASSES.coi_declaration;
  return ORIGIN_BADGE_CLASSES[origin_kind] || "bg-gray-50 text-gray-700 border-gray-200";
}
