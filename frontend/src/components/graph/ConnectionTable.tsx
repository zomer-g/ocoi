"use client";

import type { ConnectionEdge } from "@/lib/api-client";

const EDGE_LABELS: Record<string, string> = {
  restricted_from: "מוגבל מ-",
  owns: "בעלות",
  manages: "מנהל",
  employed_by: "מועסק ב-",
  related_to: "קשור ל-",
  board_member: "חבר דירקטוריון",
  operates_in: "פועל בתחום",
  family_member: "בן משפחה",
};

const TYPE_LABELS: Record<string, string> = {
  person: "אדם",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

interface ConnectionTableProps {
  edges: ConnectionEdge[];
  caption?: string;
  className?: string;
}

export function ConnectionTable({ edges, caption, className }: ConnectionTableProps) {
  if (!edges.length) return null;

  return (
    <section aria-label="טבלת קשרים" className={className}>
      <div className="overflow-x-auto rounded-lg border border-gray-200">
        <table className="w-full text-sm" dir="rtl">
          {caption && (
            <caption className="sr-only">{caption}</caption>
          )}
          <thead>
            <tr className="bg-primary-50 text-primary-800">
              <th scope="col" className="px-4 py-3 text-start font-semibold">מקור</th>
              <th scope="col" className="px-4 py-3 text-start font-semibold">סוג מקור</th>
              <th scope="col" className="px-4 py-3 text-start font-semibold">יעד</th>
              <th scope="col" className="px-4 py-3 text-start font-semibold">סוג יעד</th>
              <th scope="col" className="px-4 py-3 text-start font-semibold">סוג קשר</th>
              <th scope="col" className="px-4 py-3 text-start font-semibold">מגבלה</th>
            </tr>
          </thead>
          <tbody>
            {edges.map((edge, i) => (
              <tr key={`${edge.source_id}-${edge.target_id}-${edge.relationship_type}-${i}`}
                  className={i % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                <td className="px-4 py-3">{edge.source_name}</td>
                <td className="px-4 py-3 text-gray-500">{TYPE_LABELS[edge.source_type] || edge.source_type}</td>
                <td className="px-4 py-3">{edge.target_name}</td>
                <td className="px-4 py-3 text-gray-500">{TYPE_LABELS[edge.target_type] || edge.target_type}</td>
                <td className="px-4 py-3">{EDGE_LABELS[edge.relationship_type] || edge.relationship_type}</td>
                <td className="px-4 py-3">
                  {edge.relationship_type === "restricted_from" ? (
                    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
                      כן
                    </span>
                  ) : (
                    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600">
                      לא
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
