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
              <th scope="col" className="px-4 py-3 text-start font-semibold">מסמך מקור</th>
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
                <td className="px-4 py-3">
                  {edge.document_url && !edge.document_url.startsWith("upload://") ? (
                    <a
                      href={edge.document_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 text-primary-700 hover:text-primary-900 text-xs font-medium"
                      title={edge.document_title || "מסמך מקור"}
                    >
                      <svg xmlns="http://www.w3.org/2000/svg" className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                        <polyline points="14 2 14 8 20 8"/>
                      </svg>
                      PDF
                    </a>
                  ) : (
                    <span className="text-gray-300 text-xs">—</span>
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
