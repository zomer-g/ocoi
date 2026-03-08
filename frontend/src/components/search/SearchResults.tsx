"use client";

import type { EntitySummary } from "@/lib/api-client";

const TYPE_LABELS: Record<string, string> = {
  person: "איש/אשת ציבור",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

const TYPE_COLORS: Record<string, string> = {
  person: "bg-blue-100 text-blue-800",
  company: "bg-green-100 text-green-800",
  association: "bg-purple-100 text-purple-800",
  domain: "bg-orange-100 text-orange-800",
};

interface SearchResultsProps {
  results: EntitySummary[];
}

export function SearchResults({ results }: SearchResultsProps) {
  if (results.length === 0) {
    return <div className="text-center py-8 text-gray-400">לא נמצאו תוצאות</div>;
  }

  return (
    <div className="space-y-3">
      {results.map((entity) => (
        <a
          key={entity.id}
          href={`/entity?id=${entity.id}&type=${entity.entity_type}`}
          className="block bg-white rounded-lg p-4 border border-gray-200 hover:border-blue-300
                     hover:shadow-sm transition"
        >
          <div className="flex items-center justify-between">
            <div>
              <span className="text-lg font-medium">{entity.name}</span>
            </div>
            <span
              className={`text-xs px-2 py-1 rounded-full ${TYPE_COLORS[entity.entity_type] || "bg-gray-100"}`}
            >
              {TYPE_LABELS[entity.entity_type] || entity.entity_type}
            </span>
          </div>
        </a>
      ))}
    </div>
  );
}
