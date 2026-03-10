"use client";

import type { EntitySummary } from "@/lib/api-client";

const TYPE_LABELS: Record<string, string> = {
  person: "איש/אשת ציבור",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

const TYPE_COLORS: Record<string, string> = {
  person: "bg-primary-50 text-primary-800 border-primary-200",
  company: "bg-green-50 text-green-800 border-green-200",
  association: "bg-purple-50 text-purple-800 border-purple-200",
  domain: "bg-amber-50 text-amber-800 border-amber-200",
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
          className="block bg-white rounded-lg p-4 border border-gray-200 hover:shadow-sm transition-shadow"
        >
          <div className="flex items-center justify-between">
            <div>
              <span className="text-base font-medium text-gray-900">{entity.name}</span>
            </div>
            <span
              className={`text-xs px-2 py-0.5 rounded-full border ${TYPE_COLORS[entity.entity_type] || "bg-gray-50 border-gray-200"}`}
            >
              {TYPE_LABELS[entity.entity_type] || entity.entity_type}
            </span>
          </div>
        </a>
      ))}
    </div>
  );
}
