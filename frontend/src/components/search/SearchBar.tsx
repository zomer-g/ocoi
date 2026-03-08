"use client";

import { useState } from "react";

const ENTITY_TYPES = [
  { value: "", label: "הכל" },
  { value: "person", label: "אנשים" },
  { value: "company", label: "חברות" },
  { value: "association", label: "עמותות" },
  { value: "domain", label: "תחומים" },
];

interface SearchBarProps {
  onSearch: (query: string, type?: string) => void;
}

export function SearchBar({ onSearch }: SearchBarProps) {
  const [query, setQuery] = useState("");
  const [type, setType] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      onSearch(query.trim(), type || undefined);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="flex gap-2">
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="חיפוש אנשים, חברות, עמותות..."
        className="flex-1 px-4 py-3 border border-gray-300 rounded-lg text-lg
                   focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        dir="rtl"
      />
      <select
        value={type}
        onChange={(e) => setType(e.target.value)}
        className="px-3 py-3 border border-gray-300 rounded-lg bg-white text-sm"
      >
        {ENTITY_TYPES.map((t) => (
          <option key={t.value} value={t.value}>
            {t.label}
          </option>
        ))}
      </select>
      <button
        type="submit"
        className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
      >
        חיפוש
      </button>
    </form>
  );
}
