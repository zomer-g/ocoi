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
    <form onSubmit={handleSubmit} className="flex gap-2 max-w-2xl mx-auto">
      <div className="relative flex-1">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="חיפוש אנשים, חברות, עמותות..."
          className="w-full pr-12 pl-4 py-4 rounded-xl text-lg text-gray-900
                     shadow-lg focus:outline-none focus:ring-2 focus:ring-primary-300
                     focus:ring-offset-2 focus:ring-offset-primary-700"
          dir="rtl"
        />
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400 pointer-events-none"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <circle cx="11" cy="11" r="8" />
          <path d="m21 21-4.35-4.35" />
        </svg>
      </div>
      <select
        value={type}
        onChange={(e) => setType(e.target.value)}
        className="px-3 py-3 border border-gray-300 rounded-xl bg-white text-sm shadow-lg"
      >
        {ENTITY_TYPES.map((t) => (
          <option key={t.value} value={t.value}>
            {t.label}
          </option>
        ))}
      </select>
      <button
        type="submit"
        className="px-6 py-3 bg-primary-700 text-white rounded-xl hover:bg-primary-800 transition-colors shadow-lg font-medium"
      >
        חיפוש
      </button>
    </form>
  );
}
