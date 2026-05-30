"use client";

import { useState } from "react";

interface SearchBarProps {
  onSearch: (query: string) => void;
  placeholder?: string;
}

/**
 * Canonical "Home Hero" search pill — see
 * `frontend/DESIGN_SYSTEM.md → Home Hero (canonical)`.
 *
 * Visuals:
 *  • Single white pill (`bg-white rounded-full shadow-lg`) — NO
 *    separate submit button. Submit happens on Enter; the search icon
 *    is decorative for visual balance.
 *  • Icon sits on the right (RTL) in `text-gray-400`.
 *  • Input is `bg-transparent` so the pill colour shines through.
 *
 * The frosted / glassmorphism variant was retired so the three "לעם"
 * sites (OCOI, OCAL, OVER) all sit on the same white-pill foundation
 * and clear AA contrast targets.
 */
export function SearchBar({
  onSearch,
  placeholder = "חפשו איש ציבור, חברה, עמותה או תחום...",
}: SearchBarProps) {
  const [query, setQuery] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      onSearch(query.trim());
    }
  };

  return (
    <form onSubmit={handleSubmit} className="max-w-2xl mx-auto">
      <div className="relative bg-white rounded-full shadow-lg">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={placeholder}
          className="w-full pr-12 pl-6 py-4 rounded-full text-lg text-gray-900
                     bg-transparent
                     placeholder:text-gray-400
                     focus:outline-none focus:ring-2 focus:ring-primary-500
                     focus:ring-offset-2 focus:ring-offset-primary-800"
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
          aria-hidden="true"
        >
          <circle cx="11" cy="11" r="8" />
          <path d="m21 21-4.35-4.35" />
        </svg>
      </div>
    </form>
  );
}
