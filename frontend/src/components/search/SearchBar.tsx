"use client";

import { useState } from "react";

interface SearchBarProps {
  onSearch: (query: string) => void;
}

export function SearchBar({ onSearch }: SearchBarProps) {
  const [query, setQuery] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      onSearch(query.trim());
    }
  };

  return (
    <form onSubmit={handleSubmit} className="max-w-2xl mx-auto">
      <div className="relative">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="חפשו אירוע, נושא או שם..."
          className="w-full pr-12 pl-4 py-4 rounded-2xl text-lg text-white
                     bg-white/15 backdrop-blur-sm border border-white/20
                     placeholder:text-white/60
                     shadow-lg focus:outline-none focus:ring-2 focus:ring-primary-300
                     focus:ring-offset-2 focus:ring-offset-primary-800"
          dir="rtl"
        />
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-white/60 pointer-events-none"
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
    </form>
  );
}
