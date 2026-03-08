"use client";

import { useState } from "react";
import { SearchBar } from "@/components/search/SearchBar";
import { SearchResults } from "@/components/search/SearchResults";
import type { EntitySummary } from "@/lib/api-client";

export default function HomePage() {
  const [results, setResults] = useState<EntitySummary[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState("");

  const handleSearch = async (q: string, type?: string) => {
    setQuery(q);
    setLoading(true);
    try {
      const res = await fetch(
        `/api/v1/search?q=${encodeURIComponent(q)}${type ? `&type=${type}` : ""}&limit=20`
      );
      const data = await res.json();
      setResults(data.data || []);
      setTotal(data.meta?.total || 0);
    } catch {
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto px-4 py-12">
      <div className="text-center mb-10">
        <h1 className="text-4xl font-bold text-gray-800 mb-3">אינטרסים לעם</h1>
        <p className="text-lg text-gray-500">
          חיפוש בהסדרי ניגוד עניינים של בעלי תפקידים ציבוריים
        </p>
      </div>

      <SearchBar onSearch={handleSearch} />

      {loading && (
        <div className="text-center py-8 text-gray-400">טוען...</div>
      )}

      {!loading && query && (
        <div className="mt-6">
          <p className="text-sm text-gray-500 mb-4">
            {total} תוצאות עבור &quot;{query}&quot;
          </p>
          <SearchResults results={results} />
        </div>
      )}

      {!loading && !query && (
        <div className="mt-16 grid grid-cols-1 md:grid-cols-3 gap-6 text-center">
          <div className="bg-white rounded-lg p-6 shadow-sm border">
            <div className="text-3xl font-bold text-blue-600">988+</div>
            <div className="text-sm text-gray-500 mt-1">מסמכים ממידע לעם</div>
          </div>
          <div className="bg-white rounded-lg p-6 shadow-sm border">
            <div className="text-3xl font-bold text-green-600">345</div>
            <div className="text-sm text-gray-500 mt-1">הסדרי שרים</div>
          </div>
          <div className="bg-white rounded-lg p-6 shadow-sm border">
            <div className="text-3xl font-bold text-purple-600">API</div>
            <div className="text-sm text-gray-500 mt-1">ממשק לאתרים חיצוניים</div>
          </div>
        </div>
      )}
    </div>
  );
}
