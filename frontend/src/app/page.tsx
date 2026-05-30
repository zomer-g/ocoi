"use client";

import { useState, useEffect } from "react";
import { SearchBar } from "@/components/search/SearchBar";
import { SearchResults } from "@/components/search/SearchResults";
import { EntityDiscovery } from "@/components/EntityDiscovery";
import { ShowcaseGraph } from "@/components/ShowcaseGraph";
import { MkExpenseToggle } from "@/components/MkExpenseToggle";
import type { EntitySummary } from "@/lib/api-client";
import { getStats } from "@/lib/api-client";

export default function HomePage() {
  const [results, setResults] = useState<EntitySummary[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [stats, setStats] = useState<{ documents: number; entities: number; connections: number } | null>(null);

  useEffect(() => {
    getStats().then((res) => {
      const d = res.data;
      setStats({
        documents: d.documents ?? 0,
        entities: (d.persons ?? 0) + (d.companies ?? 0) + (d.associations ?? 0),
        connections: d.relationships ?? 0,
      });
    }).catch(() => {});
  }, []);

  const handleSearch = async (q: string) => {
    setQuery(q);
    setLoading(true);
    try {
      const res = await fetch(
        `/api/v1/search?q=${encodeURIComponent(q)}&limit=50`
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
    <>
      {/* Hero section — canonical per DESIGN_SYSTEM.md → "Home Hero
          (canonical)". Sizes / paddings / colours MUST stay aligned
          with OCAL + OVER. */}
      <section className="bg-gradient-to-b from-primary-800 to-primary-700 py-12 sm:py-16 px-4">
        <div className="max-w-3xl mx-auto text-center">
          <h1 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-2">
            ניגוד עניינים לעם
          </h1>
          <p className="text-primary-100 text-sm sm:text-base mb-8">
            חיפוש בהסדרי ניגוד עניינים של בעלי תפקידים ציבוריים
          </p>

          <SearchBar onSearch={handleSearch} />

          {!loading && !query && stats && (
            <div className="flex justify-center gap-8 sm:gap-12 mt-8">
              <div className="text-center">
                <div className="text-2xl sm:text-3xl font-bold text-white">{stats.documents.toLocaleString()}</div>
                <div className="text-xs sm:text-sm text-primary-200">מסמכים</div>
              </div>
              <div className="text-center">
                <div className="text-2xl sm:text-3xl font-bold text-white">{stats.entities.toLocaleString()}</div>
                <div className="text-xs sm:text-sm text-primary-200">ישויות</div>
              </div>
              <div className="text-center">
                <div className="text-2xl sm:text-3xl font-bold text-white">{stats.connections.toLocaleString()}</div>
                <div className="text-xs sm:text-sm text-primary-200">קשרים</div>
              </div>
            </div>
          )}
        </div>
      </section>

      {/* Results section */}
      {loading && (
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="text-center py-8 text-gray-400">טוען...</div>
        </div>
      )}

      {!loading && query && (
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="flex items-center justify-between mb-4">
            <p className="text-sm text-gray-500">
              {total} תוצאות עבור &quot;{query}&quot;
            </p>
            {/* Subtle — controls the relationship graph that appears
                when the visitor clicks through into an entity page. */}
            <MkExpenseToggle />
          </div>
          <SearchResults results={results} />
        </div>
      )}

      {/* Showcase: a real connection from the data, shown when not searching.
          The toggle sits in the showcase header (just below the section
          title) so it stays understated but discoverable. */}
      {!loading && !query && (
        <>
          <ShowcaseGraph />
          <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 -mt-1 mb-2 flex justify-center">
            <MkExpenseToggle />
          </div>
        </>
      )}

      {/* Entity Discovery - shown when not searching */}
      {!loading && !query && <EntityDiscovery />}
    </>
  );
}
