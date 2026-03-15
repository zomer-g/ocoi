"use client";

import { useState, useEffect, useCallback } from "react";
import { getTopConnected, getMinistries, type RankedEntity, type MinistryInfo } from "@/lib/api-client";

const TABS = [
  { key: "", label: "הכל" },
  { key: "person", label: "אנשי ציבור" },
  { key: "company", label: "חברות" },
  { key: "association", label: "עמותות" },
  { key: "ministry", label: "משרדים" },
] as const;

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

const PAGE_SIZE = 12;

export function EntityDiscovery() {
  const [activeTab, setActiveTab] = useState("");
  const [entities, setEntities] = useState<RankedEntity[]>([]);
  const [ministries, setMinistries] = useState<MinistryInfo[]>([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  const [loading, setLoading] = useState(true);

  const fetchEntities = useCallback(async (tab: string, pg: number, append: boolean) => {
    setLoading(true);
    try {
      if (tab === "ministry") {
        const res = await getMinistries();
        setMinistries(res.data);
        setTotalPages(1);
      } else {
        const res = await getTopConnected(tab || undefined, pg, PAGE_SIZE);
        setEntities((prev) => (append ? [...prev, ...res.data] : res.data));
        setTotalPages(res.meta.pages);
      }
    } catch {
      if (!append) {
        setEntities([]);
        setMinistries([]);
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    setPage(1);
    setEntities([]);
    setMinistries([]);
    fetchEntities(activeTab, 1, false);
  }, [activeTab, fetchEntities]);

  const loadMore = () => {
    const next = page + 1;
    setPage(next);
    fetchEntities(activeTab, next, true);
  };

  return (
    <section className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="text-center mb-6">
        <h2 className="text-xl sm:text-2xl font-bold text-gray-900 mb-1">
          גלו ישויות
        </h2>
        <p className="text-sm text-gray-500">
          ישויות ממוינות לפי מספר הקשרים — מהמרכזיות ביותר
        </p>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 mb-6 overflow-x-auto pb-1" role="tablist">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            role="tab"
            aria-selected={activeTab === tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`whitespace-nowrap px-4 py-2 rounded-full text-sm font-medium transition-colors ${
              activeTab === tab.key
                ? "bg-primary-700 text-white"
                : "bg-gray-100 text-gray-600 hover:bg-gray-200"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Ministries grid */}
      {activeTab === "ministry" && !loading && ministries.length === 0 && (
        <div className="text-center py-12 text-gray-400">אין משרדים להצגה</div>
      )}

      {activeTab === "ministry" && ministries.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {ministries.map((m, index) => (
            <a
              key={m.ministry}
              href={`/graph?q=${encodeURIComponent(m.ministry)}`}
              className="block bg-white rounded-lg p-4 border border-gray-200 hover:shadow-md transition-shadow"
            >
              <div className="flex items-start justify-between gap-2 mb-2">
                <span className="text-base font-medium text-gray-900 leading-tight">
                  {m.ministry}
                </span>
                {index < 3 && (
                  <span className="shrink-0 text-[10px] px-1.5 py-0.5 rounded bg-amber-100 text-amber-700 border border-amber-200 font-medium">
                    מרכזי
                  </span>
                )}
              </div>
              <div className="flex items-center justify-between">
                <span className="text-xs px-2 py-0.5 rounded-full border bg-blue-50 text-blue-800 border-blue-200">
                  {m.person_count} בעלי תפקידים
                </span>
                <span className="text-xs text-gray-500">
                  {m.connection_count} קשרים
                </span>
              </div>
            </a>
          ))}
        </div>
      )}

      {/* Entity grid */}
      {activeTab !== "ministry" && !loading && entities.length === 0 && (
        <div className="text-center py-12 text-gray-400">אין ישויות להצגה</div>
      )}

      {activeTab !== "ministry" && entities.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {entities.map((entity, index) => {
            const subtitle = [entity.position, entity.ministry].filter(Boolean).join(" · ");
            return (
              <a
                key={entity.id}
                href={`/entity?id=${entity.id}&type=${entity.entity_type}`}
                title={subtitle || undefined}
                className="block bg-white rounded-lg p-4 border border-gray-200 hover:shadow-md transition-shadow"
              >
                <div className="flex items-start justify-between gap-2 mb-1">
                  <span className="text-base font-medium text-gray-900 leading-tight">
                    {entity.name}
                  </span>
                  {index < 3 && page === 1 && !activeTab && (
                    <span className="shrink-0 text-[10px] px-1.5 py-0.5 rounded bg-amber-100 text-amber-700 border border-amber-200 font-medium">
                      מרכזי
                    </span>
                  )}
                </div>
                {subtitle && (
                  <p className="text-xs text-gray-500 mb-2 line-clamp-1">{subtitle}</p>
                )}
                <div className="flex items-center justify-between">
                  <span
                    className={`text-xs px-2 py-0.5 rounded-full border ${TYPE_COLORS[entity.entity_type] || "bg-gray-50 border-gray-200"}`}
                  >
                    {TYPE_LABELS[entity.entity_type] || entity.entity_type}
                  </span>
                  <span className="text-xs text-gray-500">
                    {entity.connection_count} קשרים
                  </span>
                </div>
              </a>
            );
          })}
        </div>
      )}

      {/* Loading state */}
      {loading && (
        <div className="text-center py-8 text-gray-400">טוען...</div>
      )}

      {/* Load more */}
      {!loading && activeTab !== "ministry" && page < totalPages && (
        <div className="text-center mt-6">
          <button
            onClick={loadMore}
            className="px-6 py-2.5 rounded-lg border border-gray-300 text-sm font-medium text-gray-700 hover:bg-gray-50 transition-colors"
          >
            הצג עוד
          </button>
        </div>
      )}
    </section>
  );
}
