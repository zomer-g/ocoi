"use client";

import { useEffect, useState, useCallback } from "react";
import { deletePerson, deleteCompany, deleteAssociation, deleteDomain } from "@/lib/admin-api";
import Link from "next/link";

type Tab = "persons" | "companies" | "associations" | "domains";

const TABS: { key: Tab; label: string; fields: string[]; metaFields?: string[] }[] = [
  { key: "persons", label: "אנשים", fields: ["name_hebrew"], metaFields: ["title", "position", "ministry"] },
  { key: "companies", label: "חברות", fields: ["name_hebrew", "registration_number", "company_type", "status"] },
  { key: "associations", label: "עמותות", fields: ["name_hebrew", "registration_number"] },
  { key: "domains", label: "תחומים", fields: ["name_hebrew", "description"] },
];

const FIELD_LABELS: Record<string, string> = {
  name_hebrew: "שם",
  title: "תואר",
  position: "תפקיד",
  ministry: "משרד",
  registration_number: "מספר רישום",
  company_type: "סוג",
  status: "סטטוס",
  description: "תיאור",
};

export default function EntitiesPage() {
  const [tab, setTab] = useState<Tab>("persons");
  const [items, setItems] = useState<Record<string, string>[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [search, setSearch] = useState("");
  const [searchInput, setSearchInput] = useState("");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ page: String(page), limit: "50" });
      if (search) params.set("q", search);
      const res = await fetch(`/api/v1/${tab}?${params}`, { credentials: "include" });
      const data = await res.json();
      setItems(data.data || []);
      setTotal(data.meta?.total || 0);
    } catch {
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [tab, page, search]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleSearch = () => {
    setSearch(searchInput.trim());
    setPage(1);
  };

  const handleDelete = async (id: string) => {
    if (!confirm("למחוק את הרשומה?")) return;
    const deleteFn = { persons: deletePerson, companies: deleteCompany, associations: deleteAssociation, domains: deleteDomain }[tab];
    await deleteFn(id);
    fetchData();
  };

  const tabConfig = TABS.find((t) => t.key === tab)!;
  const totalPages = Math.ceil(total / 50);

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-4">ניהול ישויות</h1>

      {/* Tabs */}
      <div className="flex gap-1 mb-4">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => { setTab(t.key); setPage(1); setSearch(""); setSearchInput(""); }}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              tab === t.key
                ? "bg-primary-100 text-primary-700"
                : "text-gray-500 hover:bg-gray-100"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Search */}
      <div className="flex gap-2 mb-4">
        <input
          type="text"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSearch()}
          placeholder="חיפוש לפי שם..."
          className="flex-1 px-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:border-primary-500"
          dir="rtl"
        />
        <button
          onClick={handleSearch}
          className="px-4 py-2 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium"
        >
          חיפוש
        </button>
        {search && (
          <button
            onClick={() => { setSearch(""); setSearchInput(""); setPage(1); }}
            className="px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 text-sm text-gray-600"
          >
            נקה
          </button>
        )}
      </div>

      {/* Table */}
      {loading ? (
        <div className="text-gray-400 py-8 text-center">טוען...</div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                {tabConfig.fields.map((f) => (
                  <th key={f} className="text-start px-4 py-3 font-medium text-gray-700">
                    {FIELD_LABELS[f] || f}
                  </th>
                ))}
                <th className="text-start px-4 py-3 font-medium text-gray-700">מסמכים</th>
                <th className="px-4 py-3 w-20"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map((item) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  {tabConfig.fields.map((f) => {
                    // For the name field, append metadata in parentheses
                    if (f === "name_hebrew" && tabConfig.metaFields) {
                      const meta = tabConfig.metaFields.map((mf) => item[mf]).filter(Boolean);
                      return (
                        <td key={f} className="px-4 py-3 text-gray-700">
                          <span className="font-medium">{item[f] || "—"}</span>
                          {meta.length > 0 && (
                            <span className="text-gray-400 text-xs mr-1">
                              ({meta.join(", ")})
                            </span>
                          )}
                        </td>
                      );
                    }
                    return <td key={f} className="px-4 py-3 text-gray-700">{item[f] || "—"}</td>;
                  })}
                  <td className="px-4 py-3">
                    <Link
                      href={`/admin/entities/detail?type=${tab}&id=${item.id}`}
                      className="text-xs text-primary-600 hover:underline"
                    >
                      צפה
                    </Link>
                  </td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => handleDelete(item.id)}
                      className="text-xs text-red-500 hover:text-red-700"
                    >
                      מחק
                    </button>
                  </td>
                </tr>
              ))}
              {items.length === 0 && (
                <tr><td colSpan={tabConfig.fields.length + 2} className="px-4 py-8 text-center text-gray-400">אין רשומות</td></tr>
              )}
            </tbody>
          </table>
          {total > 20 && (
            <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200">
              <span className="text-xs text-gray-500">{total} סה&quot;כ · עמוד {page} מתוך {totalPages}</span>
              <div className="flex gap-1">
                <button
                  disabled={page <= 1}
                  onClick={() => setPage((p) => p - 1)}
                  className="px-3 py-1 text-xs rounded border disabled:opacity-50 hover:bg-gray-50"
                >
                  הקודם
                </button>
                <button
                  disabled={page * 20 >= total}
                  onClick={() => setPage((p) => p + 1)}
                  className="px-3 py-1 text-xs rounded border disabled:opacity-50 hover:bg-gray-50"
                >
                  הבא
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
