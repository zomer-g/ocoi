"use client";

import { useEffect, useState, useCallback } from "react";
import { deleteRelationship, deleteRelationshipsBulk } from "@/lib/admin-api";
import Link from "next/link";

interface RelItem {
  id: string;
  entity1_name: string;
  entity1_type: string;
  entity2_name: string;
  entity2_type: string;
  relationship_type: string;
  details: string | null;
  confidence: number;
  document_id: string;
  document_title: string;
  source_name: string;
  source_date: string | null;
  created_at: string | null;
}

const TYPE_LABELS: Record<string, string> = {
  person: "אדם",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

const TYPE_COLORS: Record<string, string> = {
  person: "bg-blue-100 text-blue-700",
  company: "bg-amber-100 text-amber-700",
  association: "bg-green-100 text-green-700",
  domain: "bg-purple-100 text-purple-700",
};

function EntityBadge({ name, type }: { name: string; type: string }) {
  const color = TYPE_COLORS[type.toLowerCase()] || "bg-gray-100 text-gray-700";
  const label = TYPE_LABELS[type.toLowerCase()] || type;
  return (
    <div className="flex flex-col gap-0.5">
      <span className="font-medium text-gray-900">{name}</span>
      <span className={`text-[10px] px-1.5 py-0.5 rounded-full w-fit ${color}`}>{label}</span>
    </div>
  );
}

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleDateString("he-IL");
  } catch {
    return iso;
  }
}

export default function RelationshipsPage() {
  const [items, setItems] = useState<RelItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [deleting, setDeleting] = useState(false);
  const [search, setSearch] = useState("");
  const [searchInput, setSearchInput] = useState("");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ page: String(page), limit: "50" });
      if (search) params.set("q", search);
      const res = await fetch(`/api/v1/admin/relationships?${params}`, { credentials: "include" });
      const data = await res.json();
      setItems(data.data || []);
      setTotal(data.meta?.total || 0);
    } catch {
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [page, search]);

  useEffect(() => { fetchData(); }, [fetchData]);
  useEffect(() => { setSelected(new Set()); }, [items]);

  const handleSearch = () => { setSearch(searchInput.trim()); setPage(1); };

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === items.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(items.map((i) => i.id)));
    }
  };

  const handleDeleteSelected = async () => {
    if (selected.size === 0) return;
    if (!confirm(`למחוק ${selected.size} קשרים?`)) return;
    setDeleting(true);
    try {
      await deleteRelationshipsBulk(Array.from(selected));
      setSelected(new Set());
      fetchData();
    } catch {
      alert("שגיאה במחיקה");
    } finally {
      setDeleting(false);
    }
  };

  const handleDeleteOne = async (id: string) => {
    if (!confirm("למחוק את הקשר?")) return;
    await deleteRelationship(id);
    fetchData();
  };

  const totalPages = Math.ceil(total / 50);

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-bold text-gray-900">ניהול קשרים</h1>
        {selected.size > 0 && (
          <button
            onClick={handleDeleteSelected}
            disabled={deleting}
            className="px-4 py-2 bg-red-600 text-white text-sm rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50"
          >
            {deleting ? "מוחק..." : `מחק ${selected.size} נבחרים`}
          </button>
        )}
      </div>

      {/* Search */}
      <div className="flex gap-2 mb-4">
        <input
          type="text"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSearch()}
          placeholder="חיפוש לפי סוג קשר..."
          className="flex-1 px-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:border-primary-500"
          dir="rtl"
        />
        <button onClick={handleSearch} className="px-4 py-2 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors text-sm font-medium">חיפוש</button>
        {search && (
          <button onClick={() => { setSearch(""); setSearchInput(""); setPage(1); }} className="px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 text-sm text-gray-600">נקה</button>
        )}
      </div>

      {loading ? (
        <div className="text-gray-400 py-8 text-center">טוען...</div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="px-3 py-3 w-10">
                    <input
                      type="checkbox"
                      checked={items.length > 0 && selected.size === items.length}
                      onChange={toggleAll}
                      className="rounded border-gray-300"
                    />
                  </th>
                  <th className="text-start px-3 py-3 font-medium text-gray-700">ישות 1</th>
                  <th className="text-start px-3 py-3 font-medium text-gray-700">ישות 2</th>
                  <th className="text-start px-3 py-3 font-medium text-gray-700">סוג קשר</th>
                  <th className="text-start px-3 py-3 font-medium text-gray-700">מקור</th>
                  <th className="text-start px-3 py-3 font-medium text-gray-700">מסמך</th>
                  <th className="text-start px-3 py-3 font-medium text-gray-700">תאריך מקור</th>
                  <th className="text-start px-3 py-3 font-medium text-gray-700">תאריך ייבוא</th>
                  <th className="px-3 py-3 w-16"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {items.map((item) => (
                  <tr key={item.id} className={`hover:bg-gray-50 ${selected.has(item.id) ? "bg-blue-50" : ""}`}>
                    <td className="px-3 py-3">
                      <input
                        type="checkbox"
                        checked={selected.has(item.id)}
                        onChange={() => toggleSelect(item.id)}
                        className="rounded border-gray-300"
                      />
                    </td>
                    <td className="px-3 py-3">
                      <EntityBadge name={item.entity1_name} type={item.entity1_type} />
                    </td>
                    <td className="px-3 py-3">
                      <EntityBadge name={item.entity2_name} type={item.entity2_type} />
                    </td>
                    <td className="px-3 py-3 text-gray-700">{item.relationship_type}</td>
                    <td className="px-3 py-3 text-gray-600 max-w-[160px] truncate" title={item.source_name}>
                      {item.source_name || "—"}
                    </td>
                    <td className="px-3 py-3 max-w-[200px]">
                      {item.document_id ? (
                        <Link
                          href={`/admin/documents/detail?id=${item.document_id}`}
                          className="text-primary-700 hover:underline truncate block"
                          title={item.document_title}
                        >
                          {item.document_title || "צפה במסמך"}
                        </Link>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td className="px-3 py-3 text-gray-500 text-xs whitespace-nowrap">
                      {formatDate(item.source_date)}
                    </td>
                    <td className="px-3 py-3 text-gray-500 text-xs whitespace-nowrap">
                      {formatDate(item.created_at)}
                    </td>
                    <td className="px-3 py-3">
                      <button
                        onClick={() => handleDeleteOne(item.id)}
                        className="text-xs text-red-500 hover:text-red-700"
                      >
                        מחק
                      </button>
                    </td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr>
                    <td colSpan={9} className="px-4 py-8 text-center text-gray-400">
                      אין קשרים
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
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
