"use client";

import { useEffect, useState, useCallback } from "react";
import { deleteDocument } from "@/lib/admin-api";

export default function DocumentsPage() {
  const [items, setItems] = useState<Record<string, string | null>[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [statusFilter, setStatusFilter] = useState("");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ page: String(page), limit: "20" });
      if (statusFilter) params.set("status", statusFilter);
      const res = await fetch(`/api/v1/admin/documents?${params}`, { credentials: "include" });
      const data = await res.json();
      setItems(data.data || []);
      setTotal(data.meta?.total || 0);
    } catch {
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [page, statusFilter]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleDelete = async (id: string) => {
    if (!confirm("למחוק את המסמך?")) return;
    await deleteDocument(id);
    fetchData();
  };

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-4">ניהול מסמכים</h1>

      <div className="flex gap-2 mb-4">
        {["", "pending", "complete"].map((s) => (
          <button
            key={s}
            onClick={() => { setStatusFilter(s); setPage(1); }}
            className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
              statusFilter === s ? "bg-primary-100 text-primary-700" : "text-gray-500 hover:bg-gray-100"
            }`}
          >
            {s === "" ? "הכל" : s === "pending" ? "ממתין" : "הושלם"}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="text-gray-400 py-8 text-center">טוען...</div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="text-start px-4 py-3 font-medium text-gray-700">כותרת</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700">המרה</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700">חילוץ</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700">תאריך</th>
                <th className="px-4 py-3 w-20"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map((item) => (
                <tr key={item.id!} className="hover:bg-gray-50">
                  <td className="px-4 py-3 text-gray-700 max-w-xs truncate">{item.title || "—"}</td>
                  <td className="px-4 py-3">
                    <span className={`text-xs px-2 py-0.5 rounded-full ${
                      item.conversion_status === "complete" ? "bg-green-50 text-green-700" : "bg-yellow-50 text-yellow-700"
                    }`}>{item.conversion_status}</span>
                  </td>
                  <td className="px-4 py-3">
                    <span className={`text-xs px-2 py-0.5 rounded-full ${
                      item.extraction_status === "complete" ? "bg-green-50 text-green-700" : "bg-yellow-50 text-yellow-700"
                    }`}>{item.extraction_status}</span>
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-500">{item.created_at?.slice(0, 10) || "—"}</td>
                  <td className="px-4 py-3">
                    <button onClick={() => handleDelete(item.id!)} className="text-xs text-red-500 hover:text-red-700">מחק</button>
                  </td>
                </tr>
              ))}
              {items.length === 0 && (
                <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-400">אין מסמכים</td></tr>
              )}
            </tbody>
          </table>
          {total > 20 && (
            <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200">
              <span className="text-xs text-gray-500">{total} סה&quot;כ</span>
              <div className="flex gap-1">
                <button disabled={page <= 1} onClick={() => setPage((p) => p - 1)} className="px-3 py-1 text-xs rounded border disabled:opacity-50">הקודם</button>
                <button disabled={page * 20 >= total} onClick={() => setPage((p) => p + 1)} className="px-3 py-1 text-xs rounded border disabled:opacity-50">הבא</button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
