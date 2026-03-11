"use client";

import { useEffect, useState, useCallback } from "react";
import { deleteRelationship } from "@/lib/admin-api";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function deleteRelationshipById(id: string) { return deleteRelationship(id); }

export default function RelationshipsPage() {
  const [items, setItems] = useState<Record<string, string | number>[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/v1/admin/relationships?page=${page}&limit=20`, { credentials: "include" });
      const data = await res.json();
      setItems(data.data || []);
      setTotal(data.meta?.total || 0);
    } catch {
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [page]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleDelete = async (id: string) => {
    if (!confirm("למחוק את הקשר?")) return;
    await deleteRelationshipById(id);
    fetchData();
  };

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-4">ניהול קשרים</h1>
      {loading ? (
        <div className="text-gray-400 py-8 text-center">טוען...</div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="text-start px-4 py-3 font-medium text-gray-700">סוג מקור</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700">סוג יעד</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700">סוג קשר</th>
                <th className="text-start px-4 py-3 font-medium text-gray-700">ביטחון</th>
                <th className="px-4 py-3 w-20"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map((item) => (
                <tr key={String(item.id)} className="hover:bg-gray-50">
                  <td className="px-4 py-3">{String(item.source_entity_type)}</td>
                  <td className="px-4 py-3">{String(item.target_entity_type)}</td>
                  <td className="px-4 py-3">{String(item.relationship_type)}</td>
                  <td className="px-4 py-3">{Number(item.confidence).toFixed(2)}</td>
                  <td className="px-4 py-3">
                    <button onClick={() => handleDelete(String(item.id))} className="text-xs text-red-500 hover:text-red-700">מחק</button>
                  </td>
                </tr>
              ))}
              {items.length === 0 && (
                <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-400">אין קשרים</td></tr>
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
