"use client";

import { useEffect, useState } from "react";
import { getAdminStats } from "@/lib/admin-api";

const STAT_LABELS: Record<string, string> = {
  persons: "אנשים",
  companies: "חברות",
  associations: "עמותות",
  domains: "תחומים",
  documents: "מסמכים",
  relationships: "קשרים",
  sources: "מקורות",
};

export default function AdminDashboard() {
  const [stats, setStats] = useState<Record<string, number>>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getAdminStats()
      .then((res) => setStats(res.data))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="text-gray-400">טוען...</div>;

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">לוח בקרה</h1>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
        {Object.entries(STAT_LABELS).map(([key, label]) => (
          <div key={key} className="bg-white rounded-lg border border-gray-200 p-4">
            <div className="text-2xl font-bold text-primary-700">{stats[key] ?? 0}</div>
            <div className="text-sm text-gray-500 mt-1">{label}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
