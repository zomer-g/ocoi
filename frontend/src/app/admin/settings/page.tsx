"use client";

import { useEffect, useState } from "react";
import { getAdminUsers } from "@/lib/admin-api";

export default function SettingsPage() {
  const [admins, setAdmins] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getAdminUsers()
      .then((res) => setAdmins(res.data))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-4">הגדרות</h1>

      <div className="bg-white rounded-lg border border-gray-200 p-6 max-w-lg">
        <h2 className="text-lg font-semibold text-gray-900 mb-3">מנהלים מורשים</h2>
        <p className="text-xs text-gray-500 mb-4">
          רשימת כתובות האימייל המורשות לגישה לפאנל הניהול. לשינוי, עדכנו את משתנה הסביבה ADMIN_EMAILS.
        </p>
        {loading ? (
          <div className="text-gray-400 text-sm">טוען...</div>
        ) : admins.length === 0 ? (
          <div className="text-gray-400 text-sm">לא הוגדרו מנהלים (ADMIN_EMAILS ריק)</div>
        ) : (
          <ul className="space-y-2">
            {admins.map((email) => (
              <li key={email} className="flex items-center gap-2 text-sm">
                <span className="w-2 h-2 rounded-full bg-green-400"></span>
                <span className="text-gray-700">{email}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
