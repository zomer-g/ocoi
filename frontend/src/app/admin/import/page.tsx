"use client";

import { useState } from "react";
import { triggerImport } from "@/lib/admin-api";

export default function ImportPage() {
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");

  const handleTrigger = async () => {
    setLoading(true);
    setMessage("");
    try {
      const res = await triggerImport() as { message?: string };
      setMessage(res.message || "הייבוא הופעל בהצלחה");
    } catch (err) {
      setMessage("שגיאה בהפעלת הייבוא");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-4">ייבוא מסמכים</h1>

      <div className="bg-white rounded-lg border border-gray-200 p-6 max-w-lg">
        <p className="text-sm text-gray-600 mb-4">
          הפעלת תהליך ייבוא מסמכים חדשים ממקורות המידע (CKAN, Gov.il).
        </p>

        <button
          onClick={handleTrigger}
          disabled={loading}
          className="px-6 py-3 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors font-medium disabled:opacity-50"
        >
          {loading ? "מפעיל..." : "הפעל ייבוא"}
        </button>

        {message && (
          <div className="mt-4 p-3 rounded-lg bg-gray-50 text-sm text-gray-700">
            {message}
          </div>
        )}
      </div>
    </div>
  );
}
