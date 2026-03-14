"use client";

import { useEffect, useState } from "react";

export default function AboutPage() {
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/v1/site/content/about_content")
      .then((r) => r.json())
      .then((d) => {
        setContent(d?.data?.value || "");
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <>
      <section className="bg-gradient-to-b from-primary-800 to-primary-700 py-10 px-4">
        <div className="max-w-4xl mx-auto text-center">
          <h1 className="text-2xl sm:text-3xl font-bold text-white mb-2">אודות</h1>
          <p className="text-primary-200 text-sm sm:text-base">
            אודות הפרויקט ניגוד עניינים לעם
          </p>
        </div>
      </section>

      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {loading ? (
          <div className="text-center py-12 text-gray-400">טוען...</div>
        ) : content ? (
          <div
            className="prose prose-lg max-w-none bg-white rounded-xl shadow-sm border border-gray-200 p-6 sm:p-8"
            dir="rtl"
            dangerouslySetInnerHTML={{ __html: content }}
          />
        ) : (
          <div className="text-center py-12 bg-white rounded-xl shadow-sm border border-gray-200">
            <p className="text-gray-400 text-lg">עמוד האודות טרם הוגדר</p>
            <p className="text-gray-400 text-sm mt-2">ניתן לערוך את התוכן בפאנל הניהול → תוכן האתר</p>
          </div>
        )}
      </div>
    </>
  );
}
