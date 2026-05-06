"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";

interface DocSummary {
  id: string;
  title: string;
  file_format: string;
  file_url: string | null;
  conversion_status: string;
  extraction_status: string;
}

interface ListResponse {
  status: string;
  data: DocSummary[];
  meta: { total: number; page: number; limit: number; pages: number };
}

const PAGE_SIZE = 20;

export default function DocumentsListPage() {
  const [docs, setDocs] = useState<DocSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [pages, setPages] = useState(0);
  const [page, setPage] = useState(1);
  const [q, setQ] = useState("");
  const [submittedQ, setSubmittedQ] = useState("");
  const [loading, setLoading] = useState(true);

  const fetchPage = useCallback(async (p: number, query: string) => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ page: String(p), limit: String(PAGE_SIZE) });
      if (query.trim()) params.set("q", query.trim());
      const res = await fetch(`/api/v1/documents?${params}`);
      const data: ListResponse = await res.json();
      setDocs(data.data || []);
      setTotal(data.meta?.total || 0);
      setPages(data.meta?.pages || 0);
    } catch {
      setDocs([]);
      setTotal(0);
      setPages(0);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPage(page, submittedQ);
  }, [page, submittedQ, fetchPage]);

  const onSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    setSubmittedQ(q);
  };

  return (
    <>
      <section className="bg-gradient-to-b from-primary-800 to-primary-700 py-10 px-4">
        <div className="max-w-5xl mx-auto text-center">
          <h1 className="text-2xl sm:text-3xl font-bold text-white mb-2">מסמכים</h1>
          <p className="text-primary-200 text-sm sm:text-base mb-5">
            כל הסדרי ניגוד העניינים שנאספו במאגר. לחצו על מסמך כדי לראות את הקשרים שחולצו ממנו.
          </p>
          <form onSubmit={onSearch} className="max-w-xl mx-auto">
            <div className="flex gap-2">
              <input
                type="text"
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="חיפוש בכותרות..."
                dir="rtl"
                className="flex-1 px-4 py-2.5 rounded-lg text-sm bg-white/15 backdrop-blur-sm border border-white/20 text-white placeholder:text-white/60 focus:outline-none focus:ring-2 focus:ring-primary-300"
              />
              <button
                type="submit"
                className="px-5 py-2.5 rounded-lg bg-white text-primary-800 text-sm font-semibold hover:bg-primary-100 transition-colors"
              >
                חפש
              </button>
            </div>
          </form>
        </div>
      </section>

      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {loading ? (
          <div className="text-center py-12 text-gray-400">טוען...</div>
        ) : docs.length === 0 ? (
          <div className="text-center py-12 text-gray-400">לא נמצאו מסמכים</div>
        ) : (
          <>
            <p className="text-sm text-gray-500 mb-4">
              {total.toLocaleString()} מסמכים{submittedQ ? ` עבור "${submittedQ}"` : ""}
            </p>
            <ul className="space-y-2">
              {docs.map((d) => (
                <li
                  key={d.id}
                  className="bg-white rounded-lg border border-gray-200 hover:border-primary-300 hover:shadow-sm transition-all"
                >
                  <Link
                    href={`/document?id=${d.id}`}
                    className="flex items-start gap-3 p-4"
                  >
                    <div className="shrink-0 w-9 h-9 rounded-lg bg-primary-50 text-primary-700 flex items-center justify-center">
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        className="w-5 h-5"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      >
                        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                        <polyline points="14 2 14 8 20 8" />
                      </svg>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-gray-900 line-clamp-2">
                        {d.title || "ללא כותרת"}
                      </div>
                      <div className="mt-1 flex items-center gap-2 text-xs text-gray-500">
                        <span className="uppercase">{d.file_format || "—"}</span>
                        {d.extraction_status === "extracted" && (
                          <span className="px-1.5 py-0.5 rounded-full bg-green-50 text-green-700 border border-green-200">
                            חולץ
                          </span>
                        )}
                      </div>
                    </div>
                  </Link>
                </li>
              ))}
            </ul>

            {pages > 1 && (
              <div className="flex items-center justify-center gap-2 mt-6">
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1}
                  className="px-3 py-1.5 rounded-lg border border-gray-300 text-sm disabled:opacity-40"
                >
                  הקודם
                </button>
                <span className="text-sm text-gray-500">
                  עמוד {page} מתוך {pages}
                </span>
                <button
                  onClick={() => setPage((p) => Math.min(pages, p + 1))}
                  disabled={page >= pages}
                  className="px-3 py-1.5 rounded-lg border border-gray-300 text-sm disabled:opacity-40"
                >
                  הבא
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </>
  );
}
