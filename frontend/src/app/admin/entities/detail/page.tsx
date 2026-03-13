"use client";

import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";

const TYPE_LABELS: Record<string, string> = {
  persons: "אדם",
  companies: "חברה",
  associations: "עמותה",
  domains: "תחום",
};

const FIELD_LABELS: Record<string, string> = {
  name_hebrew: "שם בעברית",
  name_english: "שם באנגלית",
  title: "תואר",
  position: "תפקיד",
  ministry: "משרד",
  registration_number: "מספר רישום",
  company_type: "סוג חברה",
  status: "סטטוס",
  match_confidence: "רמת התאמה",
  description: "תיאור",
  aliases: "כינויים",
};

interface DocItem {
  id: string;
  title: string;
  file_url: string | null;
}

export default function EntityDetailPage() {
  const searchParams = useSearchParams();
  const entityType = searchParams.get("type") || "";
  const entityId = searchParams.get("id") || "";

  const [entity, setEntity] = useState<Record<string, string | number | null> | null>(null);
  const [docs, setDocs] = useState<DocItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!entityType || !entityId) { setLoading(false); return; }
    (async () => {
      try {
        const [entityRes, docsRes] = await Promise.all([
          fetch(`/api/v1/${entityType}/${entityId}`, { credentials: "include" }),
          fetch(`/api/v1/${entityType}/${entityId}/documents`, { credentials: "include" }),
        ]);
        const entityData = await entityRes.json();
        const docsData = await docsRes.json();
        setEntity(entityData.data || null);
        setDocs(docsData.data || []);
      } catch {
        setEntity(null);
      } finally {
        setLoading(false);
      }
    })();
  }, [entityType, entityId]);

  if (!entityType || !entityId) return <div className="text-red-500 py-8 text-center">פרמטרים חסרים</div>;
  if (loading) return <div className="text-gray-400 py-8 text-center">טוען...</div>;
  if (!entity) return <div className="text-red-500 py-8 text-center">ישות לא נמצאה</div>;

  const typeLabel = TYPE_LABELS[entityType] || entityType;

  const displayFields = Object.entries(entity).filter(
    ([key]) => key !== "id" && FIELD_LABELS[key]
  );

  return (
    <div>
      <div className="mb-4">
        <Link href="/admin/entities" className="text-sm text-primary-600 hover:underline">
          ← חזרה לרשימת ישויות
        </Link>
      </div>

      <div className="flex items-center gap-3 mb-6">
        <h1 className="text-2xl font-bold text-gray-900">
          {entity.name_hebrew as string || "ישות"}
        </h1>
        <span className="text-sm px-2.5 py-1 rounded-full bg-primary-100 text-primary-700">
          {typeLabel}
        </span>
      </div>

      {/* Entity details */}
      <div className="bg-white rounded-lg border border-gray-200 p-5 mb-6">
        <h2 className="text-lg font-semibold text-gray-800 mb-3">פרטים</h2>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
          {displayFields.map(([key, value]) => (
            <div key={key}>
              <span className="text-gray-500 block">{FIELD_LABELS[key]}</span>
              <span className="font-medium text-gray-800">
                {value !== null && value !== undefined && value !== "" ? String(value) : "—"}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Related documents */}
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h2 className="text-lg font-semibold text-gray-800 mb-3">
          מסמכים קשורים ({docs.length})
        </h2>
        {docs.length === 0 ? (
          <p className="text-sm text-gray-400">לא נמצאו מסמכים קשורים לישות זו</p>
        ) : (
          <div className="divide-y divide-gray-100">
            {docs.map((doc) => (
              <div key={doc.id} className="flex items-center justify-between py-3">
                <Link
                  href={`/admin/documents/detail?id=${doc.id}`}
                  className="text-sm text-primary-700 hover:underline font-medium"
                >
                  {doc.title || "מסמך"}
                </Link>
                <div className="flex gap-2">
                  {doc.file_url && !doc.file_url.startsWith("upload://") && (
                    <a
                      href={doc.file_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-xs text-primary-600 hover:text-primary-800"
                    >
                      PDF
                    </a>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
