"use client";

import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { updatePerson, updateCompany, updateAssociation, updateDomain } from "@/lib/admin-api";

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
  aliases: "כינויים (שמות קודמים)",
};

const UPDATE_FN: Record<string, (id: string, data: Record<string, string | null>) => Promise<unknown>> = {
  persons: updatePerson,
  companies: updateCompany,
  associations: updateAssociation,
  domains: updateDomain,
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

  const [entity, setEntity] = useState<Record<string, unknown> | null>(null);
  const [docs, setDocs] = useState<DocItem[]>([]);
  const [loading, setLoading] = useState(true);

  // Inline edit state
  const [editingName, setEditingName] = useState(false);
  const [newName, setNewName] = useState("");
  const [saving, setSaving] = useState(false);

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

  const handleSaveName = async () => {
    if (!entity || !newName.trim() || newName.trim() === entity.name_hebrew) {
      setEditingName(false);
      return;
    }
    const updateFn = UPDATE_FN[entityType];
    if (!updateFn) return;

    setSaving(true);
    try {
      await updateFn(entityId, { name_hebrew: newName.trim() });
      // Refetch entity to get updated aliases
      const res = await fetch(`/api/v1/${entityType}/${entityId}`, { credentials: "include" });
      const data = await res.json();
      setEntity(data.data || null);
      setEditingName(false);
    } catch (e) {
      alert(`שגיאה בשמירה: ${e instanceof Error ? e.message : "שגיאה"}`);
    } finally {
      setSaving(false);
    }
  };

  if (!entityType || !entityId) return <div className="text-red-500 py-8 text-center">פרמטרים חסרים</div>;
  if (loading) return <div className="text-gray-400 py-8 text-center">טוען...</div>;
  if (!entity) return <div className="text-red-500 py-8 text-center">ישות לא נמצאה</div>;

  const typeLabel = TYPE_LABELS[entityType] || entityType;
  const aliases: string[] = Array.isArray(entity.aliases) ? entity.aliases : [];

  const displayFields = Object.entries(entity).filter(
    ([key]) => key !== "id" && key !== "aliases" && FIELD_LABELS[key]
  );

  return (
    <div>
      <div className="mb-4">
        <Link href="/admin/entities" className="text-sm text-primary-600 hover:underline">
          ← חזרה לרשימת ישויות
        </Link>
      </div>

      <div className="flex items-center gap-3 mb-6">
        {editingName ? (
          <div className="flex items-center gap-2">
            <input
              type="text"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              className="text-2xl font-bold text-gray-900 border border-primary-300 rounded px-2 py-1 focus:outline-none focus:ring-2 focus:ring-primary-500"
              autoFocus
              dir="rtl"
              onKeyDown={(e) => {
                if (e.key === "Enter") handleSaveName();
                if (e.key === "Escape") setEditingName(false);
              }}
            />
            <button
              onClick={handleSaveName}
              disabled={saving}
              className="text-sm px-3 py-1.5 bg-primary-600 text-white rounded hover:bg-primary-700 disabled:opacity-50"
            >
              {saving ? "שומר..." : "שמור"}
            </button>
            <button
              onClick={() => setEditingName(false)}
              className="text-sm px-3 py-1.5 bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
            >
              ביטול
            </button>
          </div>
        ) : (
          <>
            <h1 className="text-2xl font-bold text-gray-900">
              {entity.name_hebrew as string || "ישות"}
            </h1>
            <button
              onClick={() => {
                setNewName(entity.name_hebrew as string || "");
                setEditingName(true);
              }}
              className="text-xs px-2 py-1 text-primary-600 border border-primary-300 rounded hover:bg-primary-50"
              title="עריכת שם"
            >
              ✏️ ערוך שם
            </button>
          </>
        )}
        <span className="text-sm px-2.5 py-1 rounded-full bg-primary-100 text-primary-700">
          {typeLabel}
        </span>
      </div>

      {/* Aliases (old names) */}
      {aliases.length > 0 && (
        <div className="mb-4 p-3 bg-amber-50 border border-amber-200 rounded-lg">
          <span className="text-sm text-amber-700 font-medium">{FIELD_LABELS.aliases}: </span>
          <span className="text-sm text-amber-600">
            {aliases.join(" · ")}
          </span>
        </div>
      )}

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
