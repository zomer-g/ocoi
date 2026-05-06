"use client";

import { useState, useEffect } from "react";

export interface SuggestionTarget {
  target_kind: "document" | "person" | "company" | "association" | "domain" | "relationship";
  target_id: string;
  field_name: string;
  field_label: string; // Hebrew display label, e.g. "כותרת", "שם", "תפקיד"
  current_value: string | null;
  document_id?: string | null;
}

interface Props {
  target: SuggestionTarget | null;
  onClose: () => void;
}

export function SuggestionDialog({ target, onClose }: Props) {
  const [proposedValue, setProposedValue] = useState("");
  const [comment, setComment] = useState("");
  const [email, setEmail] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Reset form whenever a new target is opened
    setProposedValue(target?.current_value || "");
    setComment("");
    setEmail("");
    setSubmitting(false);
    setDone(false);
    setError(null);
  }, [target]);

  // Close on Escape
  useEffect(() => {
    if (!target) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [target, onClose]);

  if (!target) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!proposedValue.trim() && !comment.trim()) {
      setError("מלא ערך מוצע או הערה");
      return;
    }
    setError(null);
    setSubmitting(true);
    try {
      const res = await fetch("/api/v1/suggestions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          target_kind: target.target_kind,
          target_id: target.target_id,
          field_name: target.field_name,
          current_value: target.current_value,
          proposed_value: proposedValue.trim() || null,
          comment: comment.trim() || null,
          submitter_email: email.trim() || null,
          document_id: target.document_id || null,
        }),
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || "שליחה נכשלה");
      }
      setDone(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "שליחה נכשלה");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center bg-black/40 p-4"
      role="dialog"
      aria-modal="true"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        className="bg-white rounded-xl shadow-xl w-full max-w-lg p-5"
        dir="rtl"
      >
        <div className="flex items-start justify-between mb-3">
          <div>
            <h3 className="text-lg font-bold text-gray-900">הוספת הערה / תיקון</h3>
            <p className="text-xs text-gray-500 mt-0.5">
              שדה: <span className="font-medium text-gray-700">{target.field_label}</span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-700 text-xl leading-none"
            aria-label="סגור"
          >
            ✕
          </button>
        </div>

        {done ? (
          <div className="text-center py-8">
            <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-green-100 text-green-700 text-2xl mb-3">
              ✓
            </div>
            <p className="text-gray-800 font-medium">תודה! ההצעה נשלחה לבדיקה.</p>
            <p className="text-xs text-gray-500 mt-1">
              נסקור אותה במהירות ונעדכן את הנתונים בהתאם.
            </p>
            <button
              onClick={onClose}
              className="mt-5 px-4 py-2 rounded-lg bg-primary-700 text-white text-sm font-medium hover:bg-primary-800 transition-colors"
            >
              סגור
            </button>
          </div>
        ) : (
          <form onSubmit={handleSubmit} className="space-y-3">
            {target.current_value && (
              <div>
                <label className="block text-xs text-gray-500 mb-1">ערך נוכחי</label>
                <div className="px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-700 max-h-24 overflow-y-auto">
                  {target.current_value}
                </div>
              </div>
            )}

            <div>
              <label className="block text-xs text-gray-500 mb-1">
                ערך מוצע <span className="text-gray-400">(אופציונלי)</span>
              </label>
              <textarea
                value={proposedValue}
                onChange={(e) => setProposedValue(e.target.value)}
                rows={2}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
                placeholder="מה צריך להופיע במקום?"
              />
            </div>

            <div>
              <label className="block text-xs text-gray-500 mb-1">
                הערה / הסבר <span className="text-gray-400">(אופציונלי)</span>
              </label>
              <textarea
                value={comment}
                onChange={(e) => setComment(e.target.value)}
                rows={3}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
                placeholder="למה? מאיפה זה ידוע? אסמכתא?"
              />
            </div>

            <div>
              <label className="block text-xs text-gray-500 mb-1">
                דוא&quot;ל <span className="text-gray-400">(לא חובה — נצטרך רק אם נצטרך לחזור אליך)</span>
              </label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
                dir="ltr"
                placeholder="you@example.com"
              />
            </div>

            {error && (
              <div className="p-2 rounded-lg bg-red-50 text-red-700 text-sm">{error}</div>
            )}

            <div className="flex justify-end gap-2 pt-2">
              <button
                type="button"
                onClick={onClose}
                className="px-3 py-2 text-sm text-gray-600 hover:text-gray-800"
              >
                ביטול
              </button>
              <button
                type="submit"
                disabled={submitting}
                className="px-4 py-2 rounded-lg bg-primary-700 text-white text-sm font-medium hover:bg-primary-800 disabled:opacity-60 transition-colors"
              >
                {submitting ? "שולח..." : "שלח"}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}


/**
 * Convenience: a small inline button that opens the SuggestionDialog
 * with a given target. Lives next to the field it annotates.
 */
export function SuggestButton({
  onClick,
  label = "הוסף הערה / תיקון",
}: {
  onClick: () => void;
  label?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      aria-label={label}
      className="text-gray-300 hover:text-primary-600 transition-colors text-xs leading-none"
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="w-3.5 h-3.5 inline"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        <path d="M12 20h9" />
        <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
      </svg>
    </button>
  );
}
