"use client";

import { useEffect, useState } from "react";
import { ConnectionMap } from "@/components/graph/ConnectionMap";
import type { SubGraph } from "@/lib/api-client";

const TYPE_LABEL_HE: Record<string, string> = {
  person: "איש/אשת ציבור",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

export function ShowcaseGraph() {
  const [graph, setGraph] = useState<SubGraph | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    fetch("/api/v1/graph/showcase")
      .then((r) => r.json())
      .then((d) => {
        if (cancelled) return;
        setGraph(d?.data ?? null);
      })
      .catch(() => {
        if (cancelled) return;
        setGraph(null);
      })
      .finally(() => {
        if (cancelled) return;
        setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Hide the section entirely while loading or when there is no data
  if (loading || !graph || graph.nodes.length < 2 || graph.edges.length < 1) {
    return null;
  }

  // Build a one-line caption summarising the showcase
  const persons = graph.nodes.filter((n) => n.entity_type === "person");
  const hub = graph.nodes.find((n) => n.entity_type !== "person");
  const caption =
    persons.length >= 2 && hub
      ? `${persons[0].name} ו־${persons[1].name} הצהירו שניהם על קשר ל${TYPE_LABEL_HE[hub.entity_type] || ""} ${hub.name}`
      : persons.length >= 2
      ? `${persons[0].name} ו־${persons[1].name} מקושרים זה לזה במסמך מוצהר`
      : "דוגמה לקשר שמופיע במאגר";

  return (
    <section className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 pt-8 pb-2">
      <div className="text-center mb-4">
        <h2 className="text-xl sm:text-2xl font-bold text-gray-900 mb-1">
          המחשה: כך נראה קשר במאגר
        </h2>
        <p className="text-sm text-gray-500">
          {caption}
        </p>
      </div>

      <div
        className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden"
        style={{ height: 360 }}
      >
        <ConnectionMap
          graph={graph}
          onNodeClick={(nodeId, nodeType) => {
            window.location.href = `/entity?id=${nodeId}&type=${nodeType}`;
          }}
        />
      </div>

      <p className="text-xs text-gray-400 text-center mt-2">
        לחיצה על צומת תפתח את עמוד הישות. דוגמה אחת מתוך עשרות אלפי קשרים שתוכלו לחקור באתר.
      </p>
    </section>
  );
}
