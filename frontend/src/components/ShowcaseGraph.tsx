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

  // After loading: hide if there's no usable data.
  if (!loading && (!graph || graph.nodes.length < 2 || graph.edges.length < 1)) {
    return null;
  }

  // Identify the two "centre" persons — those with the most edges in the
  // subgraph. Everything else is a neighbour orbiting one (or both) of them.
  let person1: { id: string; name: string } | undefined;
  let person2: { id: string; name: string } | undefined;
  let sharedNames: string[] = [];

  if (graph) {
    const degree = new Map<string, number>();
    for (const e of graph.edges) {
      degree.set(e.source_id, (degree.get(e.source_id) || 0) + 1);
      degree.set(e.target_id, (degree.get(e.target_id) || 0) + 1);
    }
    const persons = graph.nodes
      .filter((n) => n.entity_type === "person")
      .sort((a, b) => (degree.get(b.id) || 0) - (degree.get(a.id) || 0));
    person1 = persons[0];
    person2 = persons[1];

    if (person1 && person2) {
      const p1Neighbors = new Set<string>();
      const p2Neighbors = new Set<string>();
      for (const e of graph.edges) {
        if (e.source_id === person1.id) p1Neighbors.add(e.target_id);
        if (e.target_id === person1.id) p1Neighbors.add(e.source_id);
        if (e.source_id === person2.id) p2Neighbors.add(e.target_id);
        if (e.target_id === person2.id) p2Neighbors.add(e.source_id);
      }
      const sharedIds = [...p1Neighbors].filter((id) => p2Neighbors.has(id));
      sharedNames = graph.nodes
        .filter((n) => sharedIds.includes(n.id) && n.entity_type !== "person")
        .map((n) => `${TYPE_LABEL_HE[n.entity_type] || ""} ${n.name}`.trim())
        .slice(0, 3);
    }
  }

  const caption =
    person1 && person2
      ? sharedNames.length > 0
        ? `${person1.name} ו־${person2.name} מקושרים שניהם ל${sharedNames.join(", ")}${
            sharedNames.length >= 3 ? " ועוד" : ""
          }`
        : `${person1.name} ו־${person2.name} — חולקים ישויות במאגר`
      : "דוגמה לקשר מתוך המאגר";

  return (
    <section className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 pt-8 pb-2">
      <div className="text-center mb-4">
        <h2 className="text-xl sm:text-2xl font-bold text-gray-900 mb-1">
          המחשה: שני בעלי תפקיד וכל הקשרים סביבם
        </h2>
        <p className="text-sm text-gray-500 max-w-3xl mx-auto">
          {caption}
        </p>
      </div>

      <div
        className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden"
        style={{ height: 520 }}
      >
        {loading || !graph ? (
          <div className="flex items-center justify-center h-full text-gray-400 text-sm">
            טוען המחשה…
          </div>
        ) : (
          <ConnectionMap
            graph={graph}
            onNodeClick={(nodeId, nodeType) => {
              window.location.href = `/entity?id=${nodeId}&type=${nodeType}`;
            }}
          />
        )}
      </div>

      <p className="text-xs text-gray-400 text-center mt-2">
        לחיצה על צומת תפתח את עמוד הישות. דוגמה אחת מתוך אלפי קשרים שתוכלו לחקור באתר.
      </p>
    </section>
  );
}
