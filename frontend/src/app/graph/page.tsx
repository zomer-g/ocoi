"use client";

import { useState, useCallback } from "react";
import { ConnectionMap } from "@/components/graph/ConnectionMap";
import { ConnectionTable } from "@/components/graph/ConnectionTable";
import type { SubGraph } from "@/lib/api-client";

export default function GraphPage() {
  const [graph, setGraph] = useState<SubGraph | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [showTable, setShowTable] = useState(false);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!searchQuery.trim()) return;
    setLoading(true);

    try {
      // First search for the entity
      const searchRes = await fetch(
        `/api/v1/search?q=${encodeURIComponent(searchQuery)}&limit=1`
      );
      const searchData = await searchRes.json();
      const firstResult = searchData.data?.[0];

      if (firstResult) {
        // Then get the graph
        const graphRes = await fetch(
          `/api/v1/graph/neighbors/${firstResult.id}?type=${firstResult.entity_type}&depth=2`
        );
        const graphData = await graphRes.json();
        setGraph(graphData.data);
      }
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  };

  const handleExpand = useCallback(async (nodeId: string, nodeType: string) => {
    try {
      const res = await fetch(
        `/api/v1/graph/neighbors/${nodeId}?type=${nodeType}&depth=1`
      );
      const data = await res.json();
      const newGraph: SubGraph = data.data;

      // Merge with existing graph
      setGraph((prev) => {
        if (!prev) return newGraph;
        const existingNodeIds = new Set(prev.nodes.map((n) => n.id));
        const existingEdgeKeys = new Set(
          prev.edges.map((e) => `${e.source_id}-${e.target_id}-${e.relationship_type}`)
        );

        const newNodes = newGraph.nodes.filter((n) => !existingNodeIds.has(n.id));
        const newEdges = newGraph.edges.filter(
          (e) => !existingEdgeKeys.has(`${e.source_id}-${e.target_id}-${e.relationship_type}`)
        );

        return {
          nodes: [...prev.nodes, ...newNodes],
          edges: [...prev.edges, ...newEdges],
        };
      });
    } catch {
      // ignore
    }
  }, []);

  return (
    <div className="flex flex-col" style={{ minHeight: "calc(100vh - 60px)" }}>
      <div className="p-4 border-b border-gray-200 bg-white flex items-start justify-between gap-4">
        <div>
          <form onSubmit={handleSearch} className="flex gap-2 max-w-xl">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="חיפוש ישות להצגה במפת קשרים..."
              className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm
                         focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              dir="rtl"
            />
            <button
              type="submit"
              disabled={loading}
              className="px-4 py-2 bg-primary-700 text-white rounded-lg text-sm font-medium
                         hover:bg-primary-800 transition-colors disabled:opacity-50"
            >
              {loading ? "טוען..." : "הצג מפה"}
            </button>
          </form>
          <p className="text-xs text-gray-400 mt-1">
            לחיצה כפולה על צומת להרחבת הקשרים שלו
          </p>
        </div>

        {graph && graph.edges.length > 0 && (
          <button
            onClick={() => setShowTable((v) => !v)}
            className="px-3 py-2 text-sm border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors shrink-0"
          >
            {showTable ? "הסתר טבלה" : "הצג כטבלה"}
          </button>
        )}
      </div>

      <div className="flex-1 bg-gray-100" style={{ minHeight: "500px" }}>
        {graph ? (
          <ConnectionMap
            graph={graph}
            onNodeClick={(nodeId, nodeType) => {
              window.location.href = `/entity?id=${nodeId}&type=${nodeType}`;
            }}
            onExpandNode={handleExpand}
          />
        ) : (
          <div className="flex items-center justify-center h-full text-gray-400" style={{ minHeight: "500px" }}>
            חפשו שם של איש ציבור, חברה או עמותה כדי להציג את מפת הקשרים
          </div>
        )}
      </div>

      {/* Accessible table view */}
      {showTable && graph && graph.edges.length > 0 && (
        <div className="p-4 bg-white border-t border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900 mb-3">טבלת קשרים</h2>
          <ConnectionTable
            edges={graph.edges}
            nodes={graph.nodes}
            caption="רשימת כל הקשרים המוצגים במפה"
          />
        </div>
      )}
    </div>
  );
}
