"use client";

import { useEffect, useRef, useCallback, useState } from "react";
import type { SubGraph } from "@/lib/api-client";

// Cytoscape types
type CyInstance = import("cytoscape").Core;

const NODE_COLORS: Record<string, string> = {
  person: "#3B82F6",     // blue
  company: "#10B981",    // green
  association: "#8B5CF6", // purple
  domain: "#F59E0B",     // amber
};

export const EDGE_LABELS: Record<string, string> = {
  restricted_from: "מוגבל מ-",
  owns: "בעלות",
  manages: "מנהל",
  employed_by: "מועסק ב-",
  related_to: "קשור ל-",
  board_member: "חבר דירקטוריון",
  operates_in: "פועל בתחום",
  family_member: "בן משפחה",
};

let fcoseRegistered = false;

interface ConnectionMapProps {
  graph: SubGraph;
  centerId?: string;
  centerType?: string;
  onNodeClick?: (nodeId: string, nodeType: string) => void;
  onExpandNode?: (nodeId: string, nodeType: string) => void;
}

export function ConnectionMap({
  graph,
  centerId,
  centerType,
  onNodeClick,
  onExpandNode,
}: ConnectionMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<CyInstance | null>(null);
  const [tooltip, setTooltip] = useState<{ x: number; y: number; name: string; title?: string; position?: string; ministry?: string } | null>(null);

  const initGraph = useCallback(async () => {
    if (!containerRef.current) return;

    const cytoscape = (await import("cytoscape")).default;

    // Register fcose layout extension (once)
    if (!fcoseRegistered) {
      try {
        const fcose = (await import("cytoscape-fcose")).default;
        cytoscape.use(fcose);
      } catch {
        // fcose not available, fall back to cose
      }
      fcoseRegistered = true;
    }

    // Build elements
    const nodes = graph.nodes.map((node) => ({
      data: {
        id: node.id,
        label: node.name || node.id.slice(0, 8),
        type: node.entity_type,
        isCenter: node.id === centerId,
        extraTitle: node.extra?.title || "",
        extraPosition: node.extra?.position || "",
        extraMinistry: node.extra?.ministry || "",
      },
    }));

    // Merge edges between the same pair of entities into one line.
    // If any relationship in the pair is "restricted_from", use the thick style.
    const nodeIds = new Set(nodes.map((n) => n.data.id));
    const pairMap = new Map<string, { source: string; target: string; labels: string[]; hasRestriction: boolean; docUrls: string[] }>();

    for (const edge of graph.edges) {
      if (!nodeIds.has(edge.source_id) || !nodeIds.has(edge.target_id)) continue;
      // Canonical key: sorted so A→B and B→A merge together
      const key = [edge.source_id, edge.target_id].sort().join("||");
      const existing = pairMap.get(key);
      const label = EDGE_LABELS[edge.relationship_type] || edge.relationship_type;
      const isRestricted = edge.relationship_type === "restricted_from";
      const docUrl = edge.document_url && !edge.document_url.startsWith("upload://") ? edge.document_url : "";

      if (existing) {
        if (!existing.labels.includes(label)) existing.labels.push(label);
        if (isRestricted) existing.hasRestriction = true;
        if (docUrl && !existing.docUrls.includes(docUrl)) existing.docUrls.push(docUrl);
      } else {
        pairMap.set(key, {
          source: edge.source_id,
          target: edge.target_id,
          labels: [label],
          hasRestriction: isRestricted,
          docUrls: docUrl ? [docUrl] : [],
        });
      }
    }

    const validEdges = Array.from(pairMap.entries()).map(([key, info]) => ({
      data: {
        id: `e_${key}`,
        source: info.source,
        target: info.target,
        label: info.labels.join(" + "),
        relType: info.hasRestriction ? "restricted_from" : "other",
        docUrl: info.docUrls[0] || "",
      },
    }));

    if (cyRef.current) {
      cyRef.current.destroy();
    }

    const cy = cytoscape({
      container: containerRef.current,
      elements: [...nodes, ...validEdges],
      style: [
        // ── Nodes ──
        {
          selector: "node",
          style: {
            label: "data(label)",
            "text-valign": "bottom",
            "text-margin-y": 6,
            "font-size": 13,
            "font-family": "Rubik, Heebo, sans-serif",
            "text-wrap": "wrap",
            "text-max-width": "90px",
            "text-outline-color": "#fff",
            "text-outline-width": 2,
            width: 45,
            height: 45,
            "background-color": (ele: { data: (key: string) => string }) =>
              NODE_COLORS[ele.data("type")] || "#6B7280",
            "border-width": (ele: { data: (key: string) => boolean }) =>
              ele.data("isCenter") ? 3 : 1,
            "border-color": (ele: { data: (key: string) => boolean }) =>
              ele.data("isCenter") ? "#044E66" : "#E5E7EB",
          },
        },
        // ── Edges: WITH restriction (thick, solid, teal) ──
        {
          selector: "edge[relType = 'restricted_from']",
          style: {
            label: "data(label)",
            "font-size": 10,
            "font-family": "Rubik, Heebo, sans-serif",
            "text-rotation": "autorotate",
            "text-outline-color": "#fff",
            "text-outline-width": 1.5,
            "curve-style": "bezier",
            width: 3,
            "line-color": "#044E66",
            "line-style": "solid",
            "target-arrow-color": "#044E66",
            "target-arrow-shape": "triangle",
            "arrow-scale": 1.0,
          },
        },
        // ── Edges: WITHOUT restriction (thin, dashed, gray) ──
        {
          selector: "edge[relType != 'restricted_from']",
          style: {
            label: "data(label)",
            "font-size": 9,
            "font-family": "Rubik, Heebo, sans-serif",
            "text-rotation": "autorotate",
            "text-outline-color": "#fff",
            "text-outline-width": 1.5,
            "curve-style": "bezier",
            width: 1.5,
            "line-color": "#9CA3AF",
            "line-style": "dashed",
            "line-dash-pattern": [6, 3],
            "target-arrow-color": "#9CA3AF",
            "target-arrow-shape": "triangle",
            "arrow-scale": 0.8,
          },
        },
        // ── Selected node ──
        {
          selector: "node:selected",
          style: {
            "border-width": 3,
            "border-color": "#044E66",
          },
        },
      ],
      layout: {
        name: "fcose",
        animate: true,
        animationDuration: 600,
        nodeRepulsion: () => 20000,
        idealEdgeLength: () => 180,
        nodeSeparation: 80,
        gravity: 0.25,
        gravityRange: 3.8,
        quality: "default",
      } as import("cytoscape").LayoutOptions,
    });

    // Click handler
    cy.on("tap", "node", (evt: { target: { data: (key: string) => string } }) => {
      const nodeId = evt.target.data("id");
      const nodeType = evt.target.data("type");
      if (onNodeClick) onNodeClick(nodeId, nodeType);
    });

    // Double-click to expand
    cy.on("dbltap", "node", (evt: { target: { data: (key: string) => string } }) => {
      const nodeId = evt.target.data("id");
      const nodeType = evt.target.data("type");
      if (onExpandNode) onExpandNode(nodeId, nodeType);
    });

    // Click edge to open source document
    cy.on("tap", "edge", (evt: { target: { data: (key: string) => string } }) => {
      const url = evt.target.data("docUrl");
      if (url) window.open(url, "_blank", "noopener,noreferrer");
    });

    // Hover tooltip for person nodes
    cy.on("mouseover", "node", (evt: { target: { data: (key: string) => string }; renderedPosition: { x: number; y: number } }) => {
      const node = evt.target;
      const title = node.data("extraTitle");
      const position = node.data("extraPosition");
      const ministry = node.data("extraMinistry");
      if (title || position || ministry) {
        const pos = evt.renderedPosition || { x: 0, y: 0 };
        setTooltip({ x: pos.x, y: pos.y, name: node.data("label"), title, position, ministry });
      }
    });
    cy.on("mouseout", "node", () => setTooltip(null));

    cyRef.current = cy;
  }, [graph, centerId, onNodeClick, onExpandNode]);

  useEffect(() => {
    initGraph();
    return () => {
      if (cyRef.current) {
        cyRef.current.destroy();
        cyRef.current = null;
      }
    };
  }, [initGraph]);

  return (
    <div className="relative w-full h-full">
      <div ref={containerRef} className="w-full h-full" />

      {/* Hover tooltip */}
      {tooltip && (
        <div
          className="absolute z-50 pointer-events-none bg-white border border-gray-200 rounded-lg shadow-lg px-3 py-2 text-sm"
          style={{ left: tooltip.x + 12, top: tooltip.y - 10, maxWidth: 320 }}
          dir="rtl"
        >
          <div className="font-semibold text-gray-900 mb-1">{tooltip.name}</div>
          {tooltip.title && (
            <div className="text-gray-600"><span className="text-gray-400">תואר: </span>{tooltip.title}</div>
          )}
          {tooltip.position && (
            <div className="text-gray-600"><span className="text-gray-400">תפקיד: </span>{tooltip.position}</div>
          )}
          {tooltip.ministry && (
            <div className="text-gray-600"><span className="text-gray-400">משרד: </span>{tooltip.ministry}</div>
          )}
        </div>
      )}

      {/* Legend */}
      <div className="absolute bottom-3 start-3 flex flex-col gap-2 text-xs bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-sm border border-gray-200">
        {/* Node types */}
        <div className="flex flex-wrap gap-2">
          {Object.entries(NODE_COLORS).map(([type, color]) => (
            <div key={type} className="flex items-center gap-1">
              <span className="w-3 h-3 rounded-full inline-block" style={{ backgroundColor: color }} />
              <span>
                {type === "person" ? "אנשים" :
                 type === "company" ? "חברות" :
                 type === "association" ? "עמותות" : "תחומים"}
              </span>
            </div>
          ))}
        </div>
        {/* Connection types */}
        <div className="flex gap-4 border-t border-gray-200 pt-2">
          <div className="flex items-center gap-1.5">
            <svg width="24" height="8" className="shrink-0">
              <line x1="0" y1="4" x2="24" y2="4" stroke="#9CA3AF" strokeWidth="1.5" strokeDasharray="4 2" />
            </svg>
            <span>ללא מגבלה</span>
          </div>
          <div className="flex items-center gap-1.5">
            <svg width="24" height="8" className="shrink-0">
              <line x1="0" y1="4" x2="24" y2="4" stroke="#044E66" strokeWidth="3" />
            </svg>
            <span>עם מגבלה</span>
          </div>
        </div>
      </div>
    </div>
  );
}
