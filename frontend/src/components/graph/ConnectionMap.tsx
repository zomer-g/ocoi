"use client";

import { useEffect, useRef, useCallback } from "react";
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
      },
    }));

    const edges = graph.edges.map((edge, i) => ({
      data: {
        id: `e${i}`,
        source: edge.source_id,
        target: edge.target_id,
        label: EDGE_LABELS[edge.relationship_type] || edge.relationship_type,
        relType: edge.relationship_type,
      },
    }));

    // Filter edges to only include those connecting existing nodes
    const nodeIds = new Set(nodes.map((n) => n.data.id));
    const validEdges = edges.filter(
      (e) => nodeIds.has(e.data.source) && nodeIds.has(e.data.target)
    );

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
            "line-dash-pattern": [6, 3] as unknown as number,
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
