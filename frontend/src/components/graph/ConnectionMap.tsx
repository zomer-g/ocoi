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

const EDGE_LABELS: Record<string, string> = {
  restricted_from: "מוגבל מ-",
  owns: "בעלות",
  manages: "מנהל",
  employed_by: "מועסק ב-",
  related_to: "קשור ל-",
  board_member: "חבר דירקטוריון",
  operates_in: "פועל בתחום",
  family_member: "בן משפחה",
};

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
        {
          selector: "node",
          style: {
            label: "data(label)",
            "text-valign": "bottom",
            "text-margin-y": 5,
            "font-size": 11,
            "font-family": "Arial, sans-serif",
            width: 35,
            height: 35,
            "background-color": (ele: { data: (key: string) => string }) =>
              NODE_COLORS[ele.data("type")] || "#6B7280",
            "border-width": (ele: { data: (key: string) => boolean }) =>
              ele.data("isCenter") ? 3 : 1,
            "border-color": (ele: { data: (key: string) => boolean }) =>
              ele.data("isCenter") ? "#1D4ED8" : "#E5E7EB",
          },
        },
        {
          selector: "edge",
          style: {
            label: "data(label)",
            "font-size": 9,
            "text-rotation": "autorotate",
            "curve-style": "bezier",
            width: 1.5,
            "line-color": "#D1D5DB",
            "target-arrow-color": "#D1D5DB",
            "target-arrow-shape": "triangle",
            "arrow-scale": 0.8,
          },
        },
        {
          selector: "node:selected",
          style: {
            "border-width": 3,
            "border-color": "#1D4ED8",
          },
        },
      ],
      layout: {
        name: "cose",
        animate: true,
        animationDuration: 500,
        nodeRepulsion: () => 8000,
        idealEdgeLength: () => 120,
        gravity: 0.3,
      },
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
      <div className="absolute bottom-3 start-3 flex gap-2 text-xs">
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <div key={type} className="flex items-center gap-1 bg-white/80 px-2 py-1 rounded">
            <span className="w-3 h-3 rounded-full inline-block" style={{ backgroundColor: color }} />
            <span>
              {type === "person" ? "אנשים" :
               type === "company" ? "חברות" :
               type === "association" ? "עמותות" : "תחומים"}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
