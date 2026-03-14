"use client";

import { useEffect, useState, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { ConnectionMap } from "@/components/graph/ConnectionMap";
import { ConnectionTable } from "@/components/graph/ConnectionTable";
import type { SubGraph } from "@/lib/api-client";

interface EntityData {
  id: string;
  name_hebrew: string;
  name_english?: string;
  title?: string;
  position?: string;
  ministry?: string;
  registration_number?: string;
  company_type?: string;
  status?: string;
}

function EntityContent() {
  const searchParams = useSearchParams();
  const id = searchParams.get("id") || "";
  const type = searchParams.get("type") || "person";

  const [entity, setEntity] = useState<EntityData | null>(null);
  const [graph, setGraph] = useState<SubGraph | null>(null);
  const [loading, setLoading] = useState(true);
  const [showTable, setShowTable] = useState(false);

  useEffect(() => {
    if (!id) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      setLoading(true);
      try {
        const plural =
          type === "person" ? "persons"
          : type === "company" ? "companies"
          : type === "association" ? "associations"
          : "domains";

        const [entityRes, graphRes] = await Promise.all([
          fetch(`/api/v1/${plural}/${id}`),
          fetch(`/api/v1/graph/neighbors/${id}?type=${type}&depth=1`),
        ]);

        const entityData = await entityRes.json();
        const graphData = await graphRes.json();

        setEntity(entityData.data);
        setGraph(graphData.data);
      } catch {
        // ignore
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [id, type]);

  if (loading) return <div className="text-center py-12 text-gray-400">טוען...</div>;
  if (!id || !entity) return <div className="text-center py-12 text-gray-400">לא נמצא</div>;

  return (
    <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="bg-white rounded-lg p-6 border border-gray-200 mb-6">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">{entity.name_hebrew}</h1>
        {entity.name_english && (
          <p className="text-gray-500" dir="ltr">{entity.name_english}</p>
        )}

        <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
          {entity.title && (
            <div><span className="text-gray-500">תואר:</span> {entity.title}</div>
          )}
          {entity.position && (
            <div><span className="text-gray-500">תפקיד:</span> {entity.position}</div>
          )}
          {entity.ministry && (
            <div><span className="text-gray-500">משרד:</span> {entity.ministry}</div>
          )}
          {entity.registration_number && (
            <div><span className="text-gray-500">מספר רישום:</span> {entity.registration_number}</div>
          )}
          {entity.company_type && (
            <div><span className="text-gray-500">סוג:</span> {entity.company_type}</div>
          )}
          {entity.status && (
            <div><span className="text-gray-500">סטטוס:</span> {entity.status}</div>
          )}
        </div>
      </div>

      {graph && (graph.nodes.length > 0 || graph.edges.length > 0) && (
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="flex items-center justify-between p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">מפת קשרים</h2>
            {graph.edges.length > 0 && (
              <button
                onClick={() => setShowTable((v) => !v)}
                className="px-3 py-1.5 text-sm border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
              >
                {showTable ? "הסתר טבלה" : "הצג כטבלה"}
              </button>
            )}
          </div>
          <div className="h-[500px]">
            <ConnectionMap
              graph={graph}
              centerId={id}
              centerType={type}
              onNodeClick={(nodeId, nodeType) => {
                window.location.href = `/entity?id=${nodeId}&type=${nodeType}`;
              }}
            />
          </div>
          {showTable && graph.edges.length > 0 && (
            <div className="p-4 border-t border-gray-200">
              <ConnectionTable
                edges={graph.edges}
                caption={`קשרים של ${entity?.name_hebrew || ""}`}
              />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default function EntityPage() {
  return (
    <Suspense fallback={<div className="text-center py-12 text-gray-400">טוען...</div>}>
      <EntityContent />
    </Suspense>
  );
}
