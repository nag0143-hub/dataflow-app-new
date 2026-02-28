import { useMemo } from "react";
import { ArrowRight } from "lucide-react";
import FlowNode from "./FlowNode";

export default function PipelineFlowDiagram({ 
  job, 
  connections,
  columnMappings = {},
  dqRules = {},
  dataCleansing = {},
  dataMasking = []
}) {
  const sourceConnection = useMemo(() => 
    connections?.find(c => c.id === job?.source_connection_id),
    [job, connections]
  );

  const targetConnection = useMemo(() => 
    connections?.find(c => c.id === job?.target_connection_id),
    [job, connections]
  );

  const transformationSteps = useMemo(() => {
    const steps = [];

    // Column mappings
    if (columnMappings && Object.keys(columnMappings).length > 0) {
      const mappingCount = Object.values(columnMappings).flat().filter(m => !m.is_audit).length;
      if (mappingCount > 0) {
        steps.push({
          type: "transformation",
          title: "Column Mapping",
          description: `Map ${mappingCount} source columns`,
          metadata: [{ label: "Columns", value: mappingCount }]
        });
      }
    }

    // Data quality rules
    if (dqRules && Object.keys(dqRules).length > 0) {
      const ruleCount = Object.values(dqRules).flat().length;
      if (ruleCount > 0) {
        steps.push({
          type: "transformation",
          title: "Data Quality",
          description: "Validate data against rules",
          metadata: [{ label: "Rules", value: ruleCount }]
        });
      }
    }

    // Data cleansing
    if (dataCleansing && Object.keys(dataCleansing).length > 0) {
      steps.push({
        type: "transformation",
        title: "Data Cleansing",
        description: "Apply cleansing transformations",
        metadata: [{ label: "Rules", value: Object.keys(dataCleansing).length }]
      });
    }

    // Data masking
    if (dataMasking && dataMasking.length > 0) {
      steps.push({
        type: "security",
        title: "Data Masking",
        description: "Apply masking to sensitive data",
        metadata: [{ label: "Columns", value: dataMasking.length }]
      });
    }

    return steps;
  }, [columnMappings, dqRules, dataCleansing, dataMasking]);

  if (!job || !sourceConnection || !targetConnection) {
    return (
      <div className="text-center py-8 text-slate-400 text-sm">
        Configure source, target, and mappings to visualize the pipeline
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Pipeline flow */}
      <div className="overflow-x-auto pb-2">
        <div className="flex items-center gap-3 min-w-max px-4 py-4 bg-slate-50 rounded-lg border border-slate-200">
          {/* Source */}
          <FlowNode
            type="source"
            title={sourceConnection.name}
            description={sourceConnection.platform}
            metadata={[
              { label: "Platform", value: sourceConnection.platform }
            ]}
          />

          <div className="flex items-center gap-1 text-slate-400">
            <ArrowRight className="w-5 h-5" />
          </div>

          {/* Transformation steps */}
          {transformationSteps.length > 0 ? (
            <>
              {transformationSteps.map((step, idx) => (
                <div key={idx} className="flex items-center gap-3">
                  <FlowNode
                    type={step.type}
                    title={step.title}
                    description={step.description}
                    metadata={step.metadata}
                  />
                  {idx < transformationSteps.length - 1 && (
                    <div className="flex items-center gap-1 text-slate-400">
                      <ArrowRight className="w-5 h-5" />
                    </div>
                  )}
                </div>
              ))}
              <div className="flex items-center gap-1 text-slate-400">
                <ArrowRight className="w-5 h-5" />
              </div>
            </>
          ) : (
            <div className="flex items-center gap-1 text-slate-400">
              <ArrowRight className="w-5 h-5" />
            </div>
          )}

          {/* Target */}
          <FlowNode
            type="target"
            title={targetConnection.name}
            description={targetConnection.platform}
            metadata={[
              { label: "Load Method", value: job.load_method || "append" }
            ]}
          />
        </div>
      </div>

      {/* Pipeline summary */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 px-4">
        <div className="p-3 bg-blue-50 rounded-lg border border-blue-100">
          <div className="text-xs text-blue-600 font-medium">Source</div>
          <div className="text-sm font-semibold text-blue-900 mt-1">{sourceConnection.name}</div>
        </div>
        
        <div className="p-3 bg-purple-50 rounded-lg border border-purple-100">
          <div className="text-xs text-purple-600 font-medium">Transformations</div>
          <div className="text-sm font-semibold text-purple-900 mt-1">{transformationSteps.length}</div>
        </div>
        
        <div className="p-3 bg-green-50 rounded-lg border border-green-100">
          <div className="text-xs text-green-600 font-medium">Target</div>
          <div className="text-sm font-semibold text-green-900 mt-1">{targetConnection.name}</div>
        </div>
        
        <div className="p-3 bg-slate-50 rounded-lg border border-slate-200">
          <div className="text-xs text-slate-600 font-medium">Load Method</div>
          <div className="text-sm font-semibold text-slate-900 mt-1 capitalize">{job.load_method || "append"}</div>
        </div>
      </div>

      {/* Detailed transformation breakdown */}
      {transformationSteps.length > 0 && (
        <div className="p-4 bg-slate-50 rounded-lg border border-slate-200">
          <h4 className="text-sm font-semibold text-slate-900 mb-3">Transformation Pipeline</h4>
          <div className="space-y-2">
            {transformationSteps.map((step, idx) => (
              <div key={idx} className="flex items-start gap-3 text-xs">
                <div className="flex items-center justify-center w-5 h-5 rounded-full bg-slate-300 text-white font-semibold text-xs flex-shrink-0">
                  {idx + 1}
                </div>
                <div className="flex-1">
                  <div className="font-medium text-slate-900">{step.title}</div>
                  <div className="text-slate-600">{step.description}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}