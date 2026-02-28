import React from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export default function DependencySelector({ pipelines, selectedIds, currentPipelineId, onChange }) {
  const availablePipelines = pipelines.filter(p => p.id !== currentPipelineId);

  const handleToggle = (pipelineId) => {
    const newSelected = selectedIds.includes(pipelineId)
      ? selectedIds.filter(id => id !== pipelineId)
      : [...selectedIds, pipelineId];
    onChange(newSelected);
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Dependent Pipelines</CardTitle>
        <CardDescription>
          Select pipelines to trigger automatically when this pipeline completes successfully
        </CardDescription>
      </CardHeader>
      <CardContent>
        {availablePipelines.length === 0 ? (
          <p className="text-sm text-slate-500">No other pipelines available</p>
        ) : (
          <div className="space-y-3">
            {availablePipelines.map((pipeline) => (
              <div key={pipeline.id} className="flex items-center space-x-2">
                <Checkbox
                  id={`dep-${pipeline.id}`}
                  checked={selectedIds.includes(pipeline.id)}
                  onCheckedChange={() => handleToggle(pipeline.id)}
                />
                <Label
                  htmlFor={`dep-${pipeline.id}`}
                  className="text-sm font-medium cursor-pointer flex-1"
                >
                  {pipeline.name}
                </Label>
                {pipeline.description && (
                  <span className="text-xs text-slate-500">{pipeline.description}</span>
                )}
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}