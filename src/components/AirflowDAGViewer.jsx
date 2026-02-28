import { useState, useEffect } from "react";
import { dataflow } from '@/api/client';
import { Play, Pause, RefreshCw, ExternalLink, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { toast } from "sonner";

const API = '/api/airflow';

export default function AirflowDAGViewer({ dag, airflowConnection }) {
  const [loading, setLoading] = useState(false);
  const [triggering, setTriggering] = useState(false);

  const connectionId = airflowConnection?.id;

  const handleTriggerDAG = async () => {
    if (!connectionId) {
      toast.error("Airflow connection not configured");
      return;
    }

    setTriggering(true);
    try {
      const response = await fetch(`${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}/dagRuns`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          conf: {}
        })
      });

      if (response.ok) {
        toast.success(`DAG "${dag.dag_name}" triggered successfully`);
        await dataflow.entities.AirflowDAG.update(dag.id, {
          last_run_date: new Date().toISOString(),
          last_run_status: "running"
        });
      } else {
        toast.error("Failed to trigger DAG");
      }
    } catch (err) {
      toast.error(`Error: ${err.message}`);
    } finally {
      setTriggering(false);
    }
  };

  const handleSyncDAG = async () => {
    if (!connectionId) {
      toast.error("Airflow connection not configured");
      return;
    }

    setLoading(true);
    try {
      const dagResponse = await fetch(`${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}`);

      if (dagResponse.ok) {
        const dagData = await dagResponse.json();

        const tasksResponse = await fetch(`${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}/tasks`);
        const tasksData = tasksResponse.ok ? await tasksResponse.json() : { tasks: [] };

        await dataflow.entities.AirflowDAG.update(dag.id, {
          dag_name: dagData.dag_id,
          is_paused: dagData.is_paused,
          task_count: tasksData.tasks?.length || 0,
          dag_structure: tasksData,
          last_sync: new Date().toISOString(),
          status: dagData.is_paused ? "paused" : "active"
        });

        toast.success("DAG synced successfully");
      } else {
        toast.error("Failed to sync DAG");
      }
    } catch (err) {
      toast.error(`Error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card className="border-slate-200">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-lg">{dag.dag_name}</CardTitle>
            <p className="text-xs text-slate-500 mt-1">{dag.dag_id}</p>
          </div>
          <div className="flex items-center gap-2">
            {dag.is_paused && (
              <span className="px-2 py-1 bg-amber-100 text-amber-800 text-xs font-medium rounded">
                Paused
              </span>
            )}
            {dag.status === "error" && (
              <AlertCircle className="w-4 h-4 text-red-500" />
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {dag.description && (
          <p className="text-sm text-slate-600">{dag.description}</p>
        )}

        <div className="grid grid-cols-2 gap-4 text-sm">
          <div>
            <p className="text-slate-500">Owner</p>
            <p className="font-medium">{dag.owner || "N/A"}</p>
          </div>
          <div>
            <p className="text-slate-500">Tasks</p>
            <p className="font-medium">{dag.task_count || 0}</p>
          </div>
          <div>
            <p className="text-slate-500">Schedule</p>
            <p className="font-medium text-xs">{dag.schedule || dag.schedule_interval || "Manual"}</p>
          </div>
          <div>
            <p className="text-slate-500">Last Run</p>
            <p className="font-medium">{dag.last_run_status ? dag.last_run_status.toUpperCase() : "None"}</p>
          </div>
        </div>

        {dag.dag_structure?.tasks && (
          <div className="border-t pt-3">
            <p className="text-xs font-semibold text-slate-700 mb-2">Tasks</p>
            <div className="space-y-1">
              {dag.dag_structure.tasks.slice(0, 5).map((task) => (
                <div key={task.task_id} className="text-xs text-slate-600 flex items-center gap-2">
                  <span className="w-1.5 h-1.5 bg-blue-400 rounded-full" />
                  {task.task_id}
                </div>
              ))}
              {dag.dag_structure.tasks.length > 5 && (
                <p className="text-xs text-slate-400">+{dag.dag_structure.tasks.length - 5} more tasks</p>
              )}
            </div>
          </div>
        )}

        <div className="flex gap-2 pt-2">
          <Button
            size="sm"
            variant="outline"
            className="flex-1 gap-1.5 text-xs"
            onClick={handleSyncDAG}
            disabled={loading}
          >
            <RefreshCw className="w-3.5 h-3.5" />
            {loading ? "Syncing..." : "Sync"}
          </Button>
          <Button
            size="sm"
            className="flex-1 gap-1.5 text-xs bg-emerald-600 hover:bg-emerald-700"
            onClick={handleTriggerDAG}
            disabled={triggering || dag.is_paused}
          >
            <Play className="w-3.5 h-3.5" />
            {triggering ? "Triggering..." : "Trigger Run"}
          </Button>
          {airflowConnection?.host && (
            <Button
              size="sm"
              variant="outline"
              className="gap-1.5 text-xs"
              asChild
            >
              <a
                href={`${airflowConnection.host}/dags/${dag.dag_id}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                <ExternalLink className="w-3.5 h-3.5" />
                View
              </a>
            </Button>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
