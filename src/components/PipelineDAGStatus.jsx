import { useState, useEffect, useCallback } from "react";
import { Wind, Play, Pause, RefreshCw, ExternalLink, CheckCircle2, XCircle, Clock, Loader2, AlertTriangle, Zap } from "lucide-react";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

const STATE_COLORS = {
  success: "bg-emerald-500",
  completed: "bg-emerald-500",
  failed: "bg-red-500",
  running: "bg-blue-500",
  queued: "bg-amber-500",
  up_for_retry: "bg-orange-500",
  up_for_reschedule: "bg-amber-400",
  skipped: "bg-slate-400",
  upstream_failed: "bg-rose-400",
  removed: "bg-slate-300",
  no_status: "bg-slate-300",
};

const STATE_LABELS = {
  success: "Success",
  completed: "Completed",
  failed: "Failed",
  running: "Running",
  queued: "Queued",
  up_for_retry: "Retry",
  up_for_reschedule: "Reschedule",
  skipped: "Skipped",
  upstream_failed: "Upstream Failed",
};

function StateDot({ state, size = "w-2 h-2" }) {
  return <span className={`inline-block rounded-full shrink-0 ${size} ${STATE_COLORS[state] || STATE_COLORS.no_status}`} />;
}

function formatDuration(seconds) {
  if (!seconds || seconds < 0) return "—";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function formatTime(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

export default function PipelineDAGStatus({ pipelineId, compact = false }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState(false);

  const fetchStatus = useCallback(async () => {
    try {
      setLoading(true);
      const resp = await fetch(`/api/airflow/pipeline/${pipelineId}/dag-status`);
      if (!resp.ok) throw new Error("Failed to fetch DAG status");
      const result = await resp.json();
      setData(result);
    } catch {
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [pipelineId]);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  const handleTrigger = async () => {
    setTriggering(true);
    try {
      const resp = await fetch(`/api/airflow/pipeline/${pipelineId}/trigger`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      const result = await resp.json();
      if (resp.ok && result.success) {
        toast.success(`DAG triggered on ${result.connection_name}`);
        setTimeout(fetchStatus, 2000);
      } else {
        toast.error(result.error || "Failed to trigger DAG");
      }
    } catch (err) {
      toast.error(err.message);
    } finally {
      setTriggering(false);
    }
  };

  if (compact) {
    return <CompactStatus data={data} loading={loading} />;
  }

  if (loading) {
    return (
      <div className="flex items-center gap-2 py-4 text-sm text-slate-400 dark:text-slate-500">
        <Loader2 className="w-4 h-4 animate-spin" />
        Loading Airflow status...
      </div>
    );
  }

  if (!data || !data.found) {
    return (
      <div className="rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 p-4">
        <div className="flex items-center gap-2 text-sm text-slate-500 dark:text-slate-400">
          <Wind className="w-4 h-4" />
          <span>Not deployed to Airflow</span>
        </div>
        {data?.dag_id && (
          <p className="text-xs text-slate-400 dark:text-slate-500 mt-1">
            Expected DAG ID: <span className="font-mono">{data.dag_id}</span>
          </p>
        )}
      </div>
    );
  }

  const taskStateMap = {};
  (data.task_states || []).forEach(ts => { taskStateMap[ts.task_id] = ts; });

  return (
    <div className="rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 overflow-hidden">
      <div className="px-4 py-3 border-b border-slate-100 dark:border-slate-700 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Wind className="w-4 h-4 text-[#0060AF]" />
          <span className="text-sm font-semibold text-slate-900 dark:text-white">Airflow DAG</span>
          <span className="font-mono text-xs text-slate-500 dark:text-slate-400">{data.dag_id}</span>
          {data.is_paused ? (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 font-medium">Paused</span>
          ) : (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300 font-medium">Active</span>
          )}
        </div>
        <div className="flex items-center gap-1.5">
          <Button size="sm" variant="outline" className="h-7 text-xs gap-1" onClick={fetchStatus}>
            <RefreshCw className="w-3 h-3" />
          </Button>
          <Button
            size="sm"
            className="h-7 text-xs gap-1 bg-emerald-600 hover:bg-emerald-700"
            onClick={handleTrigger}
            disabled={triggering || data.is_paused}
          >
            {triggering ? <Loader2 className="w-3 h-3 animate-spin" /> : <Zap className="w-3 h-3" />}
            {triggering ? "Triggering..." : "Trigger"}
          </Button>
          {data.airflow_host && (
            <a
              href={`${data.airflow_host}/dags/${data.dag_id}`}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 h-7 px-2 text-xs border rounded-md border-slate-200 dark:border-slate-600 hover:bg-slate-50 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300 font-medium"
            >
              <ExternalLink className="w-3 h-3" />
            </a>
          )}
        </div>
      </div>

      <div className="px-4 py-3 grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs border-b border-slate-100 dark:border-slate-700">
        <div>
          <p className="text-slate-400 dark:text-slate-500">Schedule</p>
          <p className="font-medium text-slate-700 dark:text-slate-300">{data.schedule || "Manual"}</p>
        </div>
        <div>
          <p className="text-slate-400 dark:text-slate-500">Instance</p>
          <p className="font-medium text-slate-700 dark:text-slate-300 truncate">{data.connection_name}</p>
        </div>
        <div>
          <p className="text-slate-400 dark:text-slate-500">Owners</p>
          <p className="font-medium text-slate-700 dark:text-slate-300">{(data.owners || []).join(", ") || "—"}</p>
        </div>
        <div>
          <p className="text-slate-400 dark:text-slate-500">Latest State</p>
          <div className="flex items-center gap-1.5">
            <StateDot state={data.latest_state} />
            <span className="font-medium text-slate-700 dark:text-slate-300 capitalize">{STATE_LABELS[data.latest_state] || data.latest_state || "None"}</span>
          </div>
        </div>
      </div>

      {data.runs?.length > 0 && (
        <div className="px-4 py-3 border-b border-slate-100 dark:border-slate-700">
          <p className="text-xs font-semibold text-slate-700 dark:text-slate-300 mb-2">Recent Runs</p>
          <div className="space-y-1">
            {data.runs.map(run => (
              <div key={run.dag_run_id} className="flex items-center gap-3 py-1.5 px-2 rounded hover:bg-slate-50 dark:hover:bg-slate-700/50 text-xs">
                <StateDot state={run.state} />
                <span className="font-mono text-slate-600 dark:text-slate-300 truncate flex-1 min-w-0">{run.dag_run_id}</span>
                <span className="text-slate-400 shrink-0">{formatTime(run.logical_date)}</span>
                <span className="text-slate-400 shrink-0 w-16 text-right">{formatDuration(run.duration)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {data.tasks?.length > 0 && (
        <div className="px-4 py-3">
          <p className="text-xs font-semibold text-slate-700 dark:text-slate-300 mb-2">
            Tasks ({data.tasks.length})
            {data.task_states?.length > 0 && (
              <span className="font-normal text-slate-400 ml-1">— latest run</span>
            )}
          </p>
          <div className="flex flex-wrap gap-1.5">
            {data.tasks.map(task => {
              const ts = taskStateMap[task.task_id];
              return (
                <div
                  key={task.task_id}
                  className="inline-flex items-center gap-1.5 px-2 py-1 rounded-lg bg-slate-50 dark:bg-slate-700 text-xs"
                  title={ts ? `${task.task_id}: ${ts.state}${ts.duration ? ` (${formatDuration(Math.round(ts.duration))})` : ""}` : task.task_id}
                >
                  {ts && <StateDot state={ts.state} size="w-1.5 h-1.5" />}
                  <span className="font-mono text-slate-600 dark:text-slate-300">{task.task_id}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

function CompactStatus({ data, loading }) {
  if (loading) {
    return <Loader2 className="w-3.5 h-3.5 animate-spin text-slate-400" />;
  }

  if (!data || !data.found) {
    return null;
  }

  const latestState = data.latest_state;
  if (!latestState) return null;

  const icons = {
    success: <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />,
    completed: <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />,
    failed: <XCircle className="w-3.5 h-3.5 text-red-500" />,
    running: <Loader2 className="w-3.5 h-3.5 text-blue-500 animate-spin" />,
    queued: <Clock className="w-3.5 h-3.5 text-amber-500" />,
  };

  const stateLabel = STATE_LABELS[latestState] || latestState;

  if (data.airflow_host && data.dag_id) {
    return (
      <a
        href={`${data.airflow_host}/dags/${data.dag_id}`}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1 hover:opacity-80 transition-opacity"
        title={`Open in Airflow — ${stateLabel}`}
        onClick={e => e.stopPropagation()}
      >
        <Wind className="w-3 h-3 text-slate-400" />
        {icons[latestState] || <AlertTriangle className="w-3.5 h-3.5 text-slate-400" />}
        <ExternalLink className="w-2.5 h-2.5 text-slate-400" />
      </a>
    );
  }

  return (
    <div className="flex items-center gap-1" title={`Airflow DAG: ${stateLabel}`}>
      <Wind className="w-3 h-3 text-slate-400" />
      {icons[latestState] || <AlertTriangle className="w-3.5 h-3.5 text-slate-400" />}
    </div>
  );
}
