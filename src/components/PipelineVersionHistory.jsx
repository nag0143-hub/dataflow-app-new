import { useState, useEffect } from "react";
import { dataflow } from '@/api/client';
import { GitCommitHorizontal, ChevronDown, ChevronUp, RotateCcw, Clock, User } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import moment from "moment";
import { toast } from "sonner";

const changeTypeColors = {
  created: "bg-emerald-100 text-emerald-700",
  updated: "bg-blue-100 text-blue-700",
  paused: "bg-amber-100 text-amber-700",
  resumed: "bg-blue-100 text-blue-700",
  deleted: "bg-red-100 text-red-700",
};

export default function PipelineVersionHistory({ job, onRestore }) {
  const [versions, setVersions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState(null);
  const [restoring, setRestoring] = useState(false);

  useEffect(() => {
    if (!job?.id) return;
    dataflow.entities.PipelineVersion.filter({ job_id: job.id }, "-version_number", 50)
      .then(data => { setVersions(data); setLoading(false); });
  }, [job?.id]);

  const handleRestore = async (version) => {
    if (!confirm(`Restore pipeline to v${version.version_number}?`)) return;
    setRestoring(true);
    const snapshot = version.snapshot;
    await dataflow.entities.Pipeline.update(job.id, {
      name: snapshot.name,
      description: snapshot.description,
      source_connection_id: snapshot.source_connection_id,
      target_connection_id: snapshot.target_connection_id,
      selected_objects: snapshot.selected_objects,
      schedule_type: snapshot.schedule_type,
      cron_expression: snapshot.cron_expression,
      retry_config: snapshot.retry_config,
    });
    // log restore as new version
    await dataflow.entities.PipelineVersion.create({
      job_id: job.id,
      version_number: (versions[0]?.version_number || 0) + 1,
      label: `Restored from v${version.version_number}`,
      commit_message: `Restored to v${version.version_number}: "${version.commit_message || version.label || ""}"`,
      snapshot: snapshot,
      change_type: "updated",
    });
    toast.success(`Restored to v${version.version_number}`);
    setRestoring(false);
    onRestore?.();
  };

  if (loading) return <div className="py-6 text-center text-slate-400 text-sm">Loading history...</div>;
  if (versions.length === 0) return <div className="py-6 text-center text-slate-400 text-sm">No version history yet.</div>;

  return (
    <div className="space-y-2">
      {versions.map((v, idx) => (
        <div key={v.id} className="border border-slate-200 rounded-xl overflow-hidden">
          <button
            className="w-full flex items-center gap-3 px-4 py-3 bg-white hover:bg-slate-50 transition-colors text-left"
            onClick={() => setExpanded(expanded === v.id ? null : v.id)}
          >
            <GitCommitHorizontal className="w-4 h-4 text-slate-400 shrink-0" />
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-sm font-medium text-slate-800">v{v.version_number}</span>
                {v.label && <span className="text-xs text-slate-500">{v.label}</span>}
                <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${changeTypeColors[v.change_type] || "bg-slate-100 text-slate-600"}`}>
                  {v.change_type}
                </span>
                {idx === 0 && <span className="text-xs bg-[#0060AF] text-white px-2 py-0.5 rounded-full">current</span>}
              </div>
              {v.commit_message && <p className="text-xs text-slate-500 mt-0.5 truncate">{v.commit_message}</p>}
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <span className="text-xs text-slate-400">{moment(v.created_date).fromNow()}</span>
              {expanded === v.id ? <ChevronUp className="w-3.5 h-3.5 text-slate-400" /> : <ChevronDown className="w-3.5 h-3.5 text-slate-400" />}
            </div>
          </button>

          {expanded === v.id && (
            <div className="border-t border-slate-100 bg-slate-50 px-4 py-3 space-y-3">
              <div className="grid grid-cols-2 gap-3 text-xs">
                <div className="flex items-center gap-1.5 text-slate-500">
                  <Clock className="w-3.5 h-3.5" />
                  {moment(v.created_date).format("MMM D, YYYY [at] HH:mm")}
                </div>
                {v.changed_by && (
                  <div className="flex items-center gap-1.5 text-slate-500">
                    <User className="w-3.5 h-3.5" />
                    {v.changed_by}
                  </div>
                )}
              </div>
              <div className="bg-white border border-slate-200 rounded-lg p-3">
                <p className="text-xs text-slate-500 mb-2 font-medium uppercase tracking-wide">Snapshot</p>
                <div className="grid grid-cols-2 gap-1.5 text-xs text-slate-600">
                  <span className="text-slate-400">Name:</span><span>{v.snapshot?.name}</span>
                  <span className="text-slate-400">Schedule:</span><span>{v.snapshot?.schedule_type || "manual"}</span>
                  <span className="text-slate-400">Objects:</span><span>{(v.snapshot?.selected_objects || []).length} selected</span>
                  {v.snapshot?.cron_expression && <><span className="text-slate-400">Cron:</span><span className="font-mono">{v.snapshot.cron_expression}</span></>}
                </div>
              </div>
              {idx !== 0 && (
                <Button
                  size="sm"
                  variant="outline"
                  className="gap-1.5 text-xs"
                  onClick={() => handleRestore(v)}
                  disabled={restoring}
                >
                  <RotateCcw className="w-3.5 h-3.5" />
                  Restore this version
                </Button>
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}