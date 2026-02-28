import { memo, useState } from "react";
import { Button } from "@/components/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import StatusBadge from "@/components/StatusBadge";
import PlatformIcon from "@/components/PlatformIcon";
import { Eye, Play, MoreVertical, FileJson, RotateCcw, Pause, Copy, Edit, Trash2, ArrowRight, GitBranch } from "lucide-react";
import PipelineDAGStatus from "@/components/PipelineDAGStatus";
import moment from "moment";
import GitCheckinDialog from "@/components/GitCheckinDialog";

const PipelineListRow = memo(function PipelineListRow({
  job,
  sourceConn,
  targetConn,
  jobRuns,
  connections,
  onEdit,
  onDelete,
  onRun,
  onRetry,
  onPause,
  onClone,
  onViewDetails,
  onExport,
}) {
  const [gitCheckinOpen, setGitCheckinOpen] = useState(false);

  return (
    <>
      <GitCheckinDialog
        open={gitCheckinOpen}
        onOpenChange={setGitCheckinOpen}
        pipelineData={{ ...job, _isUpdate: true }}
        connections={connections || []}
      />
      <div className="flex items-center gap-4 px-4 py-3 border-b border-slate-100 dark:border-slate-700/50 hover:bg-slate-50/50 dark:hover:bg-slate-700/30 transition-colors">
        <div className="w-52 min-w-0 shrink-0">
          <p className="font-medium text-slate-900 dark:text-white truncate text-sm">{job.name}</p>
        </div>

        <div className="w-24 shrink-0 flex items-center gap-1.5">
          <StatusBadge status={job.status} size="sm" />
          <PipelineDAGStatus pipelineId={job.id} compact />
        </div>

        <div className="flex-1 min-w-0 hidden md:flex items-center gap-1.5">
          {sourceConn && (
            <div className="flex items-center gap-1.5">
              <PlatformIcon platform={sourceConn.platform} size="sm" />
              <span className="text-xs text-slate-600 dark:text-slate-400 truncate max-w-[120px]">{sourceConn.name}</span>
            </div>
          )}
          <ArrowRight className="w-3 h-3 text-slate-400 shrink-0" />
          {targetConn && (
            <div className="flex items-center gap-1.5">
              <PlatformIcon platform={targetConn.platform} size="sm" />
              <span className="text-xs text-slate-600 dark:text-slate-400 truncate max-w-[120px]">{targetConn.name}</span>
            </div>
          )}
        </div>

        <div className="w-16 text-center hidden lg:block">
          <span className="text-sm text-slate-600 dark:text-slate-300">{job.selected_datasets?.length || 0}</span>
        </div>

        <div className="w-24 text-right hidden xl:block">
          <span className="text-xs text-slate-500 dark:text-slate-400">
            {job.last_run ? moment(job.last_run).fromNow() : "—"}
          </span>
        </div>

        <div className="flex items-center gap-1.5 shrink-0">
          <Button
            size="sm"
            onClick={() => onRun(job)}
            disabled={job.status === "running" || job.status === "paused"}
            className="gap-1 bg-emerald-600 hover:bg-emerald-700 h-7 text-xs px-2.5"
          >
            <Play className="w-3 h-3" />
            Run
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="icon" className="h-7 w-7">
                <MoreVertical className="w-4 h-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={() => onViewDetails(job)}>
                <Eye className="w-4 h-4 mr-2" />
                Details
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setGitCheckinOpen(true)}>
                <GitBranch className="w-4 h-4 mr-2" />
                Deploy
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => onExport(job)}>
                <FileJson className="w-4 h-4 mr-2" />
                Export
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={() => onRetry(job)} disabled={job.status !== "failed"}>
                <RotateCcw className="w-4 h-4 mr-2" />
                Retry Failed
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => onPause(job)}>
                <Pause className="w-4 h-4 mr-2" />
                {job.status === "paused" ? "Resume" : "Pause"}
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => onClone(job)}>
                <Copy className="w-4 h-4 mr-2" />
                Clone
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={() => onEdit(job)}>
                <Edit className="w-4 h-4 mr-2" />
                Edit
              </DropdownMenuItem>
              <DropdownMenuItem
                onClick={() => onDelete(job)}
                className="text-red-600 focus:text-red-600"
              >
                <Trash2 className="w-4 h-4 mr-2" />
                Delete
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </>
  );
});

export default PipelineListRow;
