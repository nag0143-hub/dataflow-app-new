import { memo, useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import StatusBadge from "@/components/StatusBadge";
import PlatformIcon from "@/components/PlatformIcon";
import { Eye, Play, MoreVertical, FileJson, RotateCcw, Pause, Copy, Edit, Trash2, ArrowRight, GitBranch, Tag } from "lucide-react";
import PipelineDAGStatus from "@/components/PipelineDAGStatus";
import moment from "moment";
import GitCheckinDialog from "@/components/GitCheckinDialog";

const PipelineCard = memo(function PipelineCard({
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
  onViewHistory,
  onExport,
}) {
  const lastRun = jobRuns[0];
  const [gitCheckinOpen, setGitCheckinOpen] = useState(false);

  return (
    <div>
    <GitCheckinDialog
      open={gitCheckinOpen}
      onOpenChange={setGitCheckinOpen}
      pipelineData={{ ...job, _isUpdate: true }}
      connections={connections || []}
    />
    <Card className="border-slate-200 dark:bg-slate-800 dark:border-slate-700 hover:shadow-lg transition-shadow">
      <CardContent className="p-5">
        <div className="flex flex-col lg:flex-row lg:items-center gap-4">
          {/* Pipeline Info */}
           <div className="flex-1 min-w-0">
            <div className="flex items-center gap-3 mb-2">
              <h3 className="font-semibold text-slate-900 dark:text-white text-lg">{job.name}</h3>
              <StatusBadge status={job.status} size="sm" />
              <PipelineDAGStatus pipelineId={job.id} compact />
            </div>
            {job.description && (
              <p className="text-sm text-slate-500 dark:text-slate-400 mb-3">{job.description}</p>
            )}

            {/* Connection Flow */}
            <div className="flex items-center gap-2 flex-wrap">
              {sourceConn && (
                <div className="flex items-center gap-2 bg-slate-50 dark:bg-slate-700 rounded-lg px-3 py-1.5">
                  <PlatformIcon platform={sourceConn.platform} size="sm" />
                  <span className="text-sm text-slate-600 dark:text-slate-300">{sourceConn.name}</span>
                </div>
              )}
              <ArrowRight className="w-4 h-4 text-slate-400" />
              {targetConn && (
                <div className="flex items-center gap-2 bg-slate-50 dark:bg-slate-700 rounded-lg px-3 py-1.5">
                  <PlatformIcon platform={targetConn.platform} size="sm" />
                  <span className="text-sm text-slate-600 dark:text-slate-300">{targetConn.name}</span>
                </div>
              )}
            </div>

            {job.tags?.length > 0 && (
              <div className="mt-2 flex flex-wrap gap-1">
                {job.tags.map(tag => (
                  <span key={tag} className="inline-flex items-center px-2 py-0.5 rounded-full text-xs bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border border-blue-100 dark:border-blue-800">
                    <Tag className="w-3 h-3 mr-1" />
                    {tag}
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Stats */}
          <div className="flex items-center gap-6 text-sm">
            <div className="text-center">
              <p className="text-slate-400 dark:text-slate-500">Datasets</p>
              <p className="font-semibold text-slate-900 dark:text-white">{job.selected_datasets?.length || 0}</p>
            </div>
            <div className="text-center">
              <p className="text-slate-400 dark:text-slate-500">Runs</p>
              <p className="font-semibold text-slate-900 dark:text-white">{job.total_runs || 0}</p>
            </div>
            <div className="text-center">
              <p className="text-slate-400 dark:text-slate-500">Success</p>
              <p className="font-semibold text-emerald-600">
                {job.total_runs ? Math.round((job.successful_runs || 0) / job.total_runs * 100) : 0}%
              </p>
            </div>
            {job.sla_config?.enabled && (
              <div className="text-center">
                <p className="text-slate-400 dark:text-slate-500">SLA</p>
                <p className={`font-semibold ${job.sla_compliance_rate >= 95 ? 'text-emerald-600' : job.sla_compliance_rate >= 80 ? 'text-amber-600' : 'text-red-600'}`}>
                  {job.sla_compliance_rate || 0}%
                </p>
              </div>
            )}
            {job.last_run && (
              <div className="text-center">
                <p className="text-slate-400 dark:text-slate-500">Last Run</p>
                <p className="text-slate-600 dark:text-slate-300">{moment(job.last_run).fromNow()}</p>
              </div>
            )}
          </div>

          {/* Actions */}
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              onClick={() => onRun(job)}
              disabled={job.status === "running" || job.status === "paused"}
              className="gap-1.5 bg-emerald-600 hover:bg-emerald-700"
            >
              <Play className="w-4 h-4" />
              Run
            </Button>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="icon" className="h-8 w-8">
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
      </CardContent>
    </Card>
    </div>
    );
    });

    export default PipelineCard;