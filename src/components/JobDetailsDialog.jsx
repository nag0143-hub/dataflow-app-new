import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import StatusBadge from "@/components/StatusBadge";
import PipelineDAGStatus from "@/components/PipelineDAGStatus";
import moment from "moment";

export default function JobDetailsDialog({
  open,
  onOpenChange,
  job,
  jobRuns,
}) {
  if (!job) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-3">
            {job.name}
            {job && <StatusBadge status={job.status} size="sm" />}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-6">
          {/* Job Details */}
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <span className="text-slate-500">Total Runs</span>
              <p className="font-medium">{job.total_runs || 0}</p>
            </div>
            <div>
              <span className="text-slate-500">Success Rate</span>
              <p className="font-medium text-emerald-600">
                {job.total_runs
                  ? Math.round((job.successful_runs || 0) / job.total_runs * 100)
                  : 0}%
              </p>
            </div>
            <div>
              <span className="text-slate-500">Last Run</span>
              <p className="font-medium">
                {job.last_run ? moment(job.last_run).format("MMM D, YYYY h:mm A") : "Never"}
              </p>
            </div>
            <div>
              <span className="text-slate-500">Schedule</span>
              <p className="font-medium capitalize">{job.schedule_type}</p>
            </div>
          </div>

          {/* Selected Datasets */}
          <div>
            <h4 className="font-medium text-slate-900 mb-2">Selected Datasets ({job.selected_datasets?.length || 0})</h4>
            <div className="border border-slate-200 rounded-lg max-h-40 overflow-y-auto">
              {job.selected_datasets?.length > 0 ? (
                <table className="w-full text-sm">
                  <thead className="bg-slate-50 sticky top-0">
                    <tr>
                      <th className="text-left px-3 py-2 font-medium text-slate-600">Schema</th>
                      <th className="text-left px-3 py-2 font-medium text-slate-600">Table</th>
                      <th className="text-left px-3 py-2 font-medium text-slate-600">Target Path</th>
                    </tr>
                  </thead>
                  <tbody>
                    {job.selected_datasets.map((obj, i) => (
                      <tr key={i} className="border-t border-slate-100">
                        <td className="px-3 py-2 text-slate-600">{obj.schema}</td>
                        <td className="px-3 py-2 text-slate-900">{obj.table}</td>
                        <td className="px-3 py-2 text-slate-500">{obj.target_path}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <p className="p-4 text-center text-slate-500">No datasets selected</p>
              )}
            </div>
          </div>

          {/* Run History */}
          <div>
            <h4 className="font-medium text-slate-900 mb-2">Run History</h4>
            <div className="border border-slate-200 rounded-lg max-h-60 overflow-y-auto">
              {jobRuns.length > 0 ? (
                <table className="w-full text-sm">
                  <thead className="bg-slate-50 sticky top-0">
                    <tr>
                      <th className="text-left px-3 py-2 font-medium text-slate-600">Run #</th>
                      <th className="text-left px-3 py-2 font-medium text-slate-600">Status</th>
                      <th className="text-left px-3 py-2 font-medium text-slate-600">Rows</th>
                      <th className="text-left px-3 py-2 font-medium text-slate-600">Duration</th>
                      <th className="text-left px-3 py-2 font-medium text-slate-600">Started</th>
                    </tr>
                  </thead>
                  <tbody>
                    {jobRuns.map((run) => (
                      <tr key={run.id} className="border-t border-slate-100">
                        <td className="px-3 py-2">#{run.run_number}</td>
                        <td className="px-3 py-2">
                          <StatusBadge status={run.status} size="sm" />
                        </td>
                        <td className="px-3 py-2">{(run.rows_processed || 0).toLocaleString()}</td>
                        <td className="px-3 py-2">{run.duration_seconds ? `${run.duration_seconds}s` : "-"}</td>
                        <td className="px-3 py-2 text-slate-500">
                          {moment(run.started_at || run.created_date).format("MMM D, h:mm A")}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <p className="p-4 text-center text-slate-500">No runs yet</p>
              )}
            </div>
          </div>
          <div>
            <h4 className="font-medium text-slate-900 dark:text-white mb-2">Airflow Orchestration</h4>
            <PipelineDAGStatus pipelineId={job.id} />
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}