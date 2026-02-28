import { useState, useEffect, useMemo, useCallback } from "react";
import { dataflow } from '@/api/client';
import { Link } from "react-router-dom";
import { createPageUrl } from "@/utils";
import useAirflowStatusSync from "@/hooks/useAirflowStatusSync";
import { 
  Cable, 
  Play, 
  CheckCircle2, 
  XCircle, 
  Clock,
  ArrowRight,
  Activity,
  RefreshCw,
  BookOpen,
  Search
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import StatCard from "@/components/StatCard";
import StatusBadge from "@/components/StatusBadge";
import { useTenant } from "@/components/useTenant";
import SkeletonLoader from "@/components/SkeletonLoader";
import ErrorBoundary from "@/components/ErrorBoundary";
import ErrorState from "@/components/ErrorState";
import { createIndex } from "@/components/dataIndexing";
import moment from "moment";

import OrchestrationPanel from "@/components/OrchestrationPanel";
import { Input } from "@/components/ui/input";

export default function Dashboard() {
  const { scope } = useTenant();
  const [connections, setConnections] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [runs, setRuns] = useState([]);
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState("");


  useEffect(() => {
    loadData();
  }, []);

  useAirflowStatusSync(useCallback(() => {
    loadData();
  }, []));

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [connectionsData, jobsData, runsData, logsData] = await Promise.all([
        dataflow.entities.Connection.list(),
        dataflow.entities.Pipeline.list(),
        dataflow.entities.PipelineRun.list("-created_date", 50),
        dataflow.entities.ActivityLog.list("-created_date", 10),
      ]);
      setConnections(scope(connectionsData));
      setJobs(scope(jobsData));
      setRuns(runsData);
      setLogs(logsData);
    } catch (err) {
      console.error("[Dashboard] loadData error:", err);
      setError(err?.message || "Failed to load dashboard data");
    } finally {
      setLoading(false);
    }
  };

  const stats = {
    totalConnections: connections.length,
    activeConnections: connections.filter(c => c.status === "active").length,
    totalJobs: jobs.length,
    runningJobs: jobs.filter(j => j.status === "running").length,
    failedJobs: jobs.filter(j => j.status === "failed").length,
    completedRuns: runs.filter(r => r.status === "completed").length,
    failedRuns: runs.filter(r => r.status === "failed").length,
  };

  const jobIndex = createIndex(jobs, "id");
  
  const filteredRuns = useMemo(() => {
    const recentRuns = runs.slice(0, 50);
    if (!searchTerm.trim()) return recentRuns;
    const term = searchTerm.toLowerCase();
    return recentRuns.filter(run => {
      const job = jobIndex.get(run.pipeline_id);
      return job?.name?.toLowerCase().includes(term) || run.status?.toLowerCase().includes(term);
    });
  }, [runs, searchTerm, jobIndex]);

  if (loading) {
    return (
      <div className="space-y-8 max-w-7xl mx-auto">
        <div className="space-y-2">
          <div className="h-8 bg-slate-200 dark:bg-slate-700 rounded w-48 animate-pulse" />
          <div className="h-4 bg-slate-200 dark:bg-slate-700 rounded w-96 animate-pulse" />
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-24 bg-slate-200 dark:bg-slate-700 rounded-lg animate-pulse" />
          ))}
        </div>
        <SkeletonLoader count={5} height="h-16" />
      </div>
    );
  }

  if (error) {
    return <ErrorState title="Failed to load dashboard" message={error} onRetry={loadData} />;
  }

  return (
    <ErrorBoundary>
      <div className="space-y-8 max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-foreground tracking-tight">Dashboard</h1>
            <p className="text-muted-foreground mt-0.5">Monitor your data pipelines and connections</p>
          </div>
          <Button variant="outline" size="sm" onClick={loadData} className="gap-2">
            <RefreshCw className="w-4 h-4" />
            Refresh
          </Button>
        </div>

        {/* Stats Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard 
            title="Total Connections" 
            value={stats.totalConnections}
            subtitle={`${stats.activeConnections} active`}
            icon={Cable}
            variant="blue"
          />
          <StatCard 
            title="Data Pipelines" 
            value={stats.totalJobs}
            subtitle={stats.runningJobs > 0 ? `${stats.runningJobs} running` : stats.failedJobs > 0 ? `${stats.failedJobs} failed` : `${stats.totalJobs} configured`}
            icon={Play}
            variant="amber"
          />
          <StatCard 
            title="Successful Runs" 
            value={stats.completedRuns}
            icon={CheckCircle2}
            variant="green"
          />
          <StatCard 
            title="Failed Runs" 
            value={stats.failedRuns}
            icon={XCircle}
            variant="red"
          />
        </div>

        {/* Recent Pipeline Runs + Activity */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Recent Pipeline Runs + Airflow */}
          <Card className="lg:col-span-2">
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between mb-3">
                <CardTitle className="text-lg font-semibold flex items-center gap-2">
                  <Play className="w-5 h-5 text-[#0060AF]" />
                  Recent Pipeline Runs
                </CardTitle>
                <Link to={createPageUrl("Pipelines")}>
                  <Button variant="outline" size="sm" className="gap-1">
                    View All Pipelines
                    <ArrowRight className="w-4 h-4" />
                  </Button>
                </Link>
              </div>
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-slate-400" />
                <Input
                  placeholder="Search by pipeline name or status..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10 h-9 text-sm"
                />
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {filteredRuns.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b border-slate-100 dark:border-slate-700">
                          <th className="text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider py-3 px-4">Pipeline</th>
                          <th className="text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider py-3 px-4">Pipeline Status</th>
                          <th className="text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider py-3 px-4">Run Status</th>
                          <th className="text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider py-3 px-4">Rows</th>
                          <th className="text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider py-3 px-4">Duration</th>
                          <th className="text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider py-3 px-4">Started</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredRuns.map((run) => {
                          const job = jobIndex.get(run.pipeline_id);
                          return (
                            <tr key={run.id} className="border-b border-slate-50 dark:border-slate-700/50 hover:bg-slate-50/50 dark:hover:bg-slate-700/30 transition-colors">
                              <td className="py-3 px-4">
                                <span className="font-medium text-slate-900 dark:text-slate-100">{job?.name || "Unknown Pipeline"}</span>
                              </td>
                              <td className="py-3 px-4">
                                {job && <StatusBadge status={job.status} size="sm" />}
                              </td>
                              <td className="py-3 px-4">
                                <StatusBadge status={run.status} size="sm" />
                              </td>
                              <td className="py-3 px-4 text-sm text-slate-600 dark:text-slate-300">
                                {(run.rows_processed || 0).toLocaleString()}
                              </td>
                              <td className="py-3 px-4 text-sm text-slate-600 dark:text-slate-300">
                                {run.duration_seconds ? `${run.duration_seconds}s` : "-"}
                              </td>
                              <td className="py-3 px-4 text-sm text-slate-500 dark:text-slate-400">
                                {moment(run.started_at || run.created_date).fromNow()}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="text-center py-8">
                    <Clock className="w-12 h-12 text-slate-300 dark:text-slate-600 mx-auto mb-3" />
                    <p className="text-slate-500 dark:text-slate-400 font-medium">
                      {searchTerm ? "No pipeline runs match your search" : "No pipeline runs yet"}
                    </p>
                    {!searchTerm && (
                      <p className="text-sm text-slate-400 dark:text-slate-500 mt-1">
                        Run a pipeline from the{" "}
                        <Link to={createPageUrl("Pipelines")} className="text-[#0060AF] hover:underline">Pipelines</Link>
                        {" "}page to see results here.
                      </p>
                    )}
                  </div>
                )}
                <div className="border-t border-slate-100 dark:border-slate-700 pt-4">
                  <h3 className="text-sm font-semibold text-slate-900 dark:text-white mb-3">Orchestration</h3>
                  <OrchestrationPanel variant="summary" />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Recent Activity */}
          <Card>
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-lg font-semibold flex items-center gap-2">
                  <Activity className="w-5 h-5 text-emerald-600" />
                  Recent Activity
                </CardTitle>
                <Link to={createPageUrl("ActivityLogs")}>
                  <Button variant="ghost" size="sm" className="text-[#0060AF] hover:text-[#004d8c]">
                    View All
                  </Button>
                </Link>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {logs.length > 0 ? logs.map((log) => (
                  <div key={log.id} className="flex items-start gap-3 p-2 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
                    <div className={`w-2 h-2 rounded-full mt-2 flex-shrink-0 ${
                      log.log_type === 'error' ? 'bg-red-500' :
                      log.log_type === 'warning' ? 'bg-amber-500' :
                      log.log_type === 'success' ? 'bg-emerald-500' :
                      'bg-blue-500'
                    }`} />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm text-slate-700 dark:text-slate-200 truncate">{log.message}</p>
                      <p className="text-xs text-slate-400 dark:text-slate-500">{moment(log.created_date).fromNow()}</p>
                    </div>
                  </div>
                )) : (
                  <p className="text-sm text-slate-400 text-center py-4">No activity yet</p>
                )}
              </div>
            </CardContent>
          </Card>
        </div>


      </div>
    </ErrorBoundary>
  );
}