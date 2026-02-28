import { useState, useEffect, useCallback, useMemo } from "react";
import { dataflow } from '@/api/client';
import { Plus, Search, Play, Rocket, RefreshCw, Tag, Layers, X, LayoutGrid, List, ChevronLeft, ChevronRight } from "lucide-react";
import useAirflowStatusSync from "@/hooks/useAirflowStatusSync";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import EmptyStateGuide from "@/components/EmptyStateGuide";
import SkeletonLoader from "@/components/SkeletonLoader";
import ErrorBoundary from "@/components/ErrorBoundary";
import ErrorState from "@/components/ErrorState";
import { createIndex } from "@/components/dataIndexing";
import { createPageUrl } from "@/utils";
import { useTenant } from "@/components/useTenant";
import { useRetry } from "@/components/hooks/useRetry";
import { usePagination } from "@/components/hooks/usePagination";
import JobFormDialog from "@/components/JobFormDialog";
import PipelineCard from "@/components/PipelineCard";
import PipelineListRow from "@/components/PipelineListRow";
import JobDetailsDialog from "@/components/JobDetailsDialog";
import JobSpecExport from "@/components/JobSpecExport";
import OnboardingWizard from "@/components/OnboardingWizard";
import OrchestrationPanel from "@/components/OrchestrationPanel";

const defaultFormData = {
name: "",
description: "",
source_connection_id: "",
target_connection_id: "",
selected_datasets: [],
load_method: "append",
delivery_channel: "pull",
schedule_type: "manual",
cron_expression: "",
status: "draft",
use_custom_calendar: false,
include_calendar_id: "",
exclude_calendar_id: "",
column_mappings: {},
dq_rules: {},
data_cleansing: {},
data_masking_rules: [],
sla_config: {
  enabled: false,
  max_duration_minutes: 60,
  alert_threshold_percent: 80,
  escalation_enabled: false,
  escalation_email: ""
},
assignment_group: "",
cost_center: "",
email: "",
access_entitlements: [],
enable_advanced: false,
retry_config: {
  max_retries: 3,
  retry_delay_seconds: 60,
  exponential_backoff: true
},
dag_callable_base_path: "",
tags: []
};


export default function Pipelines() {
  const { user: currentUser, scope } = useTenant();
  const { retry } = useRetry();
  const { data: searchResults, fetchPage: fetchSearchResults, hasNextPage, loading: searchLoading, reset: resetSearch } = usePagination(50);
  const [pipelines, setPipelines] = useState([]);
  const [connections, setConnections] = useState([]);
  const [runs, setRuns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [filterStatus, setFilterStatus] = useState("all");
  const [dialogOpen, setDialogOpen] = useState(false);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [editingPipeline, setEditingPipeline] = useState(null);
  const [viewingPipeline, setViewingPipeline] = useState(null);
  const [formData, setFormData] = useState(defaultFormData);
  const [activeTab, setActiveTab] = useState("general");
  const [exportPipeline, setExportPipeline] = useState(null);
  const [showOnboarding, setShowOnboarding] = useState(false);
  const [groupByTag, setGroupByTag] = useState(false);
  const [viewMode, setViewMode] = useState("list");
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 25;

  useEffect(() => {
    loadData();
  }, []);

  useAirflowStatusSync(useCallback(() => {
    loadData();
  }, []));

  useEffect(() => {
    if (!loading && !error && pipelines.length === 0 && connections.length > 0) {
      const hasSeenOnboarding = localStorage.getItem("dataflow-onboarding-seen");
      if (!hasSeenOnboarding) {
        setShowOnboarding(true);
        localStorage.setItem("dataflow-onboarding-seen", "true");
      }
    }
  }, [loading, error, pipelines, connections]);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [pipelinesData, connectionsData, runsData] = await Promise.all([
        dataflow.entities.Pipeline.list(),
        dataflow.entities.Connection.list(),
        dataflow.entities.PipelineRun.list("-created_date", 100)
      ]);
      setPipelines(scope(pipelinesData));
      setConnections(scope(connectionsData));
      setRuns(runsData);
    } catch (err) {
      console.error("[Pipelines] loadData error:", err);
      setError(err?.message || "Failed to load pipelines");
    } finally {
      setLoading(false);
    }
  };

  const connectionIndex = useMemo(() => createIndex(connections, "id"), [connections]);
  const runsByPipeline = useMemo(() => runs.reduce((acc, run) => {
    if (!acc[run.pipeline_id]) acc[run.pipeline_id] = [];
    acc[run.pipeline_id].push(run);
    return acc;
  }, {}), [runs]);

  const getConnection = useCallback((id) => connectionIndex.get(id), [connectionIndex]);
  const getPipelineRuns = useCallback((pipelineId) => runsByPipeline[pipelineId] || [], [runsByPipeline]);

  // Use server-side search when search term is provided, otherwise filter client-side
  const displayPipelines = useMemo(() => {
    if (searchTerm.trim()) {
      const term = searchTerm.trim().toLowerCase();
      const base = searchResults.length > 0 ? searchResults : pipelines;
      return base.filter(p => {
        const matchesStatus = filterStatus === "all" || p.status === filterStatus;
        const matchesSearch =
          (p.name || "").toLowerCase().includes(term) ||
          (p.status || "").toLowerCase().includes(term) ||
          (p.tags || []).some(tag => tag.toLowerCase().includes(term));
        return matchesStatus && matchesSearch;
      });
    }
    return pipelines.filter(p => filterStatus === "all" || p.status === filterStatus);
  }, [searchTerm, searchResults, pipelines, filterStatus]);

  const statusCounts = useMemo(() => {
    const counts = { all: pipelines.length, draft: 0, active: 0, idle: 0, running: 0, completed: 0, failed: 0, paused: 0 };
    pipelines.forEach(p => { if (counts[p.status] !== undefined) counts[p.status]++; });
    return counts;
  }, [pipelines]);

  const statusChips = [
    { value: "all", label: "All" },
    { value: "draft", label: "Draft" },
    { value: "active", label: "Active" },
    { value: "idle", label: "Idle" },
    { value: "running", label: "Running" },
    { value: "completed", label: "Completed" },
    { value: "failed", label: "Failed" },
    { value: "paused", label: "Paused" },
  ];

  const totalPages = Math.max(1, Math.ceil(displayPipelines.length / itemsPerPage));
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const paginatedPipelines = useMemo(() => {
    const start = (safeCurrentPage - 1) * itemsPerPage;
    return displayPipelines.slice(start, start + itemsPerPage);
  }, [displayPipelines, safeCurrentPage, itemsPerPage]);

  useEffect(() => { setCurrentPage(1); }, [searchTerm, filterStatus]);

  const groupedByTag = groupByTag ? paginatedPipelines.reduce((acc, p) => {
    const tags = p.tags?.length ? p.tags : ["Untagged"];
    tags.forEach(tag => {
      if (!acc[tag]) acc[tag] = [];
      acc[tag].push(p);
    });
    return acc;
  }, {}) : null;

  // Trigger server-side search on search term change
  useEffect(() => {
    if (searchTerm.trim()) {
      resetSearch();
      const performSearch = async () => {
        try {
          await retry(async () => {
            await fetchSearchResults(async (params) => {
              const res = await dataflow.functions.invoke('searchPipelines', {
                searchTerm: searchTerm.trim(),
                filters: filterStatus !== 'all' ? { status: filterStatus } : {},
                cursor: params.cursor,
                limit: params.limit,
              });
              return res;
            });
          });
        } catch (err) {
          console.error('[Pipelines] Search error:', err);
        }
      };
      performSearch();
    }
  }, [searchTerm, filterStatus]);

  const handleEdit = useCallback((pipeline) => {
    setEditingPipeline(pipeline);
    const { id, created_date, updated_date, created_by, workspace_id, ...pipelineFields } = pipeline;
    const merged = { ...defaultFormData, ...pipelineFields };
    Object.keys(defaultFormData).forEach(k => {
      if (merged[k] == null) merged[k] = defaultFormData[k];
    });
    merged.sla_config = pipeline.sla_config || defaultFormData.sla_config;
    merged.retry_config = pipeline.retry_config || defaultFormData.retry_config;
    setFormData(merged);
    setDialogOpen(true);
    setActiveTab("general");
  }, []);

  const handleDelete = useCallback(async (pipeline) => {
    if (!confirm(`Delete pipeline "${pipeline.name}"?`)) return;
    try {
      await dataflow.entities.Pipeline.delete(pipeline.id);
      await dataflow.entities.ActivityLog.create({
        log_type: "warning",
        category: "job",
        message: `Pipeline "${pipeline.name}" deleted`
      }).catch(() => {}); // non-critical
      toast.success("Pipeline deleted");
      loadData();
    } catch (err) {
      console.error("[Pipelines] handleDelete error:", err);
      toast.error("Failed to delete pipeline");
    }
  }, []);

  const handleRunPipeline = useCallback(async (pipeline) => {
    try {
      const run = await dataflow.entities.PipelineRun.create({
        pipeline_id: pipeline.id,
        run_number: (pipeline.total_runs || 0) + 1,
        status: "running",
        started_at: new Date().toISOString(),
        rows_processed: 0,
        bytes_transferred: 0,
        objects_completed: [],
        objects_failed: [],
        retry_count: 0,
        triggered_by: "manual"
      });

      await Promise.all([
        dataflow.entities.Pipeline.update(pipeline.id, {
          status: "running",
          last_run: new Date().toISOString(),
          total_runs: (pipeline.total_runs || 0) + 1
        }),
        dataflow.entities.ActivityLog.create({
          log_type: "info",
          category: "job",
          job_id: pipeline.id,
          run_id: run.id,
          message: `Pipeline "${pipeline.name}" started`
        }).catch(() => {})
      ]);

      toast.success("Pipeline started");
      loadData();
      simulatePipelineRun(pipeline, run);
    } catch (err) {
      console.error("[Pipelines] handleRunPipeline error:", err);
      toast.error("Failed to start pipeline");
    }
  }, []);

  const simulatePipelineRun = async (pipeline, run) => {
    const datasets = pipeline.selected_datasets || [];
    const totalRows = Math.floor(Math.random() * 100000) + 10000;
    const bytesPerRow = 500;

    await new Promise(resolve => setTimeout(resolve, 3000));

    const success = Math.random() > 0.2;
    const completedAt = new Date().toISOString();
    const duration = Math.floor((new Date(completedAt) - new Date(run.started_at)) / 1000);

    try {
      await Promise.all([
        dataflow.entities.PipelineRun.update(run.id, {
          status: success ? "completed" : "failed",
          completed_at: completedAt,
          duration_seconds: duration,
          rows_processed: success ? totalRows : Math.floor(totalRows * 0.6),
          bytes_transferred: success ? totalRows * bytesPerRow : Math.floor(totalRows * 0.6 * bytesPerRow),
          objects_completed: success ? datasets.map(d => `${d.schema}.${d.table}`) : datasets.slice(0, Math.floor(datasets.length * 0.6)).map(d => `${d.schema}.${d.table}`),
          objects_failed: success ? [] : datasets.slice(Math.floor(datasets.length * 0.6)).map(d => `${d.schema}.${d.table}`),
          error_message: success ? null : "Connection timeout on remaining datasets"
        }),
        dataflow.entities.Pipeline.update(pipeline.id, {
          status: success ? "completed" : "failed",
          successful_runs: (pipeline.successful_runs || 0) + (success ? 1 : 0),
          failed_runs: (pipeline.failed_runs || 0) + (success ? 0 : 1)
        }),
        dataflow.entities.ActivityLog.create({
          log_type: success ? "success" : "error",
          category: "job",
          job_id: pipeline.id,
          run_id: run.id,
          message: success
            ? `Pipeline "${pipeline.name}" completed – ${totalRows.toLocaleString()} rows`
            : `Pipeline "${pipeline.name}" failed – Connection timeout`
        }).catch(() => {})
      ]);
    } catch (err) {
      console.error("[Pipelines] simulatePipelineRun error:", err);
    }

    loadData();
  };

  const handleRetryPipeline = useCallback(async (pipeline) => {
    const lastRun = getPipelineRuns(pipeline.id)[0];
    if (!lastRun) return;

    try {
      const run = await dataflow.entities.PipelineRun.create({
        pipeline_id: pipeline.id,
        run_number: (pipeline.total_runs || 0) + 1,
        status: "retrying",
        started_at: new Date().toISOString(),
        rows_processed: 0,
        bytes_transferred: 0,
        objects_completed: [],
        objects_failed: [],
        retry_count: (lastRun.retry_count || 0) + 1,
        triggered_by: "retry"
      });

      await Promise.all([
        dataflow.entities.Pipeline.update(pipeline.id, {
          status: "running",
          last_run: new Date().toISOString(),
          total_runs: (pipeline.total_runs || 0) + 1
        }),
        dataflow.entities.ActivityLog.create({
          log_type: "info",
          category: "job",
          job_id: pipeline.id,
          run_id: run.id,
          message: `Pipeline "${pipeline.name}" retry #${run.retry_count}`
        }).catch(() => {})
      ]);

      toast.success("Retry started");
      loadData();
      simulatePipelineRun(pipeline, run);
    } catch (err) {
      console.error("[Pipelines] handleRetryPipeline error:", err);
      toast.error("Failed to start retry");
    }
  }, [getPipelineRuns]);

  const handlePausePipeline = useCallback(async (pipeline) => {
    try {
      const newStatus = pipeline.status === "paused" ? "idle" : "paused";
      await dataflow.entities.Pipeline.update(pipeline.id, { status: newStatus });
      dataflow.entities.ActivityLog.create({
        log_type: "info",
        category: "job",
        job_id: pipeline.id,
        message: `Pipeline "${pipeline.name}" ${newStatus === "paused" ? "paused" : "resumed"}`
      }).catch(() => {});
      toast.success(newStatus === "paused" ? "Pipeline paused" : "Pipeline resumed");
      loadData();
    } catch (err) {
      console.error("[Pipelines] handlePausePipeline error:", err);
      toast.error("Failed to update pipeline status");
    }
  }, []);

  const handleClonePipeline = useCallback((pipeline) => {
    const { id, created_date, updated_date, created_by, workspace_id, total_runs, successful_runs, failed_runs, ...pipelineFields } = pipeline;
    const merged = { ...defaultFormData, ...pipelineFields };
    Object.keys(defaultFormData).forEach(k => {
      if (merged[k] == null) merged[k] = defaultFormData[k];
    });
    merged.name = `${pipeline.name} (Copy)`;
    merged.status = "idle";
    merged.sla_config = pipeline.sla_config || defaultFormData.sla_config;
    merged.retry_config = pipeline.retry_config || defaultFormData.retry_config;
    setFormData(merged);
    setEditingPipeline(null);
    setDialogOpen(true);
    setActiveTab("general");
    toast.success("Pipeline cloned — make your changes and save.");
  }, []);

  const openNew = useCallback(() => {
    setEditingPipeline(null);
    setFormData(defaultFormData);
    setDialogOpen(true);
    setActiveTab("general");
  }, []);

  if (loading) {
    return (
      <div className="space-y-6 max-w-7xl mx-auto">
        <div className="space-y-2">
          <div className="h-8 bg-slate-200 dark:bg-slate-700 rounded w-64 animate-pulse" />
          <div className="h-4 bg-slate-200 dark:bg-slate-700 rounded w-96 animate-pulse" />
        </div>
        <SkeletonLoader count={5} height="h-32" />
      </div>
    );
  }

  if (error) {
    return <ErrorState title="Failed to load pipelines" message={error} onRetry={loadData} />;
  }

  return (
    <ErrorBoundary>
      <div className="space-y-6 max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-foreground tracking-tight">Data Pipelines</h1>
            <p className="text-muted-foreground mt-0.5">Configure and run data pipelines between connections</p>
          </div>
          <div className="flex gap-2 flex-wrap">
            <Button variant="outline" onClick={loadData} className="gap-2">
              <RefreshCw className="w-4 h-4" />
            </Button>
            <Button variant="outline" onClick={() => setShowOnboarding(true)} className="gap-2">
              <Rocket className="w-4 h-4" />
              Quick Start
            </Button>
            <Button onClick={openNew} className="gap-2 bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c]">
              <Plus className="w-4 h-4" />
              New Pipeline
            </Button>
          </div>
        </div>

        {/* Filters + List */}
        <>

            <div className="flex flex-col gap-3">
              <div className="flex flex-col sm:flex-row gap-3">
                <div className="relative flex-1 max-w-md">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                  <Input
                    placeholder="Search by name, status, or tag..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="pl-10"
                  />
                </div>
                <Button
                  variant={groupByTag ? "default" : "outline"}
                  className="gap-2 shrink-0"
                  onClick={() => setGroupByTag(g => !g)}
                >
                  <Layers className="w-4 h-4" />
                  Group by Tag
                </Button>
                <div className="flex border rounded-md overflow-hidden shrink-0">
                  <button
                    className={`px-2.5 py-1.5 ${viewMode === "card" ? "bg-[#0060AF] text-white" : "bg-white dark:bg-slate-800 text-slate-500 hover:bg-slate-50 dark:hover:bg-slate-700"}`}
                    onClick={() => setViewMode("card")} title="Card view"
                  ><LayoutGrid className="w-4 h-4" /></button>
                  <button
                    className={`px-2.5 py-1.5 ${viewMode === "list" ? "bg-[#0060AF] text-white" : "bg-white dark:bg-slate-800 text-slate-500 hover:bg-slate-50 dark:hover:bg-slate-700"}`}
                    onClick={() => setViewMode("list")} title="List view"
                  ><List className="w-4 h-4" /></button>
                </div>
              </div>

              <div className="flex flex-wrap gap-2">
                {statusChips.map(chip => (
                  <button
                    key={chip.value}
                    onClick={() => setFilterStatus(chip.value)}
                    className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-colors ${
                      filterStatus === chip.value
                        ? "bg-[#0060AF] text-white shadow-sm"
                        : "bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200 dark:hover:bg-slate-600"
                    }`}
                  >
                    {chip.label}
                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${
                      filterStatus === chip.value
                        ? "bg-white/20"
                        : "bg-slate-200 dark:bg-slate-600 text-slate-500 dark:text-slate-400"
                    }`}>
                      {statusCounts[chip.value]}
                    </span>
                  </button>
                ))}
              </div>
            </div>

            {displayPipelines.length > 0 ? (
              <>
              {groupedByTag ? (
                <div className="space-y-8">
                  {Object.entries(groupedByTag)
                    .sort(([a], [b]) => a === "Untagged" ? 1 : b === "Untagged" ? -1 : a.localeCompare(b))
                    .map(([tag, pipelinesInTag]) => (
                      <div key={tag}>
                        <div className="flex items-center gap-2 mb-3">
                          <Tag className="w-4 h-4 text-slate-400" />
                          <h2 className="text-sm font-semibold text-slate-600 dark:text-slate-300 uppercase tracking-wider">{tag}</h2>
                          <span className="text-xs text-slate-400 bg-slate-100 dark:bg-slate-700 rounded-full px-2 py-0.5">{pipelinesInTag.length}</span>
                        </div>
                        {viewMode === "list" ? (
                          <div className="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden bg-white dark:bg-slate-800">
                            {pipelinesInTag.map((pipeline) => (
                              <ErrorBoundary key={pipeline.id}>
                                <PipelineListRow
                                  job={pipeline}
                                  sourceConn={getConnection(pipeline.source_connection_id)}
                                  targetConn={getConnection(pipeline.target_connection_id)}
                                  jobRuns={getPipelineRuns(pipeline.id)}
                                  connections={connections}
                                  onEdit={handleEdit}
                                  onDelete={handleDelete}
                                  onRun={handleRunPipeline}
                                  onRetry={handleRetryPipeline}
                                  onPause={handlePausePipeline}
                                  onClone={handleClonePipeline}
                                  onViewDetails={(p) => { setViewingPipeline(p); setDetailsDialogOpen(true); }}
                                  onExport={setExportPipeline}
                                />
                              </ErrorBoundary>
                            ))}
                          </div>
                        ) : (
                          <div className="space-y-4">
                            {pipelinesInTag.map((pipeline) => (
                              <ErrorBoundary key={pipeline.id}>
                                <PipelineCard
                                  job={pipeline}
                                  sourceConn={getConnection(pipeline.source_connection_id)}
                                  targetConn={getConnection(pipeline.target_connection_id)}
                                  jobRuns={getPipelineRuns(pipeline.id)}
                                  connections={connections}
                                  onEdit={handleEdit}
                                  onDelete={handleDelete}
                                  onRun={handleRunPipeline}
                                  onRetry={handleRetryPipeline}
                                  onPause={handlePausePipeline}
                                  onClone={handleClonePipeline}
                                  onViewDetails={(p) => { setViewingPipeline(p); setDetailsDialogOpen(true); }}
                                  onExport={setExportPipeline}
                                />
                              </ErrorBoundary>
                            ))}
                          </div>
                        )}
                      </div>
                    ))}
                </div>
              ) : viewMode === "list" ? (
                <div className="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden bg-white dark:bg-slate-800">
                  <div className="flex items-center gap-4 px-4 py-2 bg-slate-50 dark:bg-slate-700/50 border-b border-slate-200 dark:border-slate-700 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    <div className="w-52 shrink-0">Name</div>
                    <div className="w-24 shrink-0">Status</div>
                    <div className="flex-1 hidden md:block">Connection Flow</div>
                    <div className="w-16 text-center hidden lg:block">Datasets</div>
                    <div className="w-24 text-right hidden xl:block">Last Run</div>
                    <div className="shrink-0 w-24" />
                  </div>
                  {paginatedPipelines.map((pipeline) => (
                    <ErrorBoundary key={pipeline.id}>
                      <PipelineListRow
                        job={pipeline}
                        sourceConn={getConnection(pipeline.source_connection_id)}
                        targetConn={getConnection(pipeline.target_connection_id)}
                        jobRuns={getPipelineRuns(pipeline.id)}
                        connections={connections}
                        onEdit={handleEdit}
                        onDelete={handleDelete}
                        onRun={handleRunPipeline}
                        onRetry={handleRetryPipeline}
                        onPause={handlePausePipeline}
                        onClone={handleClonePipeline}
                        onViewDetails={(p) => { setViewingPipeline(p); setDetailsDialogOpen(true); }}
                        onExport={setExportPipeline}
                      />
                    </ErrorBoundary>
                  ))}
                </div>
              ) : (
              <div className="space-y-4">
                {paginatedPipelines.map((pipeline) => (
                   <ErrorBoundary key={pipeline.id}>
                    <PipelineCard
                      job={pipeline}
                      sourceConn={getConnection(pipeline.source_connection_id)}
                      targetConn={getConnection(pipeline.target_connection_id)}
                      jobRuns={getPipelineRuns(pipeline.id)}
                      connections={connections}
                      onEdit={handleEdit}
                      onDelete={handleDelete}
                      onRun={handleRunPipeline}
                      onRetry={handleRetryPipeline}
                      onPause={handlePausePipeline}
                      onClone={handleClonePipeline}
                      onViewDetails={(p) => { setViewingPipeline(p); setDetailsDialogOpen(true); }}
                      onExport={setExportPipeline}
                    />
                  </ErrorBoundary>
                ))}
              </div>
              )}

              {totalPages > 1 && (
                <div className="flex items-center justify-between mt-6 px-1">
                  <p className="text-sm text-slate-500 dark:text-slate-400">
                    Showing {((safeCurrentPage - 1) * itemsPerPage) + 1}–{Math.min(safeCurrentPage * itemsPerPage, displayPipelines.length)} of {displayPipelines.length} pipeline{displayPipelines.length !== 1 ? "s" : ""}
                  </p>
                  <div className="flex items-center gap-1">
                    <Button variant="outline" size="sm" disabled={safeCurrentPage <= 1} onClick={() => setCurrentPage(p => Math.max(1, p - 1))} className="h-8 w-8 p-0">
                      <ChevronLeft className="w-4 h-4" />
                    </Button>
                    {Array.from({ length: totalPages }, (_, i) => i + 1)
                      .filter(page => page === 1 || page === totalPages || Math.abs(page - safeCurrentPage) <= 2)
                      .reduce((acc, page, idx, arr) => {
                        if (idx > 0 && page - arr[idx - 1] > 1) acc.push("ellipsis-" + page);
                        acc.push(page);
                        return acc;
                      }, [])
                      .map(page => typeof page === "string" ? (
                        <span key={page} className="px-1 text-slate-400">…</span>
                      ) : (
                        <Button key={page} variant={page === safeCurrentPage ? "default" : "outline"} size="sm" onClick={() => setCurrentPage(page)}
                          className={`h-8 w-8 p-0 ${page === safeCurrentPage ? "bg-[#0060AF] hover:bg-[#004d8c] text-white" : ""}`}>
                          {page}
                        </Button>
                      ))}
                    <Button variant="outline" size="sm" disabled={safeCurrentPage >= totalPages} onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))} className="h-8 w-8 p-0">
                      <ChevronRight className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              )}
              </>
            ) : (
              <EmptyStateGuide
                icon={Play}
                title={searchTerm ? "No pipelines found" : connections.length === 0 ? "Set up a connection first" : "No data pipelines yet"}
                description={
                  searchTerm
                    ? "Try adjusting your search or filters"
                    : connections.length === 0
                    ? "You need at least one source and one target connection before creating a pipeline"
                    : "Create your first data pipeline to start moving data between connections"
                }
                primaryAction={!searchTerm ? (connections.length === 0 ? {
                  label: "Go to Connections",
                  icon: <Plus className="w-4 h-4" />,
                  onClick: () => { window.location.assign(createPageUrl("Connections")); }
                } : {
                  label: "New Pipeline",
                  icon: <Plus className="w-4 h-4" />,
                  onClick: openNew
                }) : null}
              />
            )}
        </>

        <OrchestrationPanel variant="full" />

        {/* Dialogs — only rendered when opened */}
        <JobFormDialog
          open={dialogOpen}
          onOpenChange={setDialogOpen}
          editingJob={editingPipeline}
          formData={formData}
          setFormData={setFormData}
          connections={connections}
          pipelines={pipelines}
          onSaveSuccess={loadData}
          currentUser={currentUser}
        />

        {exportPipeline && (
          <JobSpecExport
            job={exportPipeline}
            connections={connections}
            onClose={() => setExportPipeline(null)}
          />
        )}

        <JobDetailsDialog
          open={detailsDialogOpen}
          onOpenChange={setDetailsDialogOpen}
          job={viewingPipeline}
          jobRuns={viewingPipeline ? getPipelineRuns(viewingPipeline.id) : []}
        />

        <OnboardingWizard
          open={showOnboarding}
          onClose={() => setShowOnboarding(false)}
          connections={connections}
          jobs={pipelines}
        />
      </div>
    </ErrorBoundary>
  );
}