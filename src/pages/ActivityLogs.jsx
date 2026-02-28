import { useState, useEffect } from "react";
import { dataflow } from '@/api/client';
import {
  Search,
  Filter,
  RefreshCw,
  Download,
  AlertCircle,
  CheckCircle2,
  Info,
  AlertTriangle,
  Clock,
  ChevronDown,
  X,
  ChevronLeft,
  ChevronRight,
  User,
  Calendar,
  FileEdit,
  Trash2,
  Plus,
  Play,
  Pause,
  Copy,
  Eye,
  Shield
} from "lucide-react";
import { toast } from "sonner";
import moment from "moment";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import SkeletonLoader from "@/components/SkeletonLoader";
import { createIndex } from "@/components/dataIndexing";
import { useRetry } from "@/components/hooks/useRetry";

const logTypeConfig = {
  info: { icon: Info, color: "bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 border-blue-200", dotColor: "bg-blue-500" },
  warning: { icon: AlertTriangle, color: "bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 border-amber-200", dotColor: "bg-amber-500" },
  error: { icon: AlertCircle, color: "bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300 border-red-200", dotColor: "bg-red-500" },
  success: { icon: CheckCircle2, color: "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300 border-emerald-200", dotColor: "bg-emerald-500" },
};

const categoryConfig = {
  connection: { label: "Connection", color: "bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-300" },
  job: { label: "Job", color: "bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300" },
  system: { label: "System", color: "bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300" },
  authentication: { label: "Auth", color: "bg-orange-100 dark:bg-orange-900/40 text-orange-700 dark:text-orange-300" },
};

export default function ActivityLogs() {
  const { retry } = useRetry();
  const [logs, setLogs] = useState([]);
  const [auditLogs, setAuditLogs] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [connections, setConnections] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");
  const [filterType, setFilterType] = useState("all");
  const [filterCategory, setFilterCategory] = useState("all");
  const [selectedLog, setSelectedLog] = useState(null);
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [serverSearching, setServerSearching] = useState(false);
  const logsPerPage = 20;
  const [selectedAction, setSelectedAction] = useState("all");
  const [selectedEntity, setSelectedEntity] = useState("all");
  const [viewAuditDetails, setViewAuditDetails] = useState(null);
  const [activeTab, setActiveTab] = useState("activity");

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    setLoading(true);
    try {
      const [auditLogsData, jobsData, connectionsData] = await Promise.all([
        dataflow.entities.AuditLog.list("-created_date", 1000),
        dataflow.entities.Pipeline.list(),
        dataflow.entities.Connection.list()
      ]);
      // Don't load all logs initially - use server-side search instead
      setLogs([]);
      setAuditLogs(auditLogsData);
      setJobs(jobsData);
      setConnections(connectionsData);
    } finally {
      setLoading(false);
    }
  };

  // Server-side search for activity logs
  useEffect(() => {
    const searchLogs = async () => {
      setServerSearching(true);
      try {
        const result = await retry(async () => {
          const res = await dataflow.functions.invoke('searchActivityLogs', {
            searchTerm: searchTerm.trim(),
            filters: {
              ...(filterType !== 'all' && { log_type: filterType }),
              ...(filterCategory !== 'all' && { category: filterCategory }),
            },
            limit: logsPerPage * currentPage,
          });
          return res;
        });
        setLogs(result?.items || []);
      } catch (err) {
        console.error('[ActivityLogs] Server search error:', err);
      } finally {
        setServerSearching(false);
      }
    };

    const timeout = setTimeout(searchLogs, 300); // Debounce
    return () => clearTimeout(timeout);
  }, [searchTerm, filterType, filterCategory, currentPage]);

  // Indexed lookups for better performance
  const jobIndex = createIndex(jobs, "id");
  const connectionIndex = createIndex(connections, "id");
  
  const getJobName = (jobId) => jobIndex.get(jobId)?.name || "Unknown Job";
  const getConnectionName = (connId) => connectionIndex.get(connId)?.name || "Unknown Connection";

  // Pagination (server returns paginated results)
  const totalPages = Math.ceil(logs.length / logsPerPage);
  const paginatedLogs = logs;

  const handleExport = () => {
    const csvContent = [
      ["Timestamp", "Type", "Category", "Message", "Job", "Connection"].join(","),
      ...paginatedLogs.map(log => [
        moment(log.created_date).format("YYYY-MM-DD HH:mm:ss"),
        log.log_type,
        log.category,
        `"${log.message?.replace(/"/g, '""') || ""}"`,
        log.job_id ? getJobName(log.job_id) : "",
        log.connection_id ? getConnectionName(log.connection_id) : ""
      ].join(","))
    ].join("\n");

    const blob = new Blob([csvContent], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `activity-logs-${moment().format("YYYY-MM-DD")}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    toast.success("Logs exported");
  };

  const clearFilters = () => {
    setSearchTerm("");
    setFilterType("all");
    setFilterCategory("all");
  };

  const hasFilters = searchTerm || filterType !== "all" || filterCategory !== "all";

  const actionIcons = {
    create: Plus,
    update: FileEdit,
    delete: Trash2,
    execute: Play,
    pause: Pause,
    resume: Play,
    clone: Copy
  };

  const actionColors = {
    create: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
    update: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300",
    delete: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300",
    execute: "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300",
    pause: "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300",
    resume: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-300",
    clone: "bg-cyan-100 text-cyan-800 dark:bg-cyan-900 dark:text-cyan-300"
  };

  const stats = {
    total: logs.length,
    errors: logs.filter(l => l.log_type === "error").length,
    warnings: logs.filter(l => l.log_type === "warning").length,
    today: logs.filter(l => moment(l.created_date).isSame(moment(), 'day')).length
  };

  const auditStats = {
    total: auditLogs.length,
    creates: auditLogs.filter(l => l.action === 'create').length,
    updates: auditLogs.filter(l => l.action === 'update').length,
    deletes: auditLogs.filter(l => l.action === 'delete').length
  };

  const filteredAuditLogs = auditLogs.filter(log => {
    const matchesSearch = !searchTerm ||
      log.entity_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      log.user_email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      log.user_name?.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesAction = selectedAction === "all" || log.action === selectedAction;
    const matchesEntity = selectedEntity === "all" || log.entity_type === selectedEntity;

    return matchesSearch && matchesAction && matchesEntity;
  });

  const paginatedAuditLogs = filteredAuditLogs.slice((currentPage - 1) * logsPerPage, currentPage * logsPerPage);
  const totalAuditPages = Math.ceil(filteredAuditLogs.length / logsPerPage);

  if (loading) {
    return (
      <div className="space-y-6 max-w-7xl mx-auto">
        <div className="space-y-2">
          <div className="h-8 bg-slate-200 dark:bg-slate-700 rounded w-48 animate-pulse" />
          <div className="h-4 bg-slate-200 dark:bg-slate-700 rounded w-96 animate-pulse" />
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {Array.from({length:4}).map((_, i) => (
            <div key={i} className="h-20 bg-slate-200 dark:bg-slate-700 rounded-lg animate-pulse" />
          ))}
        </div>
        <SkeletonLoader count={8} height="h-16" />
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-foreground tracking-tight">Activity & Audit Logs</h1>
          <p className="text-muted-foreground mt-0.5">Monitor system activities and track user changes</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" onClick={loadData} className="gap-2 dark:bg-slate-700 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-600">
            <RefreshCw className="w-4 h-4" />
            Refresh
          </Button>
          <Button variant="outline" onClick={handleExport} className="gap-2 dark:bg-slate-700 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-600">
            <Download className="w-4 h-4" />
            Export
          </Button>
        </div>
      </div>

      {/* Tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="dark:bg-slate-800 dark:border dark:border-slate-700">
          <TabsTrigger value="activity" className="gap-2 dark:text-slate-300 dark:data-[state=active]:bg-slate-700 dark:data-[state=active]:text-white">
            <AlertCircle className="w-4 h-4" />
            Activity Logs
          </TabsTrigger>
          <TabsTrigger value="audit" className="gap-2 dark:text-slate-300 dark:data-[state=active]:bg-slate-700 dark:data-[state=active]:text-white">
            <Shield className="w-4 h-4" />
            Audit Trail
          </TabsTrigger>
        </TabsList>

        <TabsContent value="activity" className="space-y-6 mt-6">

        {/* Stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="border-slate-200 dark:bg-slate-800 dark:border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-lg bg-slate-100 dark:bg-slate-700 flex items-center justify-center">
                  <Clock className="w-5 h-5 text-slate-600 dark:text-slate-300" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-slate-900 dark:text-white">{stats.total}</p>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Total Logs</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="border-slate-200 dark:bg-slate-800 dark:border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-lg bg-red-100 dark:bg-red-900 flex items-center justify-center">
                  <AlertCircle className="w-5 h-5 text-red-600 dark:text-red-300" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-red-600 dark:text-red-400">{stats.errors}</p>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Errors</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="border-slate-200 dark:bg-slate-800 dark:border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-lg bg-amber-100 dark:bg-amber-900 flex items-center justify-center">
                  <AlertTriangle className="w-5 h-5 text-amber-600 dark:text-amber-300" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-amber-600 dark:text-amber-400">{stats.warnings}</p>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Warnings</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="border-slate-200 dark:bg-slate-800 dark:border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-lg bg-emerald-100 dark:bg-emerald-900 flex items-center justify-center">
                  <CheckCircle2 className="w-5 h-5 text-emerald-600 dark:text-emerald-300" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-slate-900 dark:text-white">{stats.today}</p>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Today</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Filters */}
        <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center">
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <Input
              placeholder="Search logs..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100"
            />
          </div>
          <Select value={filterType} onValueChange={setFilterType}>
            <SelectTrigger className="w-36 dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100">
              <SelectValue placeholder="Type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              <SelectItem value="info">Info</SelectItem>
              <SelectItem value="success">Success</SelectItem>
              <SelectItem value="warning">Warning</SelectItem>
              <SelectItem value="error">Error</SelectItem>
            </SelectContent>
          </Select>
          <Select value={filterCategory} onValueChange={setFilterCategory}>
            <SelectTrigger className="w-40 dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100">
              <SelectValue placeholder="Category" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Categories</SelectItem>
              <SelectItem value="connection">Connection</SelectItem>
              <SelectItem value="job">Job</SelectItem>
              <SelectItem value="system">System</SelectItem>
              <SelectItem value="authentication">Authentication</SelectItem>
            </SelectContent>
          </Select>
          {hasFilters && (
            <Button variant="ghost" size="sm" onClick={clearFilters} className="gap-1 dark:text-slate-100">
              <X className="w-4 h-4" />
              Clear
            </Button>
          )}
        </div>

        {/* Logs List */}
        <Card className="border-slate-200 dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="p-0">
          {paginatedLogs.length > 0 ? (
            <div className="divide-y divide-slate-100 dark:divide-slate-700">
              {paginatedLogs.map((log) => {
                const typeConfig = logTypeConfig[log.log_type] || logTypeConfig.info;
                const catConfig = categoryConfig[log.category] || categoryConfig.system;
                const Icon = typeConfig.icon;

                return (
                  <div
                    key={log.id}
                    className="flex items-start gap-4 p-4 hover:bg-slate-50 dark:hover:bg-slate-700/50 cursor-pointer transition-colors"
                    onClick={() => { setSelectedLog(log); setDetailsOpen(true); }}
                  >
                    {/* Type Indicator */}
                    <div className={cn(
                      "w-8 h-8 rounded-lg flex items-center justify-center shrink-0",
                      typeConfig.color.split(" ")[0]
                    )}>
                      <Icon className={cn("w-4 h-4", typeConfig.color.split(" ")[1])} />
                    </div>

                    {/* Content */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap mb-1">
                        <Badge variant="outline" className={cn("text-xs dark:border-slate-600", catConfig.color)}>
                          {catConfig.label}
                        </Badge>
                        {log.job_id && (
                          <span className="text-xs text-slate-500 dark:text-slate-400">
                            Job: {getJobName(log.job_id)}
                          </span>
                        )}
                        {log.connection_id && (
                          <span className="text-xs text-slate-500 dark:text-slate-400">
                            Connection: {getConnectionName(log.connection_id)}
                          </span>
                        )}
                      </div>
                      <p className="text-sm text-slate-900 dark:text-slate-100">{log.message}</p>
                      {log.object_name && (
                        <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Object: {log.object_name}</p>
                      )}
                    </div>

                    {/* Timestamp */}
                    <div className="text-right shrink-0">
                      <p className="text-sm text-slate-500 dark:text-slate-400">
                        {moment(log.created_date).format("h:mm A")}
                      </p>
                      <p className="text-xs text-slate-400 dark:text-slate-500">
                        {moment(log.created_date).format("MMM D")}
                      </p>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="py-16 text-center">
              <Clock className="w-12 h-12 text-slate-300 dark:text-slate-600 mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-slate-900 dark:text-white mb-2">No logs found</h3>
              <p className="text-slate-500 dark:text-slate-400">
                {hasFilters ? "Try adjusting your filters" : "Activity logs will appear here"}
              </p>
            </div>
          )}
          </CardContent>
        </Card>

        {/* Pagination */}
         {totalPages > 1 && (
           <div className="flex items-center justify-between">
             <p className="text-sm text-slate-500 dark:text-slate-400">
               Showing {(currentPage - 1) * logsPerPage + 1} to {Math.min(currentPage * logsPerPage, logs.length)} of {logs.length} logs
             </p>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                disabled={currentPage === 1}
                className="dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100"
              >
                <ChevronLeft className="w-4 h-4" />
                Previous
              </Button>
              <div className="flex items-center gap-1">
                {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                  const page = i + 1;
                  return (
                    <button
                      key={page}
                      onClick={() => setCurrentPage(page)}
                      className={`w-8 h-8 rounded text-sm font-medium ${
                        currentPage === page
                          ? "bg-blue-600 text-white"
                          : "hover:bg-slate-100 dark:hover:bg-slate-700 dark:text-white"
                      }`}
                    >
                      {page}
                    </button>
                  );
                })}
                {totalPages > 5 && <span className="text-slate-400">...</span>}
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                disabled={currentPage === totalPages}
                className="dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100"
              >
                Next
                <ChevronRight className="w-4 h-4" />
              </Button>
            </div>
          </div>
        )}
        </TabsContent>

        <TabsContent value="audit" className="space-y-6 mt-6">
          {/* Audit Stats */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <Card className="dark:bg-slate-800 dark:border-slate-700">
              <CardContent className="pt-6">
                <div className="text-center">
                  <p className="text-3xl font-bold text-blue-600">{auditStats.total}</p>
                  <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Total Events</p>
                </div>
              </CardContent>
            </Card>
            <Card className="dark:bg-slate-800 dark:border-slate-700">
              <CardContent className="pt-6">
                <div className="text-center">
                  <p className="text-3xl font-bold text-green-600">{auditStats.creates}</p>
                  <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Creates</p>
                </div>
              </CardContent>
            </Card>
            <Card className="dark:bg-slate-800 dark:border-slate-700">
              <CardContent className="pt-6">
                <div className="text-center">
                  <p className="text-3xl font-bold text-amber-600">{auditStats.updates}</p>
                  <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Updates</p>
                </div>
              </CardContent>
            </Card>
            <Card className="dark:bg-slate-800 dark:border-slate-700">
              <CardContent className="pt-6">
                <div className="text-center">
                  <p className="text-3xl font-bold text-red-600">{auditStats.deletes}</p>
                  <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Deletes</p>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Audit Filters */}
          <div className="flex flex-wrap gap-3">
            <div className="flex-1 min-w-[300px]">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <Input
                  placeholder="Search by entity name, user..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10 dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100"
                />
              </div>
            </div>

            <Select value={selectedAction} onValueChange={setSelectedAction}>
              <SelectTrigger className="w-40 dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100">
                <SelectValue placeholder="All Actions" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Actions</SelectItem>
                <SelectItem value="create">Create</SelectItem>
                <SelectItem value="update">Update</SelectItem>
                <SelectItem value="delete">Delete</SelectItem>
                <SelectItem value="execute">Execute</SelectItem>
                <SelectItem value="pause">Pause</SelectItem>
                <SelectItem value="resume">Resume</SelectItem>
                <SelectItem value="clone">Clone</SelectItem>
              </SelectContent>
            </Select>

            <Select value={selectedEntity} onValueChange={setSelectedEntity}>
              <SelectTrigger className="w-40 dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100">
                <SelectValue placeholder="All Entities" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Entities</SelectItem>
                <SelectItem value="connection">Connection</SelectItem>
                <SelectItem value="job">Job</SelectItem>
                <SelectItem value="user">User</SelectItem>
                <SelectItem value="prerequisite">Prerequisite</SelectItem>
                <SelectItem value="catalog_entry">Catalog Entry</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {/* Audit Logs */}
          <div className="space-y-3">
            {paginatedAuditLogs.length === 0 ? (
              <Card className="dark:bg-slate-800 dark:border-slate-700">
                <CardContent className="py-12 text-center">
                  <Clock className="w-12 h-12 text-slate-300 dark:text-slate-600 mx-auto mb-3" />
                  <p className="text-slate-500 dark:text-slate-400">
                    {searchTerm || selectedAction !== "all" || selectedEntity !== "all"
                      ? "No audit logs match your filters"
                      : "No audit logs yet"}
                  </p>
                </CardContent>
              </Card>
            ) : (
              paginatedAuditLogs.map(log => {
                const ActionIcon = actionIcons[log.action] || FileEdit;
                return (
                  <Card key={log.id} className="dark:bg-slate-800 dark:border-slate-700 hover:shadow-md transition-shadow">
                    <CardContent className="py-4">
                      <div className="flex items-start gap-4">
                        <div className={cn(
                          "w-10 h-10 rounded-lg flex items-center justify-center shrink-0",
                          actionColors[log.action] || "bg-slate-100"
                        )}>
                          <ActionIcon className="w-5 h-5" />
                        </div>

                        <div className="flex-1 min-w-0">
                          <div className="flex items-start justify-between gap-4 mb-2">
                            <div className="flex-1">
                              <div className="flex items-center gap-2 mb-1">
                                <Badge className={actionColors[log.action]} variant="secondary">
                                  {log.action}
                                </Badge>
                                <Badge variant="outline" className="dark:border-slate-600">
                                  {log.entity_type}
                                </Badge>
                              </div>
                              <p className="font-medium text-slate-900 dark:text-white">
                                {log.entity_name || log.entity_id}
                              </p>
                            </div>

                            {log.changes && (
                              <button
                                onClick={() => setViewAuditDetails(log)}
                                className="text-blue-600 dark:text-blue-400 hover:underline text-sm flex items-center gap-1"
                              >
                                <Eye className="w-4 h-4" />
                                View Changes
                              </button>
                            )}
                          </div>

                          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-slate-600 dark:text-slate-400">
                            <div className="flex items-center gap-1.5">
                              <User className="w-4 h-4" />
                              <span>{log.user_name || log.user_email}</span>
                            </div>
                            <div className="flex items-center gap-1.5">
                              <Calendar className="w-4 h-4" />
                              <span>{moment(log.created_date).format("MMM D, YYYY [at] h:mm A")}</span>
                            </div>
                            {log.ip_address && (
                              <div className="flex items-center gap-1.5">
                                <span className="text-xs">IP: {log.ip_address}</span>
                              </div>
                            )}
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                );
              })
            )}
          </div>

          {/* Audit Pagination */}
          {totalAuditPages > 1 && (
            <div className="flex items-center justify-between">
              <p className="text-sm text-slate-600 dark:text-slate-400">
                Showing {(currentPage - 1) * logsPerPage + 1}-{Math.min(currentPage * logsPerPage, filteredAuditLogs.length)} of {filteredAuditLogs.length}
              </p>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                  disabled={currentPage === 1}
                  className="dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100"
                >
                  Previous
                </Button>
                <span className="px-3 py-1 text-sm dark:text-white">
                  Page {currentPage} of {totalAuditPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(p => Math.min(totalAuditPages, p + 1))}
                  disabled={currentPage === totalAuditPages}
                  className="dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100"
                >
                  Next
                </Button>
              </div>
            </div>
          )}

          {/* Audit Details Dialog */}
          {viewAuditDetails && (
            <Dialog open={!!viewAuditDetails} onOpenChange={() => setViewAuditDetails(null)}>
              <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto dark:bg-slate-800 dark:border-slate-700">
                <DialogHeader>
                  <DialogTitle className="dark:text-slate-100">Change Details</DialogTitle>
                </DialogHeader>
                <div className="space-y-4">
                  <div>
                    <p className="text-sm font-medium mb-1 dark:text-slate-100">Entity:</p>
                    <p className="text-sm text-slate-600 dark:text-slate-400">{viewAuditDetails.entity_name}</p>
                  </div>
                  {viewAuditDetails.changes?.before && (
                    <div>
                      <p className="text-sm font-medium mb-2 dark:text-slate-100">Before:</p>
                      <pre className="bg-slate-100 dark:bg-slate-800 p-3 rounded text-xs overflow-x-auto dark:text-slate-300">
                        {JSON.stringify(viewAuditDetails.changes.before, null, 2)}
                      </pre>
                    </div>
                  )}
                  {viewAuditDetails.changes?.after && (
                    <div>
                      <p className="text-sm font-medium mb-2 dark:text-slate-100">After:</p>
                      <pre className="bg-slate-100 dark:bg-slate-800 p-3 rounded text-xs overflow-x-auto dark:text-slate-300">
                        {JSON.stringify(viewAuditDetails.changes.after, null, 2)}
                      </pre>
                    </div>
                  )}
                </div>
              </DialogContent>
            </Dialog>
          )}
        </TabsContent>
      </Tabs>

      {/* Log Details Dialog */}
      <Dialog open={detailsOpen} onOpenChange={setDetailsOpen}>
        <DialogContent className="max-w-lg dark:bg-slate-800 dark:border-slate-700">
          <DialogHeader>
            <DialogTitle className="dark:text-slate-100">Log Details</DialogTitle>
          </DialogHeader>

          {selectedLog && (
            <div className="space-y-4">
              <div className="flex items-center gap-3">
                {(() => {
                  const typeConfig = logTypeConfig[selectedLog.log_type] || logTypeConfig.info;
                  const Icon = typeConfig.icon;
                  return (
                    <div className={cn(
                      "w-10 h-10 rounded-lg flex items-center justify-center",
                      typeConfig.color.split(" ")[0]
                    )}>
                      <Icon className={cn("w-5 h-5", typeConfig.color.split(" ")[1])} />
                    </div>
                  );
                })()}
                <div>
                  <p className="font-medium text-slate-900 dark:text-white capitalize">{selectedLog.log_type}</p>
                  <p className="text-sm text-slate-500 dark:text-slate-400">
                    {moment(selectedLog.created_date).format("MMM D, YYYY h:mm:ss A")}
                  </p>
                </div>
              </div>

              <div className="bg-slate-50 dark:bg-slate-800 rounded-lg p-4">
                <p className="text-sm text-slate-900 dark:text-slate-100">{selectedLog.message}</p>
              </div>

              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <p className="text-slate-500 dark:text-slate-400">Category</p>
                  <p className="font-medium dark:text-white capitalize">{selectedLog.category}</p>
                </div>
                {selectedLog.job_id && (
                  <div>
                    <p className="text-slate-500 dark:text-slate-400">Job</p>
                    <p className="font-medium dark:text-white">{getJobName(selectedLog.job_id)}</p>
                  </div>
                )}
                {selectedLog.connection_id && (
                  <div>
                    <p className="text-slate-500 dark:text-slate-400">Connection</p>
                    <p className="font-medium dark:text-white">{getConnectionName(selectedLog.connection_id)}</p>
                  </div>
                )}
                {selectedLog.run_id && (
                  <div>
                    <p className="text-slate-500 dark:text-slate-400">Run ID</p>
                    <p className="font-medium dark:text-white font-mono text-xs">{selectedLog.run_id}</p>
                  </div>
                )}
                {selectedLog.object_name && (
                  <div>
                    <p className="text-slate-500 dark:text-slate-400">Object</p>
                    <p className="font-medium dark:text-white">{selectedLog.object_name}</p>
                  </div>
                )}
              </div>

              {selectedLog.stack_trace && (
                <div>
                  <p className="text-sm text-slate-500 dark:text-slate-400 mb-2">Stack Trace</p>
                  <pre className="bg-slate-900 text-slate-100 rounded-lg p-3 text-xs overflow-x-auto">
                    {selectedLog.stack_trace}
                  </pre>
                </div>
              )}

              {selectedLog.details && (
                <div>
                  <p className="text-sm text-slate-500 dark:text-slate-400 mb-2">Additional Details</p>
                  <pre className="bg-slate-50 dark:bg-slate-800 rounded-lg p-3 text-xs overflow-x-auto dark:text-slate-300">
                    {JSON.stringify(selectedLog.details, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}