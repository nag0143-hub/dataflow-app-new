import { useState } from "react";
import { dataflow } from '@/api/client';
import { useQuery } from "@tanstack/react-query";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Search,
  User,
  Calendar,
  FileEdit,
  Trash2,
  Plus,
  Play,
  Pause,
  Copy,
  Eye,
  Clock
} from "lucide-react";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import SkeletonLoader from "@/components/SkeletonLoader";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useTenant } from "@/components/useTenant";

export default function AuditTrail() {
  const { user, loading: userLoading } = useTenant();
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedAction, setSelectedAction] = useState(null);
  const [selectedEntity, setSelectedEntity] = useState(null);
  const [viewDetails, setViewDetails] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const logsPerPage = 50;

  const { data: auditLogs = [], isLoading } = useQuery({
    queryKey: ['audit-logs'],
    queryFn: () => dataflow.entities.AuditLog.list('-created_date', 1000)
  });

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

  const filteredLogs = auditLogs.filter(log => {
    const matchesSearch = !searchQuery ||
      log.entity_name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      log.user_email?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      log.user_name?.toLowerCase().includes(searchQuery.toLowerCase());
    
    const matchesAction = !selectedAction || log.action === selectedAction;
    const matchesEntity = !selectedEntity || log.entity_type === selectedEntity;

    return matchesSearch && matchesAction && matchesEntity;
  });

  // Pagination
  const totalPages = Math.ceil(filteredLogs.length / logsPerPage);
  const startIndex = (currentPage - 1) * logsPerPage;
  const paginatedLogs = filteredLogs.slice(startIndex, startIndex + logsPerPage);

  if (isLoading || userLoading) {
    return (
      <div className="space-y-6 max-w-7xl mx-auto">
        <div className="h-8 bg-slate-200 dark:bg-slate-700 rounded w-64 animate-pulse" />
        <SkeletonLoader count={8} height="h-24" />
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-7xl mx-auto">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-foreground tracking-tight">Audit Trail</h1>
        <p className="text-muted-foreground mt-0.5">
          Complete history of user actions and system changes
        </p>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <div className="flex-1 min-w-[300px]">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <Input
              placeholder="Search by entity name, user..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </div>

        <Select value={selectedAction || "all"} onValueChange={(v) => setSelectedAction(v === "all" ? null : v)}>
          <SelectTrigger className="w-40">
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

        <Select value={selectedEntity || "all"} onValueChange={(v) => setSelectedEntity(v === "all" ? null : v)}>
          <SelectTrigger className="w-40">
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

      {/* Stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <Card className="dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="pt-6">
            <div className="text-center">
              <p className="text-3xl font-bold text-blue-600">{auditLogs.length}</p>
              <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Total Events</p>
            </div>
          </CardContent>
        </Card>
        <Card className="dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="pt-6">
            <div className="text-center">
              <p className="text-3xl font-bold text-green-600">
                {auditLogs.filter(l => l.action === 'create').length}
              </p>
              <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Creates</p>
            </div>
          </CardContent>
        </Card>
        <Card className="dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="pt-6">
            <div className="text-center">
              <p className="text-3xl font-bold text-amber-600">
                {auditLogs.filter(l => l.action === 'update').length}
              </p>
              <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Updates</p>
            </div>
          </CardContent>
        </Card>
        <Card className="dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="pt-6">
            <div className="text-center">
              <p className="text-3xl font-bold text-red-600">
                {auditLogs.filter(l => l.action === 'delete').length}
              </p>
              <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Deletes</p>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Audit Logs */}
      <div className="space-y-3">
        {paginatedLogs.length === 0 ? (
          <Card className="dark:bg-slate-800 dark:border-slate-700">
            <CardContent className="py-12 text-center">
              <Clock className="w-12 h-12 text-slate-300 dark:text-slate-600 mx-auto mb-3" />
              <p className="text-slate-500 dark:text-slate-400">
                {searchQuery || selectedAction || selectedEntity
                  ? "No audit logs match your filters"
                  : "No audit logs yet"}
              </p>
            </CardContent>
          </Card>
        ) : (
          paginatedLogs.map(log => {
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
                            onClick={() => setViewDetails(log)}
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
                          <span>{format(new Date(log.created_date), "MMM d, yyyy 'at' h:mm a")}</span>
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

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-slate-600 dark:text-slate-400">
            Showing {startIndex + 1}-{Math.min(startIndex + logsPerPage, filteredLogs.length)} of {filteredLogs.length}
          </p>
          <div className="flex gap-2">
            <button
              onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
              disabled={currentPage === 1}
              className="px-3 py-1 text-sm border dark:border-slate-700 rounded disabled:opacity-50 dark:text-white"
            >
              Previous
            </button>
            <span className="px-3 py-1 text-sm dark:text-white">
              Page {currentPage} of {totalPages}
            </span>
            <button
              onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
              disabled={currentPage === totalPages}
              className="px-3 py-1 text-sm border dark:border-slate-700 rounded disabled:opacity-50 dark:text-white"
            >
              Next
            </button>
          </div>
        </div>
      )}

      {/* Change Details Dialog */}
      {viewDetails && (
        <Dialog open={!!viewDetails} onOpenChange={() => setViewDetails(null)}>
          <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>Change Details</DialogTitle>
            </DialogHeader>
            <div className="space-y-4">
              <div>
                <p className="text-sm font-medium mb-1">Entity:</p>
                <p className="text-sm text-slate-600 dark:text-slate-400">{viewDetails.entity_name}</p>
              </div>
              {viewDetails.changes?.before && (
                <div>
                  <p className="text-sm font-medium mb-2">Before:</p>
                  <pre className="bg-slate-100 dark:bg-slate-800 p-3 rounded text-xs overflow-x-auto">
                    {JSON.stringify(viewDetails.changes.before, null, 2)}
                  </pre>
                </div>
              )}
              {viewDetails.changes?.after && (
                <div>
                  <p className="text-sm font-medium mb-2">After:</p>
                  <pre className="bg-slate-100 dark:bg-slate-800 p-3 rounded text-xs overflow-x-auto">
                    {JSON.stringify(viewDetails.changes.after, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          </DialogContent>
        </Dialog>
      )}
    </div>
  );
}