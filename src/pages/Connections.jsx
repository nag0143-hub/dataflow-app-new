import { useState, useEffect, useMemo } from "react";
import { dataflow } from '@/api/client';
import { 
  Plus, Search, MoreVertical, Edit, Trash2, TestTube,
  Cable, Shield, CheckCircle2, XCircle, Loader2, Wifi, BookOpen, RefreshCw,
  Tag, X, Layers, LayoutGrid, List, Lock, KeyRound, ExternalLink, ChevronLeft, ChevronRight
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import StatusBadge from "@/components/StatusBadge";
import PlatformIcon, { platformConfig } from "@/components/PlatformIcon";
import { useTenant } from "@/components/useTenant";
import ErrorBoundary from "@/components/ErrorBoundary";
import ErrorState from "@/components/ErrorState";
import { useRetry } from "@/components/hooks/useRetry";
import { useCache } from "@/components/hooks/useCache";
import moment from "moment";
import { toast } from "sonner";

import PrerequisitePanel from "@/components/PrerequisitePanel";
import ConnectionProfilePicker from "@/components/ConnectionProfilePicker";
import ConnectionCard from "@/components/ConnectionCard";
import ConnectionListRow from "@/components/ConnectionListRow";

function SaveAsProfileButton({ formData }) {
  const [saving, setSaving] = useState(false);
  const [namePrompt, setNamePrompt] = useState(false);
  const [profileName, setProfileName] = useState("");

  const handleSave = async () => {
    if (!formData.platform) { toast.error("Select a platform first"); return; }
    if (!profileName.trim()) return;
    setSaving(true);
    try {
      const { status, last_tested, notes, name: connName, ...fields } = formData;
      await dataflow.entities.ConnectionProfile.create({ ...fields, name: profileName.trim() });
      toast.success(`Saved as profile "${profileName.trim()}"`);
      setNamePrompt(false);
      setProfileName("");
    } catch (err) {
      toast.error("Failed to save profile");
    } finally {
      setSaving(false);
    }
  };

  if (namePrompt) {
    return (
      <div className="flex items-center gap-2">
        <Input
          value={profileName}
          onChange={e => setProfileName(e.target.value)}
          placeholder="Profile name..."
          className="h-8 text-sm w-44"
          autoFocus
          onKeyDown={e => { if (e.key === "Enter") { e.preventDefault(); handleSave(); } if (e.key === "Escape") setNamePrompt(false); }}
        />
        <Button type="button" size="sm" disabled={saving || !profileName.trim()} onClick={handleSave} className="h-8 text-xs">Save</Button>
        <Button type="button" size="sm" variant="ghost" onClick={() => setNamePrompt(false)} className="h-8 text-xs">✕</Button>
      </div>
    );
  }

  return (
    <Button
      type="button" variant="ghost" size="sm"
      className="text-xs text-slate-500 hover:text-slate-800 gap-1.5"
      onClick={() => { setProfileName(formData.name || ""); setNamePrompt(true); }}
    >
      <BookOpen className="w-3.5 h-3.5" />
      Save as Profile
    </Button>
  );
}

const PLATFORM_TEMPLATES = {
  sql_server:  { port: 1433, auth_method: "password", host: "" },
  oracle:      { port: 1521, auth_method: "password", host: "" },
  postgresql:  { port: 5432, auth_method: "password", host: "localhost", database: "postgres" },
  mysql:       { port: 3306, auth_method: "password", host: "localhost" },
  mongodb:     { port: 27017, auth_method: "password", host: "localhost", database: "admin" },
  adls2:       { auth_method: "managed_identity", region: "eastus", host: "https://<account>.dfs.core.windows.net" },
  s3:          { auth_method: "key", region: "us-east-1", host: "s3.amazonaws.com" },
  flat_file_delimited: { auth_method: "none", file_config: { delimiter: ",", encoding: "UTF-8", has_header: true, quote_char: '"', escape_char: "\\", file_pattern: "*.csv", nas_path: "", archive_path: "" } },
  flat_file_fixed_width: { auth_method: "none", file_config: { encoding: "UTF-8", record_length: 80, file_pattern: "*.dat", nas_path: "", archive_path: "" } },
  cobol_ebcdic: { auth_method: "none", file_config: { encoding: "EBCDIC-US", record_length: 80, file_pattern: "*.dat", copybook_path: "", nas_path: "", archive_path: "" } },
  sftp:   { port: 22, auth_method: "sftp_key" },
  nas:    { auth_method: "none", file_config: { encoding: "UTF-8", file_pattern: "*", nas_path: "", archive_path: "" } },
  local_fs: { auth_method: "none", file_config: { encoding: "UTF-8", file_pattern: "*", nas_path: "", archive_path: "" } },
};

const DEFAULT_VAULT_CONFIG = {
  vault_url: "https://hcvault.us.bank-dns.com/",
  vault_namespace: "",
  vault_role_id: "",
  vault_secret_id: "",
  vault_mount_point: "secret",
  vault_secret_path: "",
};

const defaultFormData = {
  name: "", source_system_name: "", description: "", car_id: "",
  platform: "", host: "", port: "", database: "",
  username: "", password: "", connection_string: "", auth_method: "password",
  region: "", bucket_container: "", ssl: false,
  status: "active", notes: "", tags: [],
  vault_config: { ...DEFAULT_VAULT_CONFIG },
  file_config: { delimiter: ",", encoding: "UTF-8", has_header: true, quote_char: '"', escape_char: "\\", record_length: "", copybook_path: "", file_pattern: "*", nas_path: "", archive_path: "" }
};

export default function Connections() {
  const { user, scope } = useTenant();
  const { retry } = useRetry();
  const { data: cachedConnections = [], loading: cacheLoading, revalidate } = useCache(
    'connections',
    () => dataflow.entities.Connection.list(),
    { staleTime: 5 * 60 * 1000 }
  );
  const [prereqs, setPrereqs] = useState([]);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [groupByTag, setGroupByTag] = useState(false);
  const [viewMode, setViewMode] = useState("list"); // "grid" | "list"
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 25;
  const [tagInput, setTagInput] = useState("");
  const [dialogOpen, setDialogOpen] = useState(false);
  const [prereqDialogConn, setPrereqDialogConn] = useState(null);
  const [editingConnection, setEditingConnection] = useState(null);
  const [formData, setFormData] = useState(defaultFormData);
  const [saving, setSaving] = useState(false);
  const [formTab, setFormTab] = useState("general");
  const [testingId, setTestingId] = useState(null);
  const [testResult, setTestResult] = useState(null);
  const [vaultTesting, setVaultTesting] = useState(false);
  const [vaultTestResult, setVaultTestResult] = useState(null);

  const connections = scope(cachedConnections || []);
  const loading = cacheLoading;

  useEffect(() => { loadPrereqs(); }, []);

  const loadPrereqs = async () => {
    try {
      const data = await dataflow.entities.ConnectionPrerequisite.list();
      setPrereqs(data);
    } catch (err) {
      console.error("[Connections] loadPrereqs error:", err);
    }
  };

  const loadData = async () => {
    try {
      revalidate();
      await loadPrereqs();
    } catch (err) {
      setError(err?.message || "Failed to refresh connections");
    }
  };

  const getPrereqSummary = (connectionId) => {
    const items = prereqs.filter(p => p.connection_id === connectionId);
    const done = items.filter(p => p.status === "completed" || p.status === "not_required").length;
    return { total: items.length, done, pending: items.filter(p => p.status === "pending").length };
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!formData.name?.trim()) { toast.error("Connection name is required"); return; }
    if (!formData.platform) { toast.error("Platform is required"); return; }
    const duplicateName = connections.some(c =>
      c.name?.toLowerCase() === formData.name.trim().toLowerCase() &&
      c.id !== editingConnection?.id
    );
    if (duplicateName) { toast.error(`A connection with the name "${formData.name.trim()}" already exists.`); return; }

    setSaving(true);
    const payload = { ...formData, port: formData.port ? Number(formData.port) : null, last_tested: null };

    try {
      if (editingConnection) {
        await retry(() => dataflow.entities.Connection.update(editingConnection.id, payload));
        dataflow.entities.ActivityLog.create({ log_type: "info", category: "connection", connection_id: editingConnection.id, message: `Connection "${formData.name}" updated` }).catch(() => {});
        toast.success("Connection updated");
      } else {
        const created = await retry(() => dataflow.entities.Connection.create(payload));
        dataflow.entities.ActivityLog.create({ log_type: "success", category: "connection", connection_id: created.id, message: `Connection "${formData.name}" created` }).catch(() => {});
        toast.success("Connection created");
      }
      setDialogOpen(false);
      setEditingConnection(null);
      setFormData(defaultFormData);
      loadData();
    } catch (err) {
      console.error("[Connections] handleSubmit error:", err);
      toast.error(err.message || (editingConnection ? "Failed to update connection" : "Failed to create connection"));
    } finally {
      setSaving(false);
    }
  };

  const handleEdit = (connection) => {
    setEditingConnection(connection);
    setFormData({
      name: connection.name || "", source_system_name: connection.source_system_name || "",
      description: connection.description || "", car_id: connection.car_id || "",
      platform: connection.platform || "",
      host: connection.host || "", port: connection.port || "", database: connection.database || "",
      username: connection.username || "", password: connection.password || "",
      connection_string: connection.connection_string || "",
      auth_method: connection.auth_method || "password",
      ssl: connection.ssl || false,
      region: connection.region || "", bucket_container: connection.bucket_container || "",
      status: connection.status || "active", notes: connection.notes || "",
      tags: connection.tags || [],
      vault_config: connection.vault_config || { ...DEFAULT_VAULT_CONFIG },
      file_config: connection.file_config || defaultFormData.file_config
    });
    setFormTab("general");
    setDialogOpen(true);
  };

  const handleDelete = async (connection) => {
    if (!confirm(`Delete connection "${connection.name}"?`)) return;
    try {
      await retry(() => dataflow.entities.Connection.delete(connection.id));
      dataflow.entities.ActivityLog.create({ log_type: "warning", category: "connection", message: `Connection "${connection.name}" deleted` }).catch(() => {});
      toast.success("Connection deleted");
      loadData();
    } catch (err) {
      console.error("[Connections] handleDelete error:", err);
      toast.error("Failed to delete connection");
    }
  };

  const handleTestConnection = async (connection) => {
    setTestingId(connection.id);
    setTestResult(null);
    try {
      const response = await fetch('/api/test-connection', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          platform: connection.platform,
          host: connection.host,
          port: connection.port,
          database: connection.database,
          username: connection.username,
          password: connection.password,
          connection_string: connection.connection_string,
          auth_method: connection.auth_method,
          ssl: connection.ssl,
          region: connection.region,
          bucket_container: connection.bucket_container,
          file_config: connection.file_config,
          vault_config: connection.vault_config,
        }),
      });
      const result = await response.json();

      await retry(() => dataflow.entities.Connection.update(connection.id, {
        status: result.success ? "active" : "error",
        last_tested: new Date().toISOString(),
      }));
      dataflow.entities.ActivityLog.create({
        log_type: result.success ? "success" : "error",
        category: "connection",
        connection_id: connection.id,
        message: result.success
          ? `Connection "${connection.name}" test passed (${result.latency_ms}ms) — ${result.server_version || 'OK'}`
          : `Connection "${connection.name}" test failed: ${result.error_message || 'Unknown error'}`,
      }).catch(() => {});

      setTestResult({ connection, ...result });
      loadData();
    } catch (err) {
      console.error("[Connections] handleTestConnection error:", err);
      setTestResult({
        connection,
        success: false,
        latency_ms: 0,
        error_code: 'NETWORK_ERROR',
        error_message: `Could not reach the test endpoint: ${err.message}`,
      });
    } finally {
      setTestingId(null);
    }
  };

  const filteredConnections = useMemo(() => connections.filter(c => {
    const term = searchTerm.toLowerCase();
    const matchesSearch = !term ||
      (c.name || "").toLowerCase().includes(term) ||
      (c.platform || "").toLowerCase().includes(term) ||
      (c.tags || []).some(tag => tag.toLowerCase().includes(term));
    return matchesSearch;
  }), [connections, searchTerm]);

  // Collect all unique tags across all connections
  const allTags = [...new Set(connections.flatMap(c => c.tags || []))].sort();

  // Group filtered connections by tag (connections with no tags go under "Untagged")
  const totalPages = Math.max(1, Math.ceil(filteredConnections.length / itemsPerPage));
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const paginatedConnections = useMemo(() => {
    const start = (safeCurrentPage - 1) * itemsPerPage;
    return filteredConnections.slice(start, start + itemsPerPage);
  }, [filteredConnections, safeCurrentPage, itemsPerPage]);

  useEffect(() => { setCurrentPage(1); }, [searchTerm]);

  const groupedByTag = groupByTag ? paginatedConnections.reduce((acc, c) => {
    const tags = c.tags?.length ? c.tags : ["Untagged"];
    tags.forEach(tag => {
      if (!acc[tag]) acc[tag] = [];
      acc[tag].push(c);
    });
    return acc;
  }, {}) : null;

  const isCloudPlatform = formData.platform === "adls2" || formData.platform === "s3";
  const isFilePlatform = ["flat_file_delimited", "flat_file_fixed_width", "cobol_ebcdic", "sftp", "nas", "local_fs"].includes(formData.platform);
  const isFixedOrCobol = formData.platform === "flat_file_fixed_width" || formData.platform === "cobol_ebcdic";
  const isDelimited = formData.platform === "flat_file_delimited";
  const isCobol = formData.platform === "cobol_ebcdic";

  if (loading) {
    return (
      <div className="flex items-center justify-center h-[60vh]">
        <div className="w-8 h-8 border-4 border-slate-200 border-t-blue-600 rounded-full animate-spin" />
      </div>
    );
  }

  if (error) {
    return <ErrorState title="Failed to load connections" message={error} onRetry={loadData} />;
  }

  return (
    <ErrorBoundary>
      <div className="space-y-6 max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-foreground tracking-tight">Connections</h1>
            <p className="text-muted-foreground mt-0.5">Manage your data connections</p>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={loadData} title="Refresh">
              <RefreshCw className="w-4 h-4" />
            </Button>
            <Button onClick={() => { setEditingConnection(null); setFormData(defaultFormData); setDialogOpen(true); }} className="gap-2">
              <Plus className="w-4 h-4" />
              New Connection
            </Button>
          </div>
        </div>

        {/* Filters */}
        <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center">
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <Input placeholder="Search by name, platform, or tag..." value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} className="pl-10" />
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
              className={`px-2.5 py-1.5 ${viewMode === "grid" ? "bg-[#0060AF] text-white dark:bg-[#0060AF] dark:text-white" : "bg-white dark:bg-slate-800 text-slate-500 hover:bg-slate-50 dark:hover:bg-slate-700"}`}
              onClick={() => setViewMode("grid")} title="Grid view"
            ><LayoutGrid className="w-4 h-4" /></button>
            <button
              className={`px-2.5 py-1.5 ${viewMode === "list" ? "bg-[#0060AF] text-white dark:bg-[#0060AF] dark:text-white" : "bg-white dark:bg-slate-800 text-slate-500 hover:bg-slate-50 dark:hover:bg-slate-700"}`}
              onClick={() => setViewMode("list")} title="List view"
            ><List className="w-4 h-4" /></button>
          </div>
        </div>

        {/* Connection list/grid */}
        {filteredConnections.length > 0 ? (
          groupedByTag ? (
            <div className="space-y-8">
              {Object.entries(groupedByTag)
                .sort(([a], [b]) => a === "Untagged" ? 1 : b === "Untagged" ? -1 : a.localeCompare(b))
                .map(([tag, conns]) => (
                  <div key={tag}>
                    <div className="flex items-center gap-2 mb-3">
                      <Tag className="w-4 h-4 text-slate-400" />
                      <h2 className="text-sm font-semibold text-slate-600 dark:text-slate-300 uppercase tracking-wider">{tag}</h2>
                      <span className="text-xs text-slate-400 bg-slate-100 dark:bg-slate-700 rounded-full px-2 py-0.5">{conns.length}</span>
                    </div>
                    {viewMode === "list" ? (
                      <div className="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
                        {conns.map(connection => (
                          <ConnectionListRow key={connection.id} connection={connection} getPrereqSummary={getPrereqSummary} testingId={testingId} setPrereqDialogConn={setPrereqDialogConn} handleTestConnection={handleTestConnection} handleEdit={handleEdit} handleDelete={handleDelete} />
                        ))}
                      </div>
                    ) : (
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {conns.map(connection => (
                          <ConnectionCard key={connection.id} connection={connection} getPrereqSummary={getPrereqSummary} testingId={testingId} setPrereqDialogConn={setPrereqDialogConn} handleTestConnection={handleTestConnection} handleEdit={handleEdit} handleDelete={handleDelete} />
                        ))}
                      </div>
                    )}
                  </div>
                ))}
            </div>
          ) : viewMode === "list" ? (
            <div className="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden bg-white dark:bg-slate-800">
              <div className="flex items-center gap-4 px-4 py-2 bg-slate-50 dark:bg-slate-700/50 border-b border-slate-200 dark:border-slate-700 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                <div className="w-8 shrink-0" />
                <div className="w-52">Name</div>
                <div className="w-40 hidden sm:block">Platform</div>
                <div className="flex-1 hidden md:block">Host / System</div>
                <div className="w-40 hidden lg:block">Tags</div>
                <div className="shrink-0">Status</div>
                <div className="w-24 hidden xl:block text-right">Last Tested</div>
                <div className="shrink-0 w-44" />
              </div>
              {paginatedConnections.map(connection => (
                <ConnectionListRow key={connection.id} connection={connection} getPrereqSummary={getPrereqSummary} testingId={testingId} setPrereqDialogConn={setPrereqDialogConn} handleTestConnection={handleTestConnection} handleEdit={handleEdit} handleDelete={handleDelete} />
              ))}
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {paginatedConnections.map((connection) => (
                <ConnectionCard key={connection.id} connection={connection} getPrereqSummary={getPrereqSummary} testingId={testingId} setPrereqDialogConn={setPrereqDialogConn} handleTestConnection={handleTestConnection} handleEdit={handleEdit} handleDelete={handleDelete} />
              ))}
            </div>
          )
        ) : (
          <Card>
            <CardContent className="py-12 text-center">
              <div className="w-16 h-16 rounded-2xl bg-blue-50 dark:bg-blue-900/20 flex items-center justify-center mx-auto mb-5">
                <Cable className="w-8 h-8 text-[#0060AF] dark:text-blue-400" />
              </div>
              <h3 className="text-lg font-semibold text-foreground mb-1">No connections found</h3>
              <p className="text-muted-foreground mb-6">{searchTerm ? "Try adjusting your search" : "Create your first connection to get started"}</p>
              {!searchTerm && <Button onClick={() => setDialogOpen(true)} className="gap-2 bg-[#0060AF] hover:bg-[#004d8c] text-white"><Plus className="w-4 h-4" />New Connection</Button>}
            </CardContent>
          </Card>
        )}

        {filteredConnections.length > 0 && totalPages > 1 && (
          <div className="flex items-center justify-between mt-6 px-1">
            <p className="text-sm text-slate-500 dark:text-slate-400">
              Showing {((safeCurrentPage - 1) * itemsPerPage) + 1}–{Math.min(safeCurrentPage * itemsPerPage, filteredConnections.length)} of {filteredConnections.length} connection{filteredConnections.length !== 1 ? "s" : ""}
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

        {/* Connection Form Dialog */}
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogContent className="max-w-xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <div className="flex items-center justify-between">
                <DialogTitle>{editingConnection ? "Edit Connection" : "New Connection"}</DialogTitle>
                {!editingConnection && (
                  <ConnectionProfilePicker onApply={(fields) => setFormData(prev => ({ ...prev, ...fields }))} />
                )}
              </div>
            </DialogHeader>

            <form onSubmit={handleSubmit}>
              <Tabs value={formTab} onValueChange={setFormTab}>
                <TabsList className="grid w-full grid-cols-2 mb-4">
                  <TabsTrigger value="general">General</TabsTrigger>
                  <TabsTrigger value="fileconfig" disabled={!isFilePlatform}>File Config</TabsTrigger>
                </TabsList>

                <TabsContent value="general" className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="col-span-2">
                      <Label>Connection Name *</Label>
                      <Input value={formData.name} onChange={(e) => setFormData({...formData, name: e.target.value})} placeholder="My Database Connection" required />
                    </div>
                    <div>
                      <Label>Source System Name</Label>
                      <Input value={formData.source_system_name} onChange={(e) => setFormData({...formData, source_system_name: e.target.value})} placeholder="System name" />
                    </div>
                    <div>
                      <Label>CarID</Label>
                      <Input value={formData.car_id} onChange={(e) => setFormData({...formData, car_id: e.target.value})} placeholder="CAR ID" />
                    </div>
                    <div className="col-span-2">
                      <Label>Description</Label>
                      <Input value={formData.description} onChange={(e) => setFormData({...formData, description: e.target.value})} placeholder="Connection description" />
                    </div>
                    <div>
                      <Label>Platform *</Label>
                      <Select value={formData.platform} onValueChange={(v) => {
                        const template = PLATFORM_TEMPLATES[v] || {};
                        setFormData(prev => ({ ...defaultFormData, name: prev.name, source_system_name: prev.source_system_name, description: prev.description, car_id: prev.car_id, status: prev.status, notes: prev.notes, platform: v, ...template, file_config: { ...defaultFormData.file_config, ...(template.file_config || {}) } }));
                      }}>
                        <SelectTrigger><SelectValue placeholder="Select platform" /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="sql_server">SQL Server</SelectItem>
                          <SelectItem value="oracle">Oracle</SelectItem>
                          <SelectItem value="postgresql">PostgreSQL</SelectItem>
                          <SelectItem value="mysql">MySQL</SelectItem>
                          <SelectItem value="mongodb">MongoDB</SelectItem>
                          <SelectItem value="adls2">Azure ADLS Gen2</SelectItem>
                          <SelectItem value="s3">AWS S3</SelectItem>
                          <SelectItem value="flat_file_delimited">Flat File – Delimited</SelectItem>
                          <SelectItem value="flat_file_fixed_width">Flat File – Fixed Width</SelectItem>
                          <SelectItem value="cobol_ebcdic">COBOL / EBCDIC</SelectItem>
                          <SelectItem value="sftp">SFTP</SelectItem>
                          <SelectItem value="nas">NAS / Network Share</SelectItem>
                          <SelectItem value="local_fs">Local Filesystem</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    {formData.platform && PLATFORM_TEMPLATES[formData.platform] && !editingConnection && (
                      <div className="col-span-2 flex items-center gap-2 text-xs text-blue-700 bg-blue-50 border border-blue-100 rounded-md px-3 py-2">
                        <span className="shrink-0">✦</span>
                        <span>Default values pre-filled for <strong>{platformConfig[formData.platform]?.label}</strong>. Adjust as needed.</span>
                      </div>
                    )}

                    {!isCloudPlatform && !isFilePlatform && formData.auth_method !== "vault_credentials" && (
                      <>
                        <div><Label>Host</Label><Input value={formData.host} onChange={(e) => setFormData({...formData, host: e.target.value})} placeholder="localhost or IP" /></div>
                        <div><Label>Port</Label><Input type="number" value={formData.port} onChange={(e) => setFormData({...formData, port: e.target.value})} placeholder="1433" /></div>
                        <div className="col-span-2"><Label>Database</Label><Input value={formData.database} onChange={(e) => setFormData({...formData, database: e.target.value})} placeholder="Database name" /></div>
                      </>
                    )}

                    {isCloudPlatform && (
                      <>
                        <div><Label>Region</Label><Input value={formData.region} onChange={(e) => setFormData({...formData, region: e.target.value})} placeholder={formData.platform === "s3" ? "us-east-1" : "eastus"} /></div>
                        <div><Label>{formData.platform === "s3" ? "Bucket" : "Container"}</Label><Input value={formData.bucket_container} onChange={(e) => setFormData({...formData, bucket_container: e.target.value})} placeholder="my-bucket" /></div>
                        <div className="col-span-2"><Label>Endpoint / Account URL</Label><Input value={formData.host} onChange={(e) => setFormData({...formData, host: e.target.value})} /></div>
                      </>
                    )}

                    {isFilePlatform && (
                      <>
                        {formData.platform === "sftp" && (
                          <>
                            <div><Label>SFTP Host</Label><Input value={formData.host} onChange={(e) => setFormData({...formData, host: e.target.value})} placeholder="sftp.example.com" /></div>
                            <div><Label>Port</Label><Input type="number" value={formData.port} onChange={(e) => setFormData({...formData, port: e.target.value})} placeholder="22" /></div>
                          </>
                        )}
                        {(formData.platform === "nas" || formData.platform === "local_fs") && (
                          <div className="col-span-2"><Label>Base Path</Label><Input value={formData.file_config?.nas_path || ""} onChange={(e) => setFormData({...formData, file_config: {...formData.file_config, nas_path: e.target.value}})} placeholder="\\\\server\\share or /mnt/data" /></div>
                        )}
                      </>
                    )}

                    <div>
                      <Label>Auth Method</Label>
                      <Select value={formData.auth_method} onValueChange={(v) => {
                        setFormData({...formData, auth_method: v});
                        setVaultTestResult(null);
                      }}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="password">Password</SelectItem>
                          <SelectItem value="vault_credentials">Vault Credentials (HashiCorp)</SelectItem>
                          <SelectItem value="key">Access Key</SelectItem>
                          <SelectItem value="sftp_key">SFTP Private Key</SelectItem>
                          <SelectItem value="connection_string">Connection String</SelectItem>
                          <SelectItem value="managed_identity">Managed Identity</SelectItem>
                          <SelectItem value="none">None</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    {formData.auth_method === "vault_credentials" ? (
                      <div className="col-span-2 space-y-3">
                        <div className="flex items-center gap-2 text-xs text-amber-700 dark:text-amber-300 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg px-3 py-2">
                          <Lock className="w-3.5 h-3.5 shrink-0" />
                          <span>Credentials are retrieved from HashiCorp Vault at connection time using AppRole authentication. No passwords are stored in DataFlow.</span>
                        </div>
                        <div className="grid grid-cols-2 gap-3 p-3 border border-slate-200 dark:border-slate-700 rounded-lg bg-slate-50/50 dark:bg-slate-800/50">
                          <div className="col-span-2">
                            <Label className="text-xs">Vault URL</Label>
                            <Input value={formData.vault_config?.vault_url || ""} onChange={(e) => setFormData({...formData, vault_config: {...formData.vault_config, vault_url: e.target.value}})} placeholder="https://hcvault.us.bank-dns.com/" className="text-sm" />
                          </div>
                          <div>
                            <Label className="text-xs">Namespace</Label>
                            <Input value={formData.vault_config?.vault_namespace || ""} onChange={(e) => setFormData({...formData, vault_config: {...formData.vault_config, vault_namespace: e.target.value}})} placeholder="8118" className="text-sm" />
                          </div>
                          <div>
                            <Label className="text-xs">Mount Point</Label>
                            <Input value={formData.vault_config?.vault_mount_point || "secret"} onChange={(e) => setFormData({...formData, vault_config: {...formData.vault_config, vault_mount_point: e.target.value}})} placeholder="secret" className="text-sm" />
                          </div>
                          <div>
                            <Label className="text-xs">AppRole Role ID</Label>
                            <Input value={formData.vault_config?.vault_role_id || ""} onChange={(e) => setFormData({...formData, vault_config: {...formData.vault_config, vault_role_id: e.target.value}})} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" className="font-mono text-xs" />
                          </div>
                          <div>
                            <Label className="text-xs">AppRole Secret ID</Label>
                            <Input type="password" value={formData.vault_config?.vault_secret_id || ""} onChange={(e) => setFormData({...formData, vault_config: {...formData.vault_config, vault_secret_id: e.target.value}})} placeholder="••••••••" className="font-mono text-xs" />
                          </div>
                          <div className="col-span-2">
                            <Label className="text-xs">Secret Path</Label>
                            <Input value={formData.vault_config?.vault_secret_path || ""} onChange={(e) => setFormData({...formData, vault_config: {...formData.vault_config, vault_secret_path: e.target.value}})} placeholder="/prod/common/appid/azbdpappidprod" className="font-mono text-sm" />
                            <p className="text-xs text-muted-foreground mt-1">KV v2 path containing username/password keys</p>
                          </div>
                          <div className="col-span-2 flex items-center gap-2">
                            <Button
                              type="button" variant="outline" size="sm"
                              className="text-xs gap-1.5"
                              disabled={vaultTesting || !formData.vault_config?.vault_role_id || !formData.vault_config?.vault_secret_path}
                              onClick={async () => {
                                setVaultTesting(true);
                                setVaultTestResult(null);
                                try {
                                  const res = await fetch('/api/test-vault', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify(formData.vault_config),
                                  });
                                  const result = await res.json();
                                  setVaultTestResult(result);
                                } catch (err) {
                                  setVaultTestResult({ success: false, error: err.message });
                                } finally {
                                  setVaultTesting(false);
                                }
                              }}
                            >
                              {vaultTesting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <KeyRound className="w-3.5 h-3.5" />}
                              Test Vault Connection
                            </Button>
                            {vaultTestResult && (
                              <span className={`text-xs font-medium ${vaultTestResult.success ? "text-emerald-600" : "text-red-600"}`}>
                                {vaultTestResult.success
                                  ? `Connected — found keys: ${vaultTestResult.raw_keys?.join(", ") || "none"}`
                                  : vaultTestResult.error}
                              </span>
                            )}
                          </div>
                        </div>
                        <div className="grid grid-cols-2 gap-3">
                          <div><Label className="text-xs">Host (override or leave for vault)</Label><Input value={formData.host} onChange={(e) => setFormData({...formData, host: e.target.value})} placeholder="From vault or enter manually" className="text-sm" /></div>
                          <div><Label className="text-xs">Port (override or leave for vault)</Label><Input type="number" value={formData.port} onChange={(e) => setFormData({...formData, port: e.target.value})} placeholder="From vault" className="text-sm" /></div>
                          <div className="col-span-2"><Label className="text-xs">Database (override or leave for vault)</Label><Input value={formData.database} onChange={(e) => setFormData({...formData, database: e.target.value})} placeholder="From vault or enter manually" className="text-sm" /></div>
                        </div>
                      </div>
                    ) : formData.auth_method === "connection_string" ? (
                      <div className="col-span-2">
                        <Label>Connection String</Label>
                        <Input type="password" value={formData.connection_string} onChange={(e) => setFormData({...formData, connection_string: e.target.value})} placeholder="postgresql://user:pass@host:5432/db" />
                        <p className="text-xs text-muted-foreground mt-1">Full connection URI overrides host, port, database, and credentials</p>
                      </div>
                    ) : formData.auth_method !== "none" && formData.auth_method !== "managed_identity" ? (
                      <>
                        <div><Label>{formData.auth_method === "key" ? "Access Key ID" : "Username"}</Label><Input value={formData.username} onChange={(e) => setFormData({...formData, username: e.target.value})} placeholder={formData.auth_method === "key" ? "AKIA..." : "Username"} /></div>
                        <div><Label>{formData.auth_method === "key" ? "Secret Access Key" : "Password"}</Label><Input type="password" value={formData.password} onChange={(e) => setFormData({...formData, password: e.target.value})} placeholder="••••••••" /></div>
                      </>
                    ) : formData.auth_method !== "managed_identity" ? null : (
                      <div className="col-span-2 text-xs text-muted-foreground bg-blue-50 dark:bg-blue-900/20 border border-blue-100 dark:border-blue-800 rounded-md px-3 py-2">
                        Managed Identity authentication uses the host environment's identity. No credentials are needed.
                      </div>
                    )}
                    {!isFilePlatform && formData.auth_method !== "connection_string" && (
                      <div className="flex items-center gap-3 col-span-2">
                        <Switch checked={formData.ssl || false} onCheckedChange={v => setFormData({...formData, ssl: v})} />
                        <Label>Use SSL / TLS encryption</Label>
                      </div>
                    )}
                    <div>
                      <Label>Status</Label>
                      <Select value={formData.status} onValueChange={(v) => setFormData({...formData, status: v})}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="active">Active</SelectItem>
                          <SelectItem value="inactive">Inactive</SelectItem>
                          <SelectItem value="pending_setup">Pending Setup</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="col-span-2"><Label>Notes</Label><Textarea value={formData.notes} onChange={(e) => setFormData({...formData, notes: e.target.value})} placeholder="Optional notes..." rows={2} /></div>
                    <div className="col-span-2">
                      <Label>Tags</Label>
                      <div className="flex flex-wrap gap-1.5 mb-2">
                        {(formData.tags || []).map(tag => (
                          <span key={tag} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs bg-blue-50 text-blue-700 border border-blue-100">
                            {tag}
                            <button type="button" onClick={() => setFormData(f => ({ ...f, tags: f.tags.filter(t => t !== tag) }))} className="hover:text-red-500">
                              <X className="w-3 h-3" />
                            </button>
                          </span>
                        ))}
                      </div>
                      <div className="flex gap-2">
                        <Input
                          value={tagInput}
                          onChange={e => setTagInput(e.target.value)}
                          placeholder="Add a tag..."
                          className="h-8 text-sm"
                          onKeyDown={e => {
                            if ((e.key === "Enter" || e.key === ",") && tagInput.trim()) {
                              e.preventDefault();
                              const newTag = tagInput.trim();
                              if (!formData.tags?.includes(newTag)) {
                                setFormData(f => ({ ...f, tags: [...(f.tags || []), newTag] }));
                              }
                              setTagInput("");
                            }
                          }}
                        />
                        <Button type="button" size="sm" variant="outline" className="h-8 text-xs" onClick={() => {
                          const newTag = tagInput.trim();
                          if (newTag && !formData.tags?.includes(newTag)) {
                            setFormData(f => ({ ...f, tags: [...(f.tags || []), newTag] }));
                          }
                          setTagInput("");
                        }}>Add</Button>
                      </div>
                    </div>
                  </div>
                </TabsContent>

                <TabsContent value="fileconfig" className="space-y-4">
                  {isDelimited && (
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label>Delimiter</Label>
                        <Select value={formData.file_config?.delimiter || ","} onValueChange={v => setFormData({...formData, file_config: {...formData.file_config, delimiter: v}})}>
                          <SelectTrigger><SelectValue /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value=",">Comma (,)</SelectItem>
                            <SelectItem value="|">Pipe (|)</SelectItem>
                            <SelectItem value="\t">Tab (\t)</SelectItem>
                            <SelectItem value=";">Semicolon (;)</SelectItem>
                            <SelectItem value=" ">Space</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div><Label>Quote Character</Label><Input value={formData.file_config?.quote_char || '"'} onChange={e => setFormData({...formData, file_config: {...formData.file_config, quote_char: e.target.value}})} /></div>
                      <div><Label>Escape Character</Label><Input value={formData.file_config?.escape_char || "\\"} onChange={e => setFormData({...formData, file_config: {...formData.file_config, escape_char: e.target.value}})} /></div>
                      <div className="flex items-center gap-3 pt-6"><Switch checked={formData.file_config?.has_header ?? true} onCheckedChange={v => setFormData({...formData, file_config: {...formData.file_config, has_header: v}})} /><Label>Has Header Row</Label></div>
                    </div>
                  )}
                  {isFixedOrCobol && (
                    <div className="grid grid-cols-2 gap-4">
                      <div><Label>Record Length (bytes)</Label><Input type="number" value={formData.file_config?.record_length || ""} onChange={e => setFormData({...formData, file_config: {...formData.file_config, record_length: e.target.value}})} placeholder="80" /></div>
                      {isCobol && <div className="col-span-2"><Label>Copybook Path</Label><Input value={formData.file_config?.copybook_path || ""} onChange={e => setFormData({...formData, file_config: {...formData.file_config, copybook_path: e.target.value}})} placeholder="/layouts/CUSTMAST.cbl" /></div>}
                    </div>
                  )}
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <Label>Encoding</Label>
                      <Select value={formData.file_config?.encoding || "UTF-8"} onValueChange={v => setFormData({...formData, file_config: {...formData.file_config, encoding: v}})}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="UTF-8">UTF-8</SelectItem>
                          <SelectItem value="UTF-16">UTF-16</SelectItem>
                          <SelectItem value="ISO-8859-1">ISO-8859-1</SelectItem>
                          <SelectItem value="EBCDIC-US">EBCDIC US</SelectItem>
                          <SelectItem value="EBCDIC-UK">EBCDIC UK</SelectItem>
                          <SelectItem value="ASCII">ASCII</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div><Label>File Pattern</Label><Input value={formData.file_config?.file_pattern || "*"} onChange={e => setFormData({...formData, file_config: {...formData.file_config, file_pattern: e.target.value}})} placeholder="*.csv" /></div>
                    <div className="col-span-2"><Label>Source Path</Label><Input value={formData.file_config?.nas_path || ""} onChange={e => setFormData({...formData, file_config: {...formData.file_config, nas_path: e.target.value}})} placeholder="\\\\nas01\\data or /mnt/data/in" /></div>
                    <div className="col-span-2"><Label>Archive Path</Label><Input value={formData.file_config?.archive_path || ""} onChange={e => setFormData({...formData, file_config: {...formData.file_config, archive_path: e.target.value}})} placeholder="\\\\nas01\\archive" /></div>
                  </div>
                </TabsContent>
              </Tabs>

              <div className="flex items-center justify-between pt-4 mt-4 border-t">
                <div className="flex items-center gap-2">
                  <SaveAsProfileButton formData={formData} />
                  {formData.platform && (
                    <Button
                      type="button" variant="outline" size="sm"
                      className="text-xs gap-1.5"
                      disabled={testingId === 'form'}
                      onClick={async () => {
                        setTestingId('form');
                        try {
                          const response = await fetch('/api/test-connection', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                              platform: formData.platform,
                              host: formData.host,
                              port: formData.port,
                              database: formData.database,
                              username: formData.username,
                              password: formData.password,
                              connection_string: formData.connection_string,
                              auth_method: formData.auth_method,
                              ssl: formData.ssl,
                              region: formData.region,
                              bucket_container: formData.bucket_container,
                              file_config: formData.file_config,
                              vault_config: formData.vault_config,
                            }),
                          });
                          const result = await response.json();
                          setTestResult({ connection: { ...formData, id: 'form-test' }, ...result });
                        } catch (err) {
                          setTestResult({
                            connection: { ...formData, id: 'form-test' },
                            success: false, latency_ms: 0,
                            error_code: 'NETWORK_ERROR',
                            error_message: err.message,
                          });
                        } finally {
                          setTestingId(null);
                        }
                      }}
                    >
                      {testingId === 'form' ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Wifi className="w-3.5 h-3.5" />}
                      Test
                    </Button>
                  )}
                </div>
                <div className="flex gap-3">
                  <Button type="button" variant="outline" onClick={() => setDialogOpen(false)}>Cancel</Button>
                  <Button type="submit" disabled={saving}>{saving ? "Saving..." : editingConnection ? "Update" : "Create"}</Button>
                </div>
              </div>
            </form>
          </DialogContent>
        </Dialog>

        {/* Test Result Dialog */}
        <Dialog open={!!testResult} onOpenChange={(open) => !open && setTestResult(null)}>
          <DialogContent className="max-w-md">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                {testResult?.success ? <CheckCircle2 className="w-5 h-5 text-emerald-500" /> : <XCircle className="w-5 h-5 text-red-500" />}
                Connection Test — {testResult?.connection?.name}
              </DialogTitle>
            </DialogHeader>
            {testResult && (
              <div className="space-y-4 mt-2">
                <div className={`rounded-lg p-4 flex items-start gap-3 ${testResult.success ? "bg-emerald-50 border border-emerald-200" : "bg-red-50 border border-red-200"}`}>
                  {testResult.success ? <CheckCircle2 className="w-5 h-5 text-emerald-600 mt-0.5 shrink-0" /> : <XCircle className="w-5 h-5 text-red-600 mt-0.5 shrink-0" />}
                  <div>
                    <p className={`font-semibold text-sm ${testResult.success ? "text-emerald-800" : "text-red-800"}`}>{testResult.success ? "Connection Successful" : "Connection Failed"}</p>
                    {testResult.success && testResult.server_version && <p className="text-xs text-emerald-700 mt-1">{testResult.server_version}</p>}
                    {!testResult.success && testResult.error_message && <p className="text-xs text-red-700 mt-1">{testResult.error_message}</p>}
                  </div>
                </div>
                <div className="space-y-2 text-sm">
                  {testResult.latency_ms > 0 && <div className="flex items-center justify-between py-2 border-b border-slate-100"><span className="text-slate-500">Latency</span><span className="font-medium">{testResult.latency_ms} ms</span></div>}
                  {!testResult.success && testResult.error_code && <div className="flex items-center justify-between py-2 border-b border-slate-100"><span className="text-slate-500">Error Code</span><code className="text-xs bg-red-50 text-red-700 px-2 py-0.5 rounded font-mono">{testResult.error_code}</code></div>}
                  <div className="flex items-center justify-between py-2 border-b border-slate-100"><span className="text-slate-500">Platform</span><span className="font-medium">{platformConfig[testResult.connection?.platform]?.label || testResult.connection?.platform}</span></div>
                  <div className="flex items-center justify-between py-2"><span className="text-slate-500">Tested at</span><span>{moment().format("HH:mm:ss")}</span></div>
                </div>
                <div className="flex justify-end pt-2"><Button onClick={() => setTestResult(null)}>Close</Button></div>
              </div>
            )}
          </DialogContent>
        </Dialog>

        {/* Prerequisites Dialog */}
        <Dialog open={!!prereqDialogConn} onOpenChange={(open) => !open && setPrereqDialogConn(null)}>
          <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <Shield className="w-5 h-5 text-blue-600" />
                Setup Prerequisites — {prereqDialogConn?.name}
              </DialogTitle>
            </DialogHeader>
            {prereqDialogConn && (
              <ErrorBoundary>
                <PrerequisitePanel connection={prereqDialogConn} onUpdate={loadData} />
              </ErrorBoundary>
            )}
          </DialogContent>
        </Dialog>
      </div>
    </ErrorBoundary>
  );
}