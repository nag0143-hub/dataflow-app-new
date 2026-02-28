import { useState, useEffect } from "react";
import { dataflow } from '@/api/client';
import {
  Shield, Network, Database, Server, FolderOpen, Key, Lock, Plus, Edit, Trash2,
  CheckCircle2, Clock, XCircle, AlertCircle, MinusCircle, ChevronDown
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue
} from "@/components/ui/select";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import moment from "moment";

const prereqTypeConfig = {
  nsg_rule:           { icon: Shield,     label: "NSG Rule",                    color: "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300" },
  egress_rule:        { icon: Network,    label: "Egress Rule",                 color: "bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300" },
  nas_path:           { icon: FolderOpen, label: "NAS Path",                    color: "bg-lime-100 text-lime-700 dark:bg-lime-900/40 dark:text-lime-300" },
  dba_access:         { icon: Database,   label: "DBA Access",                  color: "bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300" },
  firewall_rule:      { icon: Lock,       label: "Firewall Rule",               color: "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300" },
  service_account:    { icon: Key,        label: "Service Account",             color: "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300" },
  vpn_tunnel:         { icon: Server,     label: "VPN Tunnel",                  color: "bg-teal-100 text-teal-700 dark:bg-teal-900/40 dark:text-teal-300" },
  vault_credentials:  { icon: Lock,       label: "Vault / HashiCorp Credentials", color: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-300" },
  app_id:             { icon: Key,        label: "App ID / Client Registration", color: "bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300" },
  entitlement:        { icon: Shield,     label: "Entitlement to Resources",    color: "bg-cyan-100 text-cyan-700 dark:bg-cyan-900/40 dark:text-cyan-300" },
  other:              { icon: AlertCircle,label: "Other",                       color: "bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300" },
};

const teamConfig = {
  networking: { label: "Networking", color: "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-700" },
  dba:        { label: "DBA",        color: "bg-orange-50 text-orange-700 border-orange-200 dark:bg-orange-900/30 dark:text-orange-300 dark:border-orange-700" },
  security:   { label: "Security",   color: "bg-red-50 text-red-700 border-red-200 dark:bg-red-900/30 dark:text-red-300 dark:border-red-700" },
  storage:    { label: "Storage",    color: "bg-lime-50 text-lime-700 border-lime-200 dark:bg-lime-900/30 dark:text-lime-300 dark:border-lime-700" },
  platform:   { label: "Platform",   color: "bg-purple-50 text-purple-700 border-purple-200 dark:bg-purple-900/30 dark:text-purple-300 dark:border-purple-700" },
  other:      { label: "Other",      color: "bg-slate-50 text-slate-600 border-slate-200 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-700" },
};

const statusConfig = {
  pending:      { icon: Clock,        label: "Pending",      color: "text-amber-600 dark:text-amber-400",  bg: "bg-amber-50 dark:bg-amber-900/20" },
  in_progress:  { icon: AlertCircle,  label: "In Progress",  color: "text-blue-600 dark:text-blue-400",   bg: "bg-blue-50 dark:bg-blue-900/20" },
  completed:    { icon: CheckCircle2, label: "Completed",    color: "text-emerald-600 dark:text-emerald-400",bg: "bg-emerald-50 dark:bg-emerald-900/20" },
  rejected:     { icon: XCircle,      label: "Rejected",     color: "text-red-600 dark:text-red-400",    bg: "bg-red-50 dark:bg-red-900/20" },
  not_required: { icon: MinusCircle,  label: "Not Required", color: "text-slate-400 dark:text-slate-500",  bg: "bg-slate-50 dark:bg-slate-800" },
};

const defaultForm = {
  prereq_type: "nsg_rule",
  title: "",
  description: "",
  assigned_team: "networking",
  status: "pending",
  priority: "medium",
  ticket_reference: "",
  due_date: "",
  notes: ""
};

// Suggested prerequisites per platform
const platformSuggestions = {
  sql_server:   ["egress_rule", "nsg_rule", "firewall_rule", "dba_access", "service_account", "vault_credentials", "app_id"],
  oracle:       ["egress_rule", "nsg_rule", "firewall_rule", "dba_access", "service_account", "vault_credentials", "app_id"],
  postgresql:   ["egress_rule", "nsg_rule", "firewall_rule", "dba_access", "vault_credentials", "app_id"],
  mysql:        ["egress_rule", "nsg_rule", "firewall_rule", "dba_access", "vault_credentials", "app_id"],
  mongodb:      ["egress_rule", "nsg_rule", "firewall_rule", "vault_credentials", "app_id"],
  adls2:        ["egress_rule", "service_account", "vault_credentials", "app_id", "entitlement"],
  s3:           ["egress_rule", "service_account", "vault_credentials", "app_id", "entitlement"],
  flat_file_delimited:   ["nas_path", "egress_rule"],
  flat_file_fixed_width: ["nas_path", "egress_rule"],
  cobol_ebcdic:          ["nas_path", "egress_rule"],
  sftp:         ["egress_rule", "firewall_rule", "service_account", "vault_credentials"],
  nas:          ["nas_path", "nsg_rule"],
  local_fs:     [],
};

export default function PrerequisitePanel({ connection }) {
  const [prereqs, setPrereqs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingPrereq, setEditingPrereq] = useState(null);
  const [formData, setFormData] = useState(defaultForm);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadPrereqs();
  }, [connection.id]);

  const loadPrereqs = async () => {
    const data = await dataflow.entities.ConnectionPrerequisite.filter({ connection_id: connection.id });
    setPrereqs(data);
    setLoading(false);
  };

  const openNew = (suggestedType = null) => {
    setEditingPrereq(null);
    setFormData({ ...defaultForm, prereq_type: suggestedType || "nsg_rule" });
    setDialogOpen(true);
  };

  const openEdit = (prereq) => {
    setEditingPrereq(prereq);
    setFormData({
      prereq_type: prereq.prereq_type,
      title: prereq.title || "",
      description: prereq.description || "",
      assigned_team: prereq.assigned_team,
      status: prereq.status,
      priority: prereq.priority || "medium",
      ticket_reference: prereq.ticket_reference || "",
      due_date: prereq.due_date ? prereq.due_date.split("T")[0] : "",
      notes: prereq.notes || ""
    });
    setDialogOpen(true);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSaving(true);

    const payload = {
      ...formData,
      connection_id: connection.id,
      due_date: formData.due_date ? new Date(formData.due_date).toISOString() : null,
      completed_at: formData.status === "completed" ? new Date().toISOString() : null,
      requested_by: ""
    };

    if (editingPrereq) {
      await dataflow.entities.ConnectionPrerequisite.update(editingPrereq.id, payload);
      toast.success("Prerequisite updated");
    } else {
      await dataflow.entities.ConnectionPrerequisite.create(payload);
      toast.success("Prerequisite added");
    }

    setSaving(false);
    setDialogOpen(false);
    loadPrereqs();
  };

  const handleDelete = async (prereq) => {
    if (!confirm("Delete this prerequisite?")) return;
    await dataflow.entities.ConnectionPrerequisite.delete(prereq.id);
    toast.success("Deleted");
    loadPrereqs();
  };

  const handleStatusChange = async (prereq, newStatus) => {
    await dataflow.entities.ConnectionPrerequisite.update(prereq.id, {
      status: newStatus,
      completed_at: newStatus === "completed" ? new Date().toISOString() : null
    });
    loadPrereqs();
  };

  const suggestions = (platformSuggestions[connection.platform] || []).filter(
    type => !prereqs.some(p => p.prereq_type === type)
  );

  const completedCount = prereqs.filter(p => p.status === "completed" || p.status === "not_required").length;
  const allDone = prereqs.length > 0 && completedCount === prereqs.length;

  return (
    <div className="space-y-4">
      {/* Summary Bar */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-sm text-slate-600">
            <span className="font-semibold text-slate-900">{completedCount}/{prereqs.length}</span> prerequisites done
          </span>
          {allDone && prereqs.length > 0 && (
            <Badge className="bg-emerald-100 text-emerald-700 border-0">Ready to connect</Badge>
          )}
          {!allDone && prereqs.some(p => p.status === "pending") && (
            <Badge className="bg-amber-100 text-amber-700 border-0">Awaiting actions</Badge>
          )}
        </div>
        <Button size="sm" onClick={() => openNew()} className="gap-1">
          <Plus className="w-3.5 h-3.5" />
          Add
        </Button>
      </div>

      {/* Suggestions */}
      {suggestions.length > 0 && (
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-3">
          <p className="text-xs font-medium text-blue-700 dark:text-blue-300 mb-2">Suggested prerequisites for this platform:</p>
          <div className="flex flex-wrap gap-2">
            {suggestions.map(type => {
              const cfg = prereqTypeConfig[type];
              return (
                <button
                  key={type}
                  onClick={() => openNew(type)}
                  className="flex items-center gap-1.5 bg-white dark:bg-slate-800 border border-blue-200 dark:border-blue-700 text-blue-700 dark:text-blue-300 rounded-md px-2 py-1 text-xs hover:bg-blue-50 dark:hover:bg-blue-900/30 transition-colors"
                >
                  <Plus className="w-3 h-3" />
                  {cfg.label}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Prereq List */}
      {loading ? (
        <div className="h-24 flex items-center justify-center">
          <div className="w-6 h-6 border-2 border-slate-200 border-t-blue-600 rounded-full animate-spin" />
        </div>
      ) : prereqs.length === 0 ? (
        <div className="text-center py-8 text-slate-400 border border-dashed border-slate-200 rounded-lg">
          <Shield className="w-8 h-8 mx-auto mb-2 opacity-40" />
          <p className="text-sm">No prerequisites added yet</p>
        </div>
      ) : (
        <div className="space-y-2">
          {prereqs.map(prereq => {
            const typeCfg = prereqTypeConfig[prereq.prereq_type] || prereqTypeConfig.other;
            const statCfg = statusConfig[prereq.status] || statusConfig.pending;
            const teamCfg = teamConfig[prereq.assigned_team] || teamConfig.other;
            const TypeIcon = typeCfg.icon;
            const StatIcon = statCfg.icon;

            return (
              <div key={prereq.id} className={cn("border rounded-lg p-3 transition-colors", statCfg.bg, "border-slate-200")}>
                <div className="flex items-start gap-3">
                  <div className={cn("w-8 h-8 rounded-lg flex items-center justify-center shrink-0 mt-0.5", typeCfg.color)}>
                    <TypeIcon className="w-4 h-4" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-start justify-between gap-2">
                      <div>
                        <p className="font-medium text-slate-900 text-sm">{prereq.title}</p>
                        {prereq.description && (
                          <p className="text-xs text-slate-500 mt-0.5 line-clamp-2">{prereq.description}</p>
                        )}
                      </div>
                      <div className="flex items-center gap-1 shrink-0">
                        <button onClick={() => openEdit(prereq)} className="p-1 text-slate-400 hover:text-slate-600">
                          <Edit className="w-3.5 h-3.5" />
                        </button>
                        <button onClick={() => handleDelete(prereq)} className="p-1 text-slate-400 hover:text-red-500">
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </div>
                    <div className="flex items-center gap-2 mt-2 flex-wrap">
                      <Badge variant="outline" className={cn("text-xs py-0", teamCfg.color)}>
                        {teamCfg.label}
                      </Badge>
                      {prereq.ticket_reference && (
                        <span className="text-xs text-slate-400 font-mono">{prereq.ticket_reference}</span>
                      )}
                      {prereq.due_date && (
                        <span className={cn("text-xs", moment(prereq.due_date).isBefore(moment()) && prereq.status !== "completed" ? "text-red-500" : "text-slate-400")}>
                          Due {moment(prereq.due_date).format("MMM D")}
                        </span>
                      )}
                      {/* Quick status change */}
                      <Select value={prereq.status} onValueChange={(v) => handleStatusChange(prereq, v)}>
                        <SelectTrigger className={cn("h-6 text-xs border-0 p-1 pr-6 font-medium", statCfg.color)}>
                          <div className="flex items-center gap-1">
                            <StatIcon className="w-3 h-3" />
                            {statCfg.label}
                          </div>
                        </SelectTrigger>
                        <SelectContent>
                          {Object.entries(statusConfig).map(([key, cfg]) => (
                            <SelectItem key={key} value={key} className="text-xs">{cfg.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    {prereq.notes && prereq.status === "completed" && (
                      <p className="text-xs text-emerald-600 mt-1">✓ {prereq.notes}</p>
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Add/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>{editingPrereq ? "Edit Prerequisite" : "Add Prerequisite"}</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleSubmit} className="space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label>Type</Label>
                <Select value={formData.prereq_type} onValueChange={v => setFormData({...formData, prereq_type: v})}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Object.entries(prereqTypeConfig).map(([k, cfg]) => (
                      <SelectItem key={k} value={k}>{cfg.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label>Assigned Team</Label>
                <Select value={formData.assigned_team} onValueChange={v => setFormData({...formData, assigned_team: v})}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Object.entries(teamConfig).map(([k, cfg]) => (
                      <SelectItem key={k} value={k}>{cfg.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div>
              <Label>Title</Label>
              <Input
                value={formData.title}
                onChange={e => setFormData({...formData, title: e.target.value})}
                placeholder="e.g. Allow port 1433 from app subnet"
                required
              />
            </div>

            <div>
              <Label>Description / Details</Label>
              <Textarea
                value={formData.description}
                onChange={e => setFormData({...formData, description: e.target.value})}
                placeholder="Source IP, destination, port ranges, NAS path, etc."
                rows={2}
              />
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label>Priority</Label>
                <Select value={formData.priority} onValueChange={v => setFormData({...formData, priority: v})}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="high">High</SelectItem>
                    <SelectItem value="medium">Medium</SelectItem>
                    <SelectItem value="low">Low</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label>Status</Label>
                <Select value={formData.status} onValueChange={v => setFormData({...formData, status: v})}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Object.entries(statusConfig).map(([k, cfg]) => (
                      <SelectItem key={k} value={k}>{cfg.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label>Ticket / JIRA Ref</Label>
                <Input
                  value={formData.ticket_reference}
                  onChange={e => setFormData({...formData, ticket_reference: e.target.value})}
                  placeholder="INFRA-1234"
                />
              </div>
              <div>
                <Label>Due Date</Label>
                <Input
                  type="date"
                  value={formData.due_date}
                  onChange={e => setFormData({...formData, due_date: e.target.value})}
                />
              </div>
            </div>

            <div>
              <Label>Notes / Resolution</Label>
              <Input
                value={formData.notes}
                onChange={e => setFormData({...formData, notes: e.target.value})}
                placeholder="e.g. Approved by Jane, applied in prod NSG"
              />
            </div>

            <div className="flex justify-end gap-2 pt-2">
              <Button type="button" variant="outline" onClick={() => setDialogOpen(false)}>Cancel</Button>
              <Button type="submit" disabled={saving}>{saving ? "Saving..." : editingPrereq ? "Update" : "Add"}</Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}