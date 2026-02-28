import { useState, useCallback } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import {
  Database, Filter, Target, ArrowRight, Plus, Trash2,
  Save, RefreshCw, Zap, GripVertical, ChevronDown, ChevronRight,
  Settings, Play, Edit, CheckCircle, Clock, Shield, Activity
} from "lucide-react";
import { toast } from "sonner";
import { dataflow } from '@/api/client';

// ─── Node type config ────────────────────────────────────────────────────────
const NODE_TYPES = {
  source: {
    label: "Source", icon: Database, color: "bg-blue-500",
    borderColor: "border-blue-400", bgLight: "bg-blue-50 dark:bg-blue-900/20",
    textColor: "text-blue-700 dark:text-blue-300",
  },
  transform: {
    label: "Transform", icon: Filter, color: "bg-amber-500",
    borderColor: "border-amber-400", bgLight: "bg-amber-50 dark:bg-amber-900/20",
    textColor: "text-amber-700 dark:text-amber-300",
  },
  target: {
    label: "Target", icon: Target, color: "bg-emerald-500",
    borderColor: "border-emerald-400", bgLight: "bg-emerald-50 dark:bg-emerald-900/20",
    textColor: "text-emerald-700 dark:text-emerald-300",
  },
};

const TRANSFORM_OPTIONS = [
  { value: "filter", label: "Filter Rows" },
  { value: "rename", label: "Rename Columns" },
  { value: "mask", label: "Data Masking" },
  { value: "dedupe", label: "Deduplication" },
  { value: "aggregate", label: "Aggregation" },
  { value: "join", label: "Join / Lookup" },
];

let nodeIdCounter = Date.now();
function makeNode(type) {
  return { id: `node-${nodeIdCounter++}`, type, config: {} };
}

// ─── Node Config Dialog ───────────────────────────────────────────────────────
function NodeConfigDialog({ node, connections, open, onClose, onSave }) {
  const [cfg, setCfg] = useState(node?.config || {});
  const patch = (k, v) => setCfg(prev => ({ ...prev, [k]: v }));

  if (!node) return null;

  const handleSave = () => { onSave(node.id, cfg); onClose(); };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-lg dark:bg-slate-800 dark:border-slate-700">
        <DialogHeader>
          <DialogTitle className="dark:text-white flex items-center gap-2">
            <Settings className="w-4 h-4" />
            Configure {NODE_TYPES[node.type]?.label} Node
          </DialogTitle>
        </DialogHeader>

        <Tabs defaultValue="basic" className="mt-2">
          <TabsList className="dark:bg-slate-700">
            <TabsTrigger value="basic">Basic</TabsTrigger>
            {node.type === "source" && <TabsTrigger value="datasets">Datasets</TabsTrigger>}
            {node.type === "target" && <TabsTrigger value="advanced">Advanced</TabsTrigger>}
            {node.type === "source" && <TabsTrigger value="schedule">Schedule</TabsTrigger>}
          </TabsList>

          {/* ── SOURCE: Basic ── */}
          {node.type === "source" && (
            <TabsContent value="basic" className="space-y-3 pt-3">
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Connection</label>
                <Select value={cfg.connection_id || ""} onValueChange={v => patch("connection_id", v)}>
                  <SelectTrigger className="mt-1 dark:bg-slate-700 dark:border-slate-600">
                    <SelectValue placeholder="Select source connection..." />
                  </SelectTrigger>
                  <SelectContent>
                    {connections.map(c => (
                      <SelectItem key={c.id} value={c.id}>{c.name} <span className="text-slate-400 ml-1">({c.platform})</span></SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Source Path / Format</label>
                <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="e.g. /data/input or table name"
                  value={cfg.source_path || ""} onChange={e => patch("source_path", e.target.value)} />
              </div>
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Delivery Channel</label>
                <Select value={cfg.delivery_channel || "pull"} onValueChange={v => patch("delivery_channel", v)}>
                  <SelectTrigger className="mt-1 dark:bg-slate-700 dark:border-slate-600"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="pull">Pull</SelectItem>
                    <SelectItem value="push">Push</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </TabsContent>
          )}

          {/* ── SOURCE: Datasets ── */}
          {node.type === "source" && (
            <TabsContent value="datasets" className="space-y-3 pt-3">
              <p className="text-xs text-slate-500 dark:text-slate-400">Add datasets/tables to extract from the source connection.</p>
              {(cfg.datasets || []).map((ds, i) => (
                <div key={i} className="flex gap-2 items-center">
                  <Input className="dark:bg-slate-700 dark:border-slate-600 text-xs" placeholder="schema.table"
                    value={ds} onChange={e => {
                      const updated = [...(cfg.datasets || [])];
                      updated[i] = e.target.value;
                      patch("datasets", updated);
                    }} />
                  <button className="text-red-400 hover:text-red-600" onClick={() => {
                    patch("datasets", (cfg.datasets || []).filter((_, j) => j !== i));
                  }}><Trash2 className="w-3.5 h-3.5" /></button>
                </div>
              ))}
              <Button size="sm" variant="outline" className="gap-1 dark:border-slate-600 dark:text-slate-300"
                onClick={() => patch("datasets", [...(cfg.datasets || []), ""])}>
                <Plus className="w-3.5 h-3.5" /> Add Dataset
              </Button>
            </TabsContent>
          )}

          {/* ── SOURCE: Schedule ── */}
          {node.type === "source" && (
            <TabsContent value="schedule" className="space-y-3 pt-3">
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Schedule Type</label>
                <Select value={cfg.schedule_type || "manual"} onValueChange={v => patch("schedule_type", v)}>
                  <SelectTrigger className="mt-1 dark:bg-slate-700 dark:border-slate-600"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="manual">Manual</SelectItem>
                    <SelectItem value="hourly">Hourly</SelectItem>
                    <SelectItem value="daily">Daily</SelectItem>
                    <SelectItem value="weekly">Weekly</SelectItem>
                    <SelectItem value="custom">Custom Cron</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {cfg.schedule_type === "custom" && (
                <div>
                  <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Cron Expression</label>
                  <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="0 2 * * *"
                    value={cfg.cron_expression || ""} onChange={e => patch("cron_expression", e.target.value)} />
                </div>
              )}
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Max Retries</label>
                <Input type="number" className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="3"
                  value={cfg.max_retries ?? ""} onChange={e => patch("max_retries", parseInt(e.target.value) || 0)} />
              </div>
            </TabsContent>
          )}

          {/* ── TRANSFORM: Basic ── */}
          {node.type === "transform" && (
            <TabsContent value="basic" className="space-y-3 pt-3">
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Transform Type</label>
                <Select value={cfg.transform_type || ""} onValueChange={v => patch("transform_type", v)}>
                  <SelectTrigger className="mt-1 dark:bg-slate-700 dark:border-slate-600">
                    <SelectValue placeholder="Select transform..." />
                  </SelectTrigger>
                  <SelectContent>
                    {TRANSFORM_OPTIONS.map(t => (
                      <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              {cfg.transform_type && (
                <div>
                  <label className="text-xs font-medium text-slate-600 dark:text-slate-300">
                    {cfg.transform_type === "filter" ? "WHERE Condition" :
                     cfg.transform_type === "rename" ? "Column Mapping (old=new, ...)" :
                     cfg.transform_type === "mask" ? "Columns to Mask (comma-separated)" :
                     "Configuration"}
                  </label>
                  <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600"
                    placeholder={
                      cfg.transform_type === "filter" ? "status = 'active' AND amount > 0" :
                      cfg.transform_type === "rename" ? "first_name=name, acct_no=account" :
                      cfg.transform_type === "mask" ? "ssn, credit_card, email" : "..."
                    }
                    value={cfg.config_value || ""} onChange={e => patch("config_value", e.target.value)} />
                </div>
              )}
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Label (optional)</label>
                <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="e.g. Filter Active Records"
                  value={cfg.label || ""} onChange={e => patch("label", e.target.value)} />
              </div>
            </TabsContent>
          )}

          {/* ── TARGET: Basic ── */}
          {node.type === "target" && (
            <TabsContent value="basic" className="space-y-3 pt-3">
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Connection</label>
                <Select value={cfg.connection_id || ""} onValueChange={v => patch("connection_id", v)}>
                  <SelectTrigger className="mt-1 dark:bg-slate-700 dark:border-slate-600">
                    <SelectValue placeholder="Select target connection..." />
                  </SelectTrigger>
                  <SelectContent>
                    {connections.map(c => (
                      <SelectItem key={c.id} value={c.id}>{c.name} <span className="text-slate-400 ml-1">({c.platform})</span></SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Load Method</label>
                <Select value={cfg.load_method || "append"} onValueChange={v => patch("load_method", v)}>
                  <SelectTrigger className="mt-1 dark:bg-slate-700 dark:border-slate-600"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="append">Append</SelectItem>
                    <SelectItem value="replace">Replace</SelectItem>
                    <SelectItem value="upsert">Upsert</SelectItem>
                    <SelectItem value="merge">Merge</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Target Path / Table</label>
                <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="e.g. dbo.target_table or /output/"
                  value={cfg.target_path || ""} onChange={e => patch("target_path", e.target.value)} />
              </div>
            </TabsContent>
          )}

          {/* ── TARGET: Advanced ── */}
          {node.type === "target" && (
            <TabsContent value="advanced" className="space-y-3 pt-3">
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Assignment Group</label>
                <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="e.g. DL-DataEngineering"
                  value={cfg.assignment_group || ""} onChange={e => patch("assignment_group", e.target.value)} />
              </div>
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Cost Center</label>
                <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="e.g. CC-12345"
                  value={cfg.cost_center || ""} onChange={e => patch("cost_center", e.target.value)} />
              </div>
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Alert Email</label>
                <Input type="email" className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="team@example.com"
                  value={cfg.email || ""} onChange={e => patch("email", e.target.value)} />
              </div>
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">SLA Max Duration (minutes)</label>
                <Input type="number" className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="60"
                  value={cfg.sla_max_minutes ?? ""} onChange={e => patch("sla_max_minutes", parseInt(e.target.value) || undefined)} />
              </div>
              <div>
                <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Data Classification</label>
                <Select value={cfg.data_classification || ""} onValueChange={v => patch("data_classification", v)}>
                  <SelectTrigger className="mt-1 dark:bg-slate-700 dark:border-slate-600"><SelectValue placeholder="Select..." /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="public">Public</SelectItem>
                    <SelectItem value="internal">Internal</SelectItem>
                    <SelectItem value="confidential">Confidential</SelectItem>
                    <SelectItem value="restricted">Restricted</SelectItem>
                    <SelectItem value="pii">PII</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </TabsContent>
          )}
        </Tabs>

        <DialogFooter className="mt-4">
          <Button variant="outline" onClick={onClose} className="dark:border-slate-600 dark:text-slate-300">Cancel</Button>
          <Button onClick={handleSave} className="bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c]">Apply</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ─── Pipeline Name & Meta Dialog ─────────────────────────────────────────────
function PipelineMetaDialog({ open, onClose, meta, onSave }) {
  const [val, setVal] = useState(meta);
  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-md dark:bg-slate-800 dark:border-slate-700">
        <DialogHeader>
          <DialogTitle className="dark:text-white">Pipeline Details</DialogTitle>
        </DialogHeader>
        <div className="space-y-3 py-2">
          <div>
            <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Pipeline Name *</label>
            <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="My Data Pipeline"
              value={val.name} onChange={e => setVal(v => ({ ...v, name: e.target.value }))} />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-600 dark:text-slate-300">Description</label>
            <Input className="mt-1 dark:bg-slate-700 dark:border-slate-600" placeholder="What does this pipeline do?"
              value={val.description} onChange={e => setVal(v => ({ ...v, description: e.target.value }))} />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose} className="dark:border-slate-600 dark:text-slate-300">Cancel</Button>
          <Button onClick={() => { onSave(val); onClose(); }} className="bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c]">Save</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ─── Pipeline Node Card ───────────────────────────────────────────────────────
function PipelineNode({ node, onRemove, onConfigure, connections, isDragging }) {
  const [expanded, setExpanded] = useState(true);
  const type = NODE_TYPES[node.type];
  const Icon = type.icon;

  const sourceConn = connections.find(c => c.id === node.config.connection_id);
  const summary = node.type === "source"
    ? sourceConn?.name || "No connection"
    : node.type === "target"
    ? sourceConn?.name || "No connection"
    : node.config.label || TRANSFORM_OPTIONS.find(t => t.value === node.config.transform_type)?.label || "Not configured";

  const isConfigured = node.type === "transform"
    ? !!node.config.transform_type
    : !!node.config.connection_id;

  return (
    <div className={`relative rounded-xl border-2 ${type.borderColor} ${type.bgLight} shadow-md transition-all ${isDragging ? "opacity-40 scale-95" : ""} w-[230px]`}>
      {/* Header */}
      <div className={`flex items-center gap-2 px-3 py-2 rounded-t-lg border-b ${type.borderColor} border-opacity-30`}>
        <span className="cursor-grab text-slate-400 hover:text-slate-600 flex-shrink-0">
          <GripVertical className="w-4 h-4" />
        </span>
        <div className={`w-6 h-6 rounded-md ${type.color} flex items-center justify-center flex-shrink-0`}>
          <Icon className="w-3.5 h-3.5 text-white" />
        </div>
        <span className={`text-xs font-bold ${type.textColor} flex-1 uppercase tracking-wide`}>{type.label}</span>
        {isConfigured
          ? <CheckCircle className="w-3.5 h-3.5 text-emerald-500 flex-shrink-0" />
          : <Clock className="w-3.5 h-3.5 text-amber-400 flex-shrink-0" />
        }
        <button onClick={() => setExpanded(!expanded)} className="text-slate-400 hover:text-slate-600">
          {expanded ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
        </button>
      </div>

      {/* Body */}
      {expanded && (
        <div className="px-3 pb-3 pt-2 space-y-2">
          <p className="text-xs text-slate-600 dark:text-slate-300 truncate font-medium">{summary}</p>

          {/* Quick inline source path / dataset */}
          {node.type === "source" && node.config.datasets?.length > 0 && (
            <div className="flex flex-wrap gap-1">
              {node.config.datasets.slice(0, 2).map((ds, i) => (
                <Badge key={i} variant="outline" className="text-xs px-1.5 py-0 font-mono">{ds}</Badge>
              ))}
              {node.config.datasets.length > 2 && (
                <Badge variant="outline" className="text-xs px-1.5 py-0">+{node.config.datasets.length - 2} more</Badge>
              )}
            </div>
          )}

          {/* Quick inline transform config */}
          {node.type === "transform" && node.config.config_value && (
            <p className="text-xs text-slate-400 dark:text-slate-500 truncate font-mono">{node.config.config_value}</p>
          )}

          {/* Quick inline load method */}
          {node.type === "target" && node.config.load_method && (
            <Badge variant="outline" className="text-xs px-1.5 py-0 capitalize">{node.config.load_method}</Badge>
          )}

          <div className="flex gap-1.5 pt-1">
            <button
              onClick={() => onConfigure(node)}
              className={`flex-1 flex items-center justify-center gap-1 text-xs px-2 py-1 rounded-md border transition-colors ${type.borderColor} ${type.textColor} hover:shadow-sm`}
            >
              <Settings className="w-3 h-3" />
              Configure
            </button>
            <button
              onClick={() => onRemove(node.id)}
              className="flex items-center justify-center px-2 py-1 rounded-md border border-slate-200 dark:border-slate-600 text-slate-400 hover:text-red-500 hover:border-red-300 transition-colors"
            >
              <Trash2 className="w-3 h-3" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function Connector() {
  return (
    <div className="flex items-center flex-shrink-0">
      <div className="w-6 h-0.5 bg-slate-300 dark:bg-slate-600" />
      <ArrowRight className="w-4 h-4 text-slate-400 dark:text-slate-500" />
    </div>
  );
}

// ─── Main Builder ─────────────────────────────────────────────────────────────
export default function VisualPipelineBuilder({ connections, onSaveSuccess }) {
  const [meta, setMeta] = useState({ name: "", description: "" });
  const [nodes, setNodes] = useState([makeNode("source"), makeNode("target")]);
  const [dragIndex, setDragIndex] = useState(null);
  const [overIndex, setOverIndex] = useState(null);
  const [saving, setSaving] = useState(false);
  const [configNode, setConfigNode] = useState(null);
  const [showMeta, setShowMeta] = useState(false);

  const addNode = (type, afterIndex) => {
    const newNode = makeNode(type);
    setNodes(prev => { const u = [...prev]; u.splice(afterIndex + 1, 0, newNode); return u; });
  };
  const removeNode = (id) => setNodes(prev => prev.filter(n => n.id !== id));
  const updateConfig = useCallback((id, newCfg) => {
    setNodes(prev => prev.map(n => n.id === id ? { ...n, config: newCfg } : n));
  }, []);

  const handleDragStart = (idx) => setDragIndex(idx);
  const handleDragOver = (e, idx) => { e.preventDefault(); setOverIndex(idx); };
  const handleDrop = (idx) => {
    if (dragIndex === null || dragIndex === idx) { setDragIndex(null); setOverIndex(null); return; }
    setNodes(prev => {
      const u = [...prev]; const [moved] = u.splice(dragIndex, 1); u.splice(idx, 0, moved); return u;
    });
    setDragIndex(null); setOverIndex(null);
  };

  const validate = () => {
    if (!meta.name.trim()) { toast.error("Pipeline name is required — click the name to edit"); return false; }
    const src = nodes.find(n => n.type === "source");
    const tgt = nodes.find(n => n.type === "target");
    if (!src) { toast.error("At least one Source node is required"); return false; }
    if (!tgt) { toast.error("At least one Target node is required"); return false; }
    if (!src.config.connection_id) { toast.error("Source connection is required — configure the Source node"); return false; }
    if (!tgt.config.connection_id) { toast.error("Target connection is required — configure the Target node"); return false; }
    return true;
  };

  const handleSave = async () => {
    if (!validate()) return;
    setSaving(true);
    const src = nodes.find(n => n.type === "source");
    const tgt = nodes.find(n => n.type === "target");
    const transforms = nodes.filter(n => n.type === "transform");

    const datasets = (src.config.datasets || []).map(ds => {
      const [schema, ...rest] = ds.split(".");
      return { schema, table: rest.join(".") || schema, load_method: tgt.config.load_method || "append" };
    });
    if (src.config.source_path && datasets.length === 0) {
      const [schema, ...rest] = src.config.source_path.split(".");
      datasets.push({ schema, table: rest.join(".") || schema, load_method: tgt.config.load_method || "append" });
    }

    const dqRules = transforms.reduce((acc, t) => {
      if (t.config.transform_type) acc[t.config.transform_type] = t.config.config_value || true;
      return acc;
    }, {});

    const pipelineData = {
      name: meta.name,
      description: meta.description || "",
      source_connection_id: src.config.connection_id,
      target_connection_id: tgt.config.connection_id,
      load_method: tgt.config.load_method || "append",
      delivery_channel: src.config.delivery_channel || "pull",
      schedule_type: src.config.schedule_type || "manual",
      cron_expression: src.config.cron_expression || "",
      selected_datasets: datasets,
      status: "idle",
      dq_rules: dqRules,
      assignment_group: tgt.config.assignment_group || "",
      cost_center: tgt.config.cost_center || "",
      email: tgt.config.email || "",
      retry_config: {
        max_retries: src.config.max_retries ?? 3,
        retry_delay_seconds: 60,
        exponential_backoff: true,
      },
      sla_config: tgt.config.sla_max_minutes
        ? { enabled: true, max_duration_minutes: tgt.config.sla_max_minutes, alert_threshold_percent: 80, escalation_enabled: false }
        : { enabled: false, max_duration_minutes: 60, alert_threshold_percent: 80 },
    };

    try {
      await dataflow.entities.Pipeline.create(pipelineData);
      toast.success(`Pipeline "${meta.name}" created!`);
      if (onSaveSuccess) onSaveSuccess();
    } catch (err) {
      toast.error("Failed to save pipeline");
    } finally {
      setSaving(false);
    }
  };

  const handleReset = () => {
    setMeta({ name: "", description: "" });
    setNodes([makeNode("source"), makeNode("target")]);
  };

  const srcCount = nodes.filter(n => n.type === "source").length;
  const tgtCount = nodes.filter(n => n.type === "target").length;
  const txCount = nodes.filter(n => n.type === "transform").length;
  const configured = nodes.filter(n =>
    n.type === "transform" ? !!n.config.transform_type : !!n.config.connection_id
  ).length;

  return (
    <div className="space-y-4">
      {/* Toolbar */}
      <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center justify-between p-4 rounded-xl bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-sm">
        <div className="flex items-center gap-3 flex-1">
          <Zap className="w-5 h-5 text-slate-700 dark:text-slate-300 flex-shrink-0" />
          <button
            onClick={() => setShowMeta(true)}
            className="flex items-center gap-2 group"
          >
            <span className={`text-sm font-semibold ${meta.name ? "text-slate-800 dark:text-white" : "text-slate-400 dark:text-slate-500 italic"}`}>
              {meta.name || "Untitled Pipeline — click to name"}
            </span>
            <Edit className="w-3.5 h-3.5 text-slate-300 group-hover:text-slate-500 transition-colors" />
          </button>
          {meta.description && (
            <span className="text-xs text-slate-400 dark:text-slate-500 hidden sm:block">{meta.description}</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={handleReset} className="gap-1.5 dark:border-slate-600 dark:text-slate-300">
            <RefreshCw className="w-3.5 h-3.5" /> Reset
          </Button>
          <Button size="sm" onClick={handleSave} disabled={saving} className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c]">
            {saving ? <RefreshCw className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
            {saving ? "Saving..." : "Save & Deploy"}
          </Button>
        </div>
      </div>

      {/* Palette */}
      <div className="flex items-center gap-3 flex-wrap px-1">
        <span className="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wide">Add Node:</span>
        {Object.entries(NODE_TYPES).map(([type, cfg]) => {
          const Icon = cfg.icon;
          return (
            <button key={type} onClick={() => addNode(type, nodes.length - 1)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border-2 ${cfg.borderColor} ${cfg.bgLight} ${cfg.textColor} text-xs font-medium hover:shadow-md transition-all`}>
              <Icon className="w-3.5 h-3.5" /> + {cfg.label}
            </button>
          );
        })}
        <span className="ml-auto text-xs text-slate-400 dark:text-slate-500 hidden md:block">
          Drag to reorder · Click "Configure" on each node to set it up
        </span>
      </div>

      {/* Canvas */}
      <div className="relative rounded-xl border-2 border-dashed border-slate-200 dark:border-slate-700 bg-gradient-to-br from-slate-50 to-slate-100 dark:from-slate-900/50 dark:to-slate-800/30 p-8 overflow-x-auto min-h-[280px]">
        {nodes.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-48 text-slate-400 dark:text-slate-600 gap-2">
            <Zap className="w-8 h-8 opacity-30" />
            <p className="text-sm">Add nodes from the palette above to build your pipeline</p>
          </div>
        ) : (
          <div className="flex items-start gap-0 w-max mx-auto">
            {nodes.map((node, idx) => (
              <div key={node.id} className="flex items-center">
                {/* Drop indicator */}
                {overIndex === idx && dragIndex !== null && dragIndex !== idx && (
                  <div className="w-1 h-40 bg-blue-400 rounded-full mx-2 opacity-70" />
                )}
                <div
                  draggable
                  onDragStart={() => handleDragStart(idx)}
                  onDragOver={(e) => handleDragOver(e, idx)}
                  onDrop={() => handleDrop(idx)}
                  onDragEnd={() => { setDragIndex(null); setOverIndex(null); }}
                >
                  <PipelineNode
                    node={node}
                    connections={connections}
                    onRemove={removeNode}
                    onConfigure={setConfigNode}
                    isDragging={dragIndex === idx}
                  />
                </div>
                {/* Connector + add transform between */}
                {idx < nodes.length - 1 && (
                  <div className="flex flex-col items-center gap-1 mx-2">
                    <Connector />
                    <button
                      onClick={() => addNode("transform", idx)}
                      title="Add transform here"
                      className="w-5 h-5 rounded-full bg-amber-100 dark:bg-amber-900/30 border border-amber-300 dark:border-amber-600 flex items-center justify-center hover:bg-amber-200 dark:hover:bg-amber-800/40 transition-colors"
                    >
                      <Plus className="w-3 h-3 text-amber-600 dark:text-amber-400" />
                    </button>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Status bar */}
      {nodes.length > 0 && (
        <div className="flex items-center gap-4 text-xs text-slate-500 dark:text-slate-400 px-1 flex-wrap">
          <span className="flex items-center gap-1"><Database className="w-3.5 h-3.5 text-blue-500" />{srcCount} Source</span>
          <span className="flex items-center gap-1"><Filter className="w-3.5 h-3.5 text-amber-500" />{txCount} Transform{txCount !== 1 ? "s" : ""}</span>
          <span className="flex items-center gap-1"><Target className="w-3.5 h-3.5 text-emerald-500" />{tgtCount} Target</span>
          <span className="flex items-center gap-1 ml-2">
            <Activity className="w-3.5 h-3.5" />
            {configured}/{nodes.length} nodes configured
          </span>
        </div>
      )}

      {/* Dialogs */}
      <NodeConfigDialog
        node={configNode}
        connections={connections}
        open={!!configNode}
        onClose={() => setConfigNode(null)}
        onSave={updateConfig}
      />
      <PipelineMetaDialog
        open={showMeta}
        onClose={() => setShowMeta(false)}
        meta={meta}
        onSave={setMeta}
      />
    </div>
  );
}