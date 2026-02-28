import { useState, useEffect, useMemo } from "react";
import { dataflow } from '@/api/client';
import dataflowConfig from '@/dataflow-config';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Plus, Trash2, Pencil, Save, X, LayoutTemplate, Zap, ShieldCheck, Search, FlaskConical, Code2, ChevronDown, ChevronUp, Info } from "lucide-react";
import { cn } from "@/lib/utils";
import { BUILTIN_TEMPLATES, AVAILABLE_PLACEHOLDERS } from "@/components/DagTemplates";

const EMPTY_FN = { name: "", label: "", category: "spark_udf", description: "", expression_template: "", param_hint: "" };
const EMPTY_TMPL = { name: "", description: "", sourceType: "any", template: "" };
const DEFAULT_TEMPLATE_BODY = `{{dag_id}}:
  default_args:
    owner: {{owner}}
    email:
      - {{email}}
    email_on_failure: {{email_on_failure}}
    retries: {{retries}}
    retry_delay_sec: {{retry_delay_sec}}
    start_date: {{start_date}}
  schedule: {{schedule}}
  catchup: false
  description: "{{description}}"

  tasks:
    # Add your tasks here using {{placeholders}}
{{dataset_extract_group}}`;

const TABS = [
  { id: "templates", label: "DAG Templates", icon: LayoutTemplate },
  { id: "transforms", label: "Transform Functions", icon: Zap },
  { id: "dq_rules", label: "DQ Rules", icon: ShieldCheck },
];

export default function CustomFunctions() {
  const [activeTab, setActiveTab] = useState("templates");
  const [functions, setFunctions] = useState([]);
  const [customTemplates, setCustomTemplates] = useState([]);
  const [loading, setLoading] = useState(true);
  const [editingFn, setEditingFn] = useState(null);
  const [fnForm, setFnForm] = useState(EMPTY_FN);
  const [editingTmpl, setEditingTmpl] = useState(null);
  const [tmplForm, setTmplForm] = useState(EMPTY_TMPL);
  const [saving, setSaving] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [showPlaceholders, setShowPlaceholders] = useState(false);

  const wizardConfig = dataflowConfig.pipelineWizard || {};
  const configTransforms = useMemo(() => wizardConfig.transformations || [], []);
  const configDqRules = useMemo(() => ({
    column: wizardConfig.dq_rules?.column_rules || [],
    dataset: wizardConfig.dq_rules?.dataset_rules || [],
    extended: wizardConfig.dq_rules?.column_rules_extended || [],
    actions: wizardConfig.dq_rules?.actions || [],
  }), []);

  const load = async () => {
    setLoading(true);
    try {
      const [fns, tmpls] = await Promise.all([
        dataflow.entities.CustomFunction.list(),
        dataflow.entities.DagTemplate.list(),
      ]);
      setFunctions(Array.isArray(fns) ? fns : []);
      setCustomTemplates(Array.isArray(tmpls) ? tmpls : []);
    } catch (e) {
      console.error("Load error:", e);
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const startNewFn = () => { setEditingFn("new"); setFnForm(EMPTY_FN); };
  const startEditFn = (fn) => { setEditingFn(fn); setFnForm({ ...fn }); };
  const cancelFn = () => { setEditingFn(null); setFnForm(EMPTY_FN); };

  const saveFn = async () => {
    setSaving(true);
    try {
      if (editingFn === "new") {
        await dataflow.entities.CustomFunction.create(fnForm);
      } else {
        await dataflow.entities.CustomFunction.update(editingFn.id, fnForm);
      }
      await load();
      cancelFn();
    } finally { setSaving(false); }
  };

  const removeFn = async (fn) => {
    if (!window.confirm(`Delete "${fn.label}"?`)) return;
    await dataflow.entities.CustomFunction.delete(fn.id);
    setFunctions(prev => prev.filter(f => f.id !== fn.id));
  };

  const startNewTmpl = () => { setEditingTmpl("new"); setTmplForm({ ...EMPTY_TMPL, template: DEFAULT_TEMPLATE_BODY }); };
  const startEditTmpl = (tmpl) => {
    if (tmpl.builtin) {
      setEditingTmpl("new");
      setTmplForm({ name: `${tmpl.name} (Modified)`, description: tmpl.description || "", sourceType: tmpl.sourceType || "any", template: tmpl.template || "" });
    } else {
      setEditingTmpl(tmpl);
      setTmplForm({ name: tmpl.name, description: tmpl.description || "", sourceType: tmpl.sourceType || "any", template: tmpl.template || "" });
    }
  };
  const cancelTmpl = () => { setEditingTmpl(null); setTmplForm(EMPTY_TMPL); setShowPlaceholders(false); };

  const saveTmpl = async () => {
    setSaving(true);
    try {
      const templateId = editingTmpl !== "new" && editingTmpl?.templateId ? editingTmpl.templateId : `custom_${Date.now()}`;
      const payload = { templateId, name: tmplForm.name.trim(), description: tmplForm.description.trim(), sourceType: tmplForm.sourceType, template: tmplForm.template, builtin: false };
      if (editingTmpl !== "new" && editingTmpl?.id) {
        await dataflow.entities.DagTemplate.update(editingTmpl.id, payload);
      } else {
        await dataflow.entities.DagTemplate.create(payload);
      }
      await load();
      cancelTmpl();
    } finally { setSaving(false); }
  };

  const removeTmpl = async (tmpl) => {
    if (!window.confirm(`Delete template "${tmpl.name}"?`)) return;
    await dataflow.entities.DagTemplate.delete(tmpl.id);
    setCustomTemplates(prev => prev.filter(t => t.id !== tmpl.id));
  };

  const insertPlaceholder = (key) => {
    const textarea = document.getElementById("template-editor");
    if (textarea) {
      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;
      const text = tmplForm.template;
      const newText = text.substring(0, start) + `{{${key}}}` + text.substring(end);
      setTmplForm(f => ({ ...f, template: newText }));
      setTimeout(() => { textarea.focus(); textarea.selectionStart = textarea.selectionEnd = start + key.length + 4; }, 0);
    } else {
      setTmplForm(f => ({ ...f, template: f.template + `{{${key}}}` }));
    }
  };

  const filteredFunctions = useMemo(() => {
    if (!searchTerm.trim()) return functions;
    const t = searchTerm.toLowerCase();
    return functions.filter(f => (f.name || "").toLowerCase().includes(t) || (f.label || "").toLowerCase().includes(t) || (f.category || "").toLowerCase().includes(t));
  }, [functions, searchTerm]);

  const filteredTransforms = useMemo(() => {
    if (!searchTerm.trim()) return configTransforms;
    const t = searchTerm.toLowerCase();
    return configTransforms.filter(tr => tr.value.toLowerCase().includes(t) || tr.label.toLowerCase().includes(t) || (tr.category || "").toLowerCase().includes(t));
  }, [configTransforms, searchTerm]);

  const categoryBadge = (cat) => {
    const colors = {
      spark_udf: "bg-blue-100 text-[#0060AF] dark:bg-blue-900/40 dark:text-blue-300",
      custom_expression: "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300",
      spark_native: "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300",
      core: "bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300",
    };
    const labels = { spark_udf: "Spark UDF", custom_expression: "Custom Expr", spark_native: "Spark Native", core: "Core" };
    return <Badge className={cn("text-[10px]", colors[cat] || colors.core)}>{labels[cat] || cat}</Badge>;
  };

  const sourceTypeBadge = (st) => {
    const map = { flat_file: ["Flat File", "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300"], database: ["Database", "bg-blue-100 text-[#0060AF] dark:bg-blue-900/40 dark:text-blue-300"], any: ["Any Source", "bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300"] };
    const [label, cls] = map[st] || map.any;
    return <Badge className={cn("text-[10px]", cls)}>{label}</Badge>;
  };

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-slate-100">Function Library</h1>
          <p className="text-sm text-slate-500 dark:text-slate-400 mt-0.5">
            Manage DAG templates, transform functions, and data quality rules.
          </p>
        </div>
      </div>

      <div className="flex items-center gap-1 border-b border-slate-200 dark:border-slate-700">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => { setActiveTab(tab.id); setSearchTerm(""); }}
            className={cn(
              "flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors",
              activeTab === tab.id
                ? "border-[#0060AF] text-[#0060AF] dark:text-blue-400 dark:border-blue-400"
                : "border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300"
            )}
          >
            <tab.icon className="w-4 h-4" />
            {tab.label}
            {tab.id === "transforms" && (
              <Badge variant="secondary" className="text-[10px] h-5 px-1.5">{configTransforms.length + functions.length}</Badge>
            )}
            {tab.id === "templates" && (
              <Badge variant="secondary" className="text-[10px] h-5 px-1.5">{BUILTIN_TEMPLATES.length + customTemplates.length}</Badge>
            )}
          </button>
        ))}
      </div>

      {activeTab === "templates" && (
        <TemplatesTab
          builtinTemplates={BUILTIN_TEMPLATES}
          customTemplates={customTemplates}
          editing={editingTmpl}
          form={tmplForm}
          setForm={setTmplForm}
          onNew={startNewTmpl}
          onEdit={startEditTmpl}
          onCancel={cancelTmpl}
          onSave={saveTmpl}
          onDelete={removeTmpl}
          saving={saving}
          loading={loading}
          sourceTypeBadge={sourceTypeBadge}
          showPlaceholders={showPlaceholders}
          setShowPlaceholders={setShowPlaceholders}
          insertPlaceholder={insertPlaceholder}
        />
      )}

      {activeTab === "transforms" && (
        <TransformsTab
          configTransforms={filteredTransforms}
          customFunctions={filteredFunctions}
          searchTerm={searchTerm}
          setSearchTerm={setSearchTerm}
          editing={editingFn}
          form={fnForm}
          setForm={setFnForm}
          onNew={startNewFn}
          onEdit={startEditFn}
          onCancel={cancelFn}
          onSave={saveFn}
          onDelete={removeFn}
          saving={saving}
          loading={loading}
          categoryBadge={categoryBadge}
        />
      )}

      {activeTab === "dq_rules" && (
        <DQRulesTab configDqRules={configDqRules} searchTerm={searchTerm} setSearchTerm={setSearchTerm} />
      )}
    </div>
  );
}

function TemplatesTab({ builtinTemplates, customTemplates, editing, form, setForm, onNew, onEdit, onCancel, onSave, onDelete, saving, loading, sourceTypeBadge, showPlaceholders, setShowPlaceholders, insertPlaceholder }) {
  const [expandedId, setExpandedId] = useState(null);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-slate-500 dark:text-slate-400">
          DAG templates define the Airflow DAG structure. Built-in templates can be copied and customized; custom templates can be created, edited, and deleted.
        </p>
        <Button onClick={onNew} className="gap-2 bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c] shrink-0">
          <Plus className="w-4 h-4" /> New Template
        </Button>
      </div>

      {editing && (
        <Card className="p-5 border-blue-200 dark:border-blue-800 bg-blue-50/40 dark:bg-blue-950/30 space-y-4">
          <h3 className="text-sm font-semibold text-slate-800 dark:text-slate-200">
            {editing === "new" ? "Create New Template" : `Edit: ${editing.name}`}
          </h3>
          <div className="grid grid-cols-3 gap-4">
            <div>
              <Label className="text-xs">Template Name</Label>
              <Input value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} placeholder="My ETL Template" className="h-8 text-xs mt-1" />
            </div>
            <div>
              <Label className="text-xs">Source Type</Label>
              <Select value={form.sourceType} onValueChange={v => setForm(f => ({ ...f, sourceType: v }))}>
                <SelectTrigger className="h-8 text-xs mt-1"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="any">Any (all sources)</SelectItem>
                  <SelectItem value="flat_file">Flat File only</SelectItem>
                  <SelectItem value="database">Database only</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label className="text-xs">Description</Label>
              <Input value={form.description} onChange={e => setForm(f => ({ ...f, description: e.target.value }))} placeholder="Brief description" className="h-8 text-xs mt-1" />
            </div>
          </div>
          <div>
            <div className="flex items-center justify-between mb-1">
              <Label className="text-xs">YAML Template</Label>
              <button type="button" onClick={() => setShowPlaceholders(!showPlaceholders)} className="flex items-center gap-1 text-xs text-[#0060AF] hover:text-[#004d8c] font-medium">
                <Info className="w-3.5 h-3.5" />{showPlaceholders ? "Hide placeholders" : "Available placeholders"}
              </button>
            </div>
            {showPlaceholders && (
              <div className="rounded-lg border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-950/40 p-3 space-y-2 max-h-48 overflow-y-auto mb-2">
                <p className="text-xs text-blue-700 dark:text-blue-300 font-semibold">Click to insert at cursor:</p>
                <div className="grid grid-cols-1 gap-0.5">
                  {AVAILABLE_PLACEHOLDERS.map(p => (
                    <button key={p.key} type="button" onClick={() => insertPlaceholder(p.key)} className="flex items-center gap-2 text-left px-2 py-1 rounded hover:bg-blue-100 dark:hover:bg-blue-900/40 transition-colors">
                      <code className="text-xs font-mono text-blue-800 dark:text-blue-300 bg-blue-100 dark:bg-blue-900/40 px-1.5 py-0.5 rounded shrink-0">{`{{${p.key}}}`}</code>
                      <span className="text-xs text-blue-600 dark:text-blue-400 truncate">{p.desc}</span>
                    </button>
                  ))}
                </div>
              </div>
            )}
            <textarea
              id="template-editor"
              value={form.template}
              onChange={e => setForm(f => ({ ...f, template: e.target.value }))}
              rows={16}
              spellCheck={false}
              className="w-full px-3 py-2 text-xs font-mono border border-slate-200 dark:border-slate-700 rounded-lg bg-slate-950 text-emerald-300 focus:ring-2 focus:ring-[#0060AF]/30 focus:border-[#0060AF] outline-none leading-relaxed resize-y"
            />
          </div>
          <div className="flex gap-2">
            <Button size="sm" onClick={onSave} disabled={!form.name.trim() || saving} className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c]">
              <Save className="w-3.5 h-3.5" /> {saving ? "Saving..." : "Save Template"}
            </Button>
            <Button size="sm" variant="ghost" onClick={onCancel}><X className="w-3.5 h-3.5 mr-1" /> Cancel</Button>
          </div>
        </Card>
      )}

      <div className="space-y-2">
        <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Built-in Templates</h3>
        {builtinTemplates.map(tmpl => (
          <Card key={tmpl.id} className="dark:bg-slate-900 dark:border-slate-700">
            <div role="button" tabIndex={0} onClick={() => setExpandedId(expandedId === tmpl.id ? null : tmpl.id)} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setExpandedId(expandedId === tmpl.id ? null : tmpl.id); }}} className="w-full flex items-center gap-3 px-4 py-3 text-left cursor-pointer">
              <LayoutTemplate className="w-4 h-4 text-slate-400 shrink-0" />
              <div className="flex-1 min-w-0">
                <span className="text-sm font-medium text-slate-800 dark:text-slate-200">{tmpl.name}</span>
                <span className="text-xs text-slate-500 dark:text-slate-400 ml-2">{tmpl.description}</span>
              </div>
              {sourceTypeBadge(tmpl.sourceType)}
              <Badge className="bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400 text-[10px]">Built-in</Badge>
              <button onClick={e => { e.stopPropagation(); onEdit(tmpl); }} className="text-slate-400 hover:text-[#0060AF] transition-colors p-1" title="Copy & customize this template">
                <Pencil className="w-3.5 h-3.5" />
              </button>
              {expandedId === tmpl.id ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
            </div>
            {expandedId === tmpl.id && (
              <div className="px-4 pb-4">
                <pre className="p-3 text-xs text-emerald-300 font-mono whitespace-pre overflow-auto max-h-60 bg-slate-950 rounded-lg leading-relaxed">{tmpl.template}</pre>
                <div className="mt-2 flex justify-end">
                  <Button size="sm" variant="outline" onClick={() => onEdit(tmpl)} className="gap-1.5 text-xs">
                    <Pencil className="w-3 h-3" /> Copy & Customize
                  </Button>
                </div>
              </div>
            )}
          </Card>
        ))}
      </div>

      {customTemplates.length > 0 && (
        <div className="space-y-2">
          <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Custom Templates</h3>
          {customTemplates.map(tmpl => (
            <Card key={tmpl.id} className="dark:bg-slate-900 dark:border-slate-700">
              <button type="button" onClick={() => setExpandedId(expandedId === `custom-${tmpl.id}` ? null : `custom-${tmpl.id}`)} className="w-full flex items-center gap-3 px-4 py-3 text-left">
                <LayoutTemplate className="w-4 h-4 text-[#0060AF] shrink-0" />
                <div className="flex-1 min-w-0">
                  <span className="text-sm font-medium text-slate-800 dark:text-slate-200">{tmpl.name}</span>
                  {tmpl.description && <span className="text-xs text-slate-500 dark:text-slate-400 ml-2">{tmpl.description}</span>}
                </div>
                {sourceTypeBadge(tmpl.sourceType || "any")}
                <Badge className="bg-blue-100 text-[#0060AF] dark:bg-blue-900/40 dark:text-blue-300 text-[10px]">Custom</Badge>
                <div className="flex items-center gap-1">
                  <button onClick={e => { e.stopPropagation(); onEdit(tmpl); }} className="text-slate-400 hover:text-blue-600 transition-colors p-1"><Pencil className="w-3.5 h-3.5" /></button>
                  <button onClick={e => { e.stopPropagation(); onDelete(tmpl); }} className="text-slate-400 hover:text-red-500 transition-colors p-1"><Trash2 className="w-3.5 h-3.5" /></button>
                </div>
                {expandedId === `custom-${tmpl.id}` ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
              </button>
              {expandedId === `custom-${tmpl.id}` && (
                <div className="px-4 pb-4">
                  <pre className="p-3 text-xs text-emerald-300 font-mono whitespace-pre overflow-auto max-h-60 bg-slate-950 rounded-lg leading-relaxed">{tmpl.template}</pre>
                </div>
              )}
            </Card>
          ))}
        </div>
      )}

      {!loading && customTemplates.length === 0 && (
        <p className="text-xs text-slate-400 dark:text-slate-500 text-center py-4">
          No custom templates yet. Click <strong>New Template</strong> to create one.
        </p>
      )}
    </div>
  );
}

function TransformsTab({ configTransforms, customFunctions, searchTerm, setSearchTerm, editing, form, setForm, onNew, onEdit, onCancel, onSave, onDelete, saving, loading, categoryBadge }) {
  const grouped = useMemo(() => {
    const groups = {};
    configTransforms.forEach(t => {
      const cat = t.category || "core";
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push(t);
    });
    return groups;
  }, [configTransforms]);

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <div className="relative flex-1">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
          <Input
            value={searchTerm}
            onChange={e => setSearchTerm(e.target.value)}
            placeholder="Search transforms..."
            className="pl-9 h-9 text-sm"
          />
        </div>
        <Button onClick={onNew} className="gap-2 bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c] shrink-0">
          <Plus className="w-4 h-4" /> New Function
        </Button>
      </div>

      {editing && (
        <Card className="p-5 border-blue-200 dark:border-blue-800 bg-blue-50/40 dark:bg-blue-950/30 space-y-4">
          <h3 className="text-sm font-semibold text-slate-800 dark:text-slate-200">
            {editing === "new" ? "Add New Function" : `Edit: ${editing.label}`}
          </h3>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label className="text-xs">Function Key (unique, no spaces)</Label>
              <Input value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value.replace(/\s+/g, "_") }))} placeholder="my_udf" className="h-8 text-xs mt-1 font-mono" />
            </div>
            <div>
              <Label className="text-xs">Display Label</Label>
              <Input value={form.label} onChange={e => setForm(f => ({ ...f, label: e.target.value }))} placeholder="my_udf(col, 'param')" className="h-8 text-xs mt-1" />
            </div>
            <div>
              <Label className="text-xs">Category</Label>
              <Select value={form.category} onValueChange={v => setForm(f => ({ ...f, category: v }))}>
                <SelectTrigger className="h-8 text-xs mt-1"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="spark_udf">Spark UDF</SelectItem>
                  <SelectItem value="custom_expression">Custom Expression</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label className="text-xs">Description (optional)</Label>
              <Input value={form.description} onChange={e => setForm(f => ({ ...f, description: e.target.value }))} placeholder="What this function does" className="h-8 text-xs mt-1" />
            </div>
            <div className="col-span-2">
              <Label className="text-xs">Expression Template (use {"{col}"} as placeholder)</Label>
              <Input value={form.expression_template} onChange={e => setForm(f => ({ ...f, expression_template: e.target.value }))} placeholder="my_udf({col}, 'default')" className="h-8 text-xs mt-1 font-mono" />
            </div>
          </div>
          <div className="flex gap-2 pt-1">
            <Button size="sm" onClick={onSave} disabled={!form.name || !form.label || saving} className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c]">
              <Save className="w-3.5 h-3.5" /> {saving ? "Saving..." : "Save Function"}
            </Button>
            <Button size="sm" variant="ghost" onClick={onCancel}><X className="w-3.5 h-3.5 mr-1" /> Cancel</Button>
          </div>
        </Card>
      )}

      {customFunctions.length > 0 && (
        <div className="space-y-2">
          <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Custom Functions ({customFunctions.length})</h3>
          <Card className="overflow-hidden dark:bg-slate-900 dark:border-slate-700">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-slate-50 dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700">
                  <th className="px-4 py-2.5 text-left font-semibold text-slate-600 dark:text-slate-400">Key</th>
                  <th className="px-4 py-2.5 text-left font-semibold text-slate-600 dark:text-slate-400">Label</th>
                  <th className="px-4 py-2.5 text-left font-semibold text-slate-600 dark:text-slate-400">Category</th>
                  <th className="px-4 py-2.5 text-left font-semibold text-slate-600 dark:text-slate-400">Expression</th>
                  <th className="py-2.5 w-20"></th>
                </tr>
              </thead>
              <tbody>
                {customFunctions.map(fn => (
                  <tr key={fn.id} className="border-b border-slate-100 dark:border-slate-800 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors group">
                    <td className="px-4 py-2.5 font-mono text-slate-800 dark:text-slate-200">{fn.name}</td>
                    <td className="px-4 py-2.5 text-slate-700 dark:text-slate-300">{fn.label}</td>
                    <td className="px-4 py-2.5">{categoryBadge(fn.category)}</td>
                    <td className="px-4 py-2.5 font-mono text-slate-500 dark:text-slate-400">{fn.expression_template || "—"}</td>
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                        <button onClick={() => onEdit(fn)} className="text-slate-400 hover:text-blue-600 transition-colors"><Pencil className="w-3.5 h-3.5" /></button>
                        <button onClick={() => onDelete(fn)} className="text-slate-400 hover:text-red-500 transition-colors"><Trash2 className="w-3.5 h-3.5" /></button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        </div>
      )}

      {Object.entries(grouped).map(([cat, transforms]) => (
        <div key={cat} className="space-y-2">
          <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider flex items-center gap-2">
            {cat === "spark_native" ? "Spark Native" : cat === "spark_udf" ? "Built-in Spark UDFs" : cat === "core" ? "Core" : cat} ({transforms.length})
          </h3>
          <Card className="overflow-hidden dark:bg-slate-900 dark:border-slate-700">
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-px bg-slate-200 dark:bg-slate-700">
              {transforms.map(t => (
                <div key={t.value} className="bg-white dark:bg-slate-900 px-3 py-2 flex items-center gap-2">
                  <code className="text-xs font-mono text-slate-700 dark:text-slate-300 truncate">{t.label}</code>
                </div>
              ))}
            </div>
          </Card>
        </div>
      ))}

      {!loading && customFunctions.length === 0 && (
        <p className="text-xs text-slate-400 dark:text-slate-500">
          <Zap className="w-3 h-3 inline mr-1" />
          Custom functions added here automatically appear in the <strong>Transform</strong> dropdown in Column Mapping.
        </p>
      )}
    </div>
  );
}

function DQRulesTab({ configDqRules, searchTerm, setSearchTerm }) {
  const filterRules = (rules) => {
    if (!searchTerm.trim()) return rules;
    const t = searchTerm.toLowerCase();
    return rules.filter(r => r.value.toLowerCase().includes(t) || r.label.toLowerCase().includes(t));
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <div className="relative flex-1">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
          <Input
            value={searchTerm}
            onChange={e => setSearchTerm(e.target.value)}
            placeholder="Search DQ rules..."
            className="pl-9 h-9 text-sm"
          />
        </div>
      </div>

      <p className="text-sm text-slate-500 dark:text-slate-400">
        Data quality rules configured in <code className="text-xs bg-slate-100 dark:bg-slate-800 px-1.5 py-0.5 rounded">pipeline-wizard.yaml</code>. These are available in the Data Quality section of the pipeline Advanced tab.
      </p>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="space-y-2">
          <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider flex items-center gap-2">
            <ShieldCheck className="w-3.5 h-3.5" /> Column-Level Rules ({configDqRules.column.length})
          </h3>
          <Card className="overflow-hidden dark:bg-slate-900 dark:border-slate-700">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-slate-50 dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700">
                  <th className="px-4 py-2 text-left font-semibold text-slate-600 dark:text-slate-400">Rule</th>
                  <th className="px-4 py-2 text-left font-semibold text-slate-600 dark:text-slate-400">Params</th>
                </tr>
              </thead>
              <tbody>
                {filterRules(configDqRules.column).map(r => (
                  <tr key={r.value} className="border-b border-slate-100 dark:border-slate-800">
                    <td className="px-4 py-2 text-slate-800 dark:text-slate-200">{r.label}</td>
                    <td className="px-4 py-2 text-slate-500 dark:text-slate-400">{r.hasParam ? r.paramLabel || "Yes" : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        </div>

        <div className="space-y-2">
          <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider flex items-center gap-2">
            <ShieldCheck className="w-3.5 h-3.5" /> Dataset-Level Rules ({configDqRules.dataset.length})
          </h3>
          <Card className="overflow-hidden dark:bg-slate-900 dark:border-slate-700">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-slate-50 dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700">
                  <th className="px-4 py-2 text-left font-semibold text-slate-600 dark:text-slate-400">Rule</th>
                </tr>
              </thead>
              <tbody>
                {filterRules(configDqRules.dataset).map(r => (
                  <tr key={r.value} className="border-b border-slate-100 dark:border-slate-800">
                    <td className="px-4 py-2 text-slate-800 dark:text-slate-200">{r.label}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        </div>
      </div>

      <div className="space-y-2">
        <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Extended Column Rules ({configDqRules.extended.length})</h3>
        <Card className="overflow-hidden dark:bg-slate-900 dark:border-slate-700">
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-px bg-slate-200 dark:bg-slate-700">
            {filterRules(configDqRules.extended).map(r => (
              <div key={r.value} className="bg-white dark:bg-slate-900 px-3 py-2 text-xs text-slate-700 dark:text-slate-300">{r.label}</div>
            ))}
          </div>
        </Card>
      </div>

      <div className="space-y-2">
        <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Failure Actions</h3>
        <div className="flex flex-wrap gap-2">
          {configDqRules.actions.map(a => (
            <Badge key={a.value} variant="outline" className="text-xs py-1 px-3 dark:border-slate-600 dark:text-slate-300">{a.label}</Badge>
          ))}
        </div>
      </div>
    </div>
  );
}
