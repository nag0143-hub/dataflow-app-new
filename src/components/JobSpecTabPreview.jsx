import { useState, useEffect, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Copy, Download, Check, FileJson, AlertCircle, ShieldCheck, ShieldAlert, ChevronDown, ChevronUp, Loader2, RefreshCw, Eye, EyeOff } from "lucide-react";
import { buildJobSpec } from "@/components/JobSpecExport";
import { FLAT_FILE_PLATFORMS } from "@/components/JobSpecExport";
import { BUILTIN_TEMPLATES, getAllTemplates, getTemplatesForSource, getDefaultTemplateId, fillTemplate } from "@/components/DagTemplates";
import { dataflow } from "@/api/client";
import { toYaml } from "@/utils/toYaml";

export default function JobSpecTabPreview({ formData, connections, setFormData }) {
  const [format, setFormat] = useState("yaml");
  const [view, setView] = useState("airflow_dag");
  const [copied, setCopied] = useState(false);
  const [validation, setValidation] = useState(null);
  const [validating, setValidating] = useState(false);
  const [validationOpen, setValidationOpen] = useState(true);
  const [showRawTemplate, setShowRawTemplate] = useState(false);
  const [customTemplates, setCustomTemplates] = useState([]);

  useEffect(() => {
    dataflow.entities.DagTemplate.list().then(res => {
      if (Array.isArray(res)) setCustomTemplates(res);
    }).catch(() => {});
  }, []);

  const sourceConn = connections.find(c => c.id === formData.source_connection_id);
  const sourcePlatform = sourceConn?.platform || "";
  const allTemplates = getAllTemplates(customTemplates);
  const isFlatFile = FLAT_FILE_PLATFORMS.includes(sourcePlatform);
  const availableTemplates = getTemplatesForSource(sourcePlatform, customTemplates);

  const selectedTemplateId = formData.dag_template_id || getDefaultTemplateId(sourcePlatform);
  const setSelectedTemplateId = (id) => {
    if (setFormData) setFormData(prev => ({ ...prev, dag_template_id: id }));
  };

  useEffect(() => {
    const currentValid = allTemplates.find(t => t.id === selectedTemplateId);
    if (!currentValid && setFormData) {
      setFormData(prev => ({ ...prev, dag_template_id: getDefaultTemplateId(sourcePlatform) }));
    }
  }, [sourcePlatform, customTemplates.length]);

  const getMissingFields = () => {
    const missing = [];
    if (!formData.name?.trim()) missing.push("Pipeline name");
    if (!formData.source_connection_id) missing.push("Source connection");
    if (!formData.target_connection_id) missing.push("Target connection");
    if (formData.source_connection_id === formData.target_connection_id) missing.push("Source and target must be different");
    if (!formData.selected_datasets || formData.selected_datasets.length === 0) missing.push("At least one dataset");
    if (formData.schedule_type === "custom" && !formData.cron_expression?.trim()) missing.push("Cron expression");
    return missing;
  };

  const missingFields = getMissingFields();

  const draftJob = {
    id: "(unsaved)",
    ...formData,
    dq_rules: formData.dq_rules || {},
  };

  const pipelineNameClean = (formData.name || "pipeline").replace(/[^a-z0-9_-]/gi, "_").toLowerCase();

  const spec = buildJobSpec(draftJob, connections);
  const cleanSpec = JSON.parse(JSON.stringify(spec));

  const selectedTmpl = allTemplates.find(t => t.id === selectedTemplateId);
  const airflowDagYaml = fillTemplate(selectedTemplateId, draftJob, connections, customTemplates);

  const specContent = format === "json"
    ? JSON.stringify(cleanSpec, null, 2)
    : `# DataFlow Pipeline Spec — ${formData.name || "untitled"}\n` + toYaml(cleanSpec);

  const airflowFilename = `${pipelineNameClean}-airflow-dag.yaml`;
  const specFilename = `${pipelineNameClean}-pipelinespec.${format === "json" ? "json" : "yaml"}`;

  const content = view === "airflow_dag" ? airflowDagYaml : specContent;
  const filename = view === "airflow_dag" ? airflowFilename : specFilename;

  const handleCopy = async () => {
    await navigator.clipboard.writeText(content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    const blob = new Blob([content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleValidate = async () => {
    setValidating(true);
    try {
      const resp = await fetch("/api/validate-spec", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ spec: cleanSpec }),
      });
      const data = await resp.json();
      setValidation(data);
      setValidationOpen(true);
    } catch (err) {
      setValidation({
        valid: false,
        errors: [{ path: "", message: `Validation request failed: ${err.message}`, severity: "error" }],
        warnings: [],
      });
      setValidationOpen(true);
    } finally {
      setValidating(false);
    }
  };

  const specFingerprint = JSON.stringify(cleanSpec);
  const prevFingerprint = useRef(specFingerprint);

  useEffect(() => {
    if (prevFingerprint.current === specFingerprint && validation) return;
    prevFingerprint.current = specFingerprint;
    const timer = setTimeout(() => handleValidate(), 600);
    return () => clearTimeout(timer);
  }, [specFingerprint]);

  return (
    <div className="space-y-3">
      <div className="rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 p-3 flex items-center gap-3 flex-wrap">
        <span className="text-xs font-semibold text-slate-600 dark:text-slate-400 shrink-0">DAG Template:</span>
        <select
          value={selectedTemplateId}
          onChange={e => setSelectedTemplateId(e.target.value)}
          className="flex-1 min-w-[200px] max-w-md px-2.5 py-1.5 text-sm border border-slate-200 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-800 dark:text-slate-200 focus:ring-2 focus:ring-[#0060AF]/30 focus:border-[#0060AF] outline-none font-medium"
        >
          {availableTemplates.map(t => (
            <option key={t.id} value={t.id}>{t.name}{t.builtin ? "" : " (custom)"}</option>
          ))}
          {allTemplates.filter(t => !availableTemplates.find(a => a.id === t.id)).length > 0 && (
            <optgroup label="Other Templates">
              {allTemplates.filter(t => !availableTemplates.find(a => a.id === t.id)).map(t => (
                <option key={t.id} value={t.id}>{t.name}{t.builtin ? "" : " (custom)"}</option>
              ))}
            </optgroup>
          )}
        </select>
        {selectedTmpl?.builtin ? (
          <span className="text-[10px] font-semibold bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300 px-2 py-0.5 rounded-full shrink-0">Built-in</span>
        ) : selectedTmpl ? (
          <span className="text-[10px] font-semibold bg-blue-100 text-[#0060AF] dark:bg-blue-900/40 dark:text-blue-300 px-2 py-0.5 rounded-full shrink-0">Custom</span>
        ) : null}
        {selectedTmpl && (
          <button
            type="button"
            onClick={() => setShowRawTemplate(!showRawTemplate)}
            className="ml-auto flex items-center gap-1.5 text-xs text-[#0060AF] hover:text-[#004d8c] dark:text-blue-400 dark:hover:text-blue-300 font-medium shrink-0"
          >
            {showRawTemplate ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
            {showRawTemplate ? "Hide template" : "View raw template"}
          </button>
        )}
      </div>

      {showRawTemplate && selectedTmpl && (
        <div className="rounded-lg border border-slate-300 dark:border-slate-700 bg-slate-900 overflow-hidden">
          <div className="px-3 py-1.5 bg-slate-800 border-b border-slate-700">
            <span className="text-[10px] font-mono text-slate-400">Raw template with {"{{placeholders}}"}</span>
          </div>
          <pre className="p-3 text-xs text-amber-300 font-mono whitespace-pre overflow-auto max-h-48 leading-relaxed">
            {selectedTmpl.template}
          </pre>
        </div>
      )}

      {validation && (
        <div className={`rounded-lg border ${validation.valid ? "border-emerald-200 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950/30" : "border-red-200 bg-red-50 dark:border-red-800 dark:bg-red-950/30"}`}>
          <button
            type="button"
            onClick={() => setValidationOpen(!validationOpen)}
            className="w-full flex items-center gap-2 px-4 py-2.5 text-left"
          >
            {validation.valid ? (
              <ShieldCheck className="w-4 h-4 text-emerald-600 dark:text-emerald-400 shrink-0" />
            ) : (
              <ShieldAlert className="w-4 h-4 text-red-600 dark:text-red-400 shrink-0" />
            )}
            <span className={`text-sm font-semibold ${validation.valid ? "text-emerald-700 dark:text-emerald-300" : "text-red-700 dark:text-red-300"}`}>
              {validation.valid
                ? `Spec Valid${validation.warnings.length > 0 ? ` — ${validation.warnings.length} warning${validation.warnings.length !== 1 ? "s" : ""}` : ""}`
                : `${validation.errors.length} error${validation.errors.length !== 1 ? "s" : ""}${validation.warnings.length > 0 ? `, ${validation.warnings.length} warning${validation.warnings.length !== 1 ? "s" : ""}` : ""}`
              }
            </span>
            <span className="ml-auto">
              {validationOpen ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
            </span>
          </button>

          {validationOpen && (validation.errors.length > 0 || validation.warnings.length > 0) && (
            <div className="px-4 pb-3 space-y-1.5">
              {validation.errors.map((e, i) => (
                <div key={`e-${i}`} className="flex items-start gap-2 text-xs">
                  <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300 uppercase shrink-0 mt-0.5">Error</span>
                  <div>
                    {e.path && <code className="text-red-600 dark:text-red-400 font-mono text-[11px]">{e.path}</code>}
                    <span className="text-red-700 dark:text-red-300 ml-1">{e.message}</span>
                  </div>
                </div>
              ))}
              {validation.warnings.map((w, i) => (
                <div key={`w-${i}`} className="flex items-start gap-2 text-xs">
                  <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 uppercase shrink-0 mt-0.5">Warn</span>
                  <div>
                    {w.path && <code className="text-amber-600 dark:text-amber-400 font-mono text-[11px]">{w.path}</code>}
                    <span className="text-amber-700 dark:text-amber-300 ml-1">{w.message}</span>
                  </div>
                </div>
              ))}
              {validation.checked_at && (
                <p className="text-[10px] text-slate-400 pt-1">Checked at {new Date(validation.checked_at).toLocaleTimeString()}</p>
              )}
            </div>
          )}
        </div>
      )}

      <div className="flex items-center gap-2 flex-wrap">
        <FileJson className="w-4 h-4 text-blue-600 dark:text-blue-400" />
        <span className="text-sm font-semibold text-slate-900 dark:text-slate-100">Generated Output</span>
        <div className="flex gap-1.5 ml-auto flex-wrap">
          {[
            { key: "airflow_dag", label: "Airflow DAG (YAML)" },
            { key: "spec", label: "Pipeline Spec" },
          ].map(v => (
            <button key={v.key} type="button" onClick={() => setView(v.key)}
              className={`px-3 py-1 rounded-lg text-xs font-semibold border transition-all ${
                view === v.key
                  ? "bg-[#0060AF] text-white border-[#0060AF]"
                  : "bg-white dark:bg-slate-800 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-600 hover:border-slate-400 dark:hover:border-slate-500"
              }`}>
              {v.label}
            </button>
          ))}
          {view === "spec" && ["yaml", "json"].map(f => (
            <button key={f} type="button" onClick={() => setFormat(f)}
              className={`px-3 py-1 rounded-lg text-xs font-semibold border transition-all ${
                format === f
                  ? "bg-blue-600 text-white border-blue-600"
                  : "bg-white dark:bg-slate-800 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-600 hover:border-blue-300"
              }`}>
              {f.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      {missingFields.length > 0 && (
        <div className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950/30 p-3">
          <div className="flex items-start gap-2">
            <AlertCircle className="w-4 h-4 text-red-600 dark:text-red-400 mt-0.5 flex-shrink-0" />
            <div className="text-sm text-red-700 dark:text-red-300">
              <p className="font-semibold mb-1">Missing required fields:</p>
              <ul className="space-y-0.5 list-disc list-inside">
                {missingFields.map((field, i) => (
                  <li key={i}>{field}</li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      )}

      <p className="text-xs text-slate-500 dark:text-slate-400">
        {view === "airflow_dag"
          ? "YAML generated from the selected template, auto-filled with your pipeline configuration. This is the artifact that will be checked in."
          : "Pipeline specification with all configuration details. Used as input to populate the DAG template."}
      </p>
      <div className="rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-950 overflow-hidden">
        <div className="px-3 py-1.5 bg-slate-800 border-b border-slate-700 flex items-center justify-between">
          <span className="text-[10px] font-mono text-slate-400">
            {view === "airflow_dag" ? `${airflowFilename} — Template: ${selectedTmpl?.name || "none"}` : specFilename}
          </span>
          <span className="text-[10px] text-slate-500">{content.split("\n").length} lines</span>
        </div>
        <pre className="p-4 text-xs text-emerald-300 font-mono whitespace-pre overflow-auto max-h-[50vh] leading-relaxed">
          {content}
        </pre>
      </div>
      <div className="flex justify-end gap-2">
        <Button type="button" variant="outline" size="sm" onClick={handleValidate} disabled={validating} className="gap-1.5">
          {validating ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <RefreshCw className="w-3.5 h-3.5" />}
          {validating ? "Validating..." : "Validate Spec"}
        </Button>
        <Button type="button" variant="outline" size="sm" onClick={handleCopy} className="gap-1.5">
          {copied ? <Check className="w-3.5 h-3.5 text-emerald-600" /> : <Copy className="w-3.5 h-3.5" />}
          {copied ? "Copied!" : "Copy"}
        </Button>
        <Button type="button" variant="outline" size="sm" onClick={handleDownload} className="gap-1.5">
          <Download className="w-3.5 h-3.5" />
          Download {filename}
        </Button>
      </div>
    </div>
  );
}
