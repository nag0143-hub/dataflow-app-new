import { useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Copy, Download, FileJson, FileText, Check } from "lucide-react";
import { toYaml } from "@/utils/toYaml";
import dataflowConfig from '@/dataflow-config';

export const FLAT_FILE_PLATFORMS = Object.entries(dataflowConfig.platforms || {})
  .filter(([, v]) => v.category === 'file')
  .map(([k]) => k);

function getOperatorType(sourcePlatform, targetPlatform, override) {
  if (override === "custom_template") return "custom_template";
  if (override && override !== "auto") return override;
  const srcFlat = FLAT_FILE_PLATFORMS.includes(sourcePlatform);
  const tgtFlat = FLAT_FILE_PLATFORMS.includes(targetPlatform);
  return (srcFlat || tgtFlat) ? "PythonOperator" : "SparkSubmitOperator";
}

function connDetails(conn) {
  if (!conn) return {};
  // Omit secrets; include only structural/config fields safe for git
  return {
    name: conn.name,
    platform: conn.platform,
    host: conn.host || undefined,
    port: conn.port || undefined,
    database: conn.database || undefined,
    username: conn.username || undefined,
    auth_method: conn.auth_method || undefined,
    region: conn.region || undefined,
    bucket_container: conn.bucket_container || undefined,
    ...(conn.file_config && Object.keys(conn.file_config).length ? { file_config: conn.file_config } : {}),
    notes: conn.notes || undefined,
  };
}

export function buildJobSpec(job, connections) {
  const sourceConn = connections.find(c => c.id === job.source_connection_id);
  const targetConn = connections.find(c => c.id === job.target_connection_id);

  const srcPlatform = sourceConn?.platform || "";
  const tgtPlatform = targetConn?.platform || "";
  const operatorOverride = job.operator_type || "auto";
  const operatorType = getOperatorType(srcPlatform, tgtPlatform, operatorOverride);
  const isSparkJob = operatorType === "SparkSubmitOperator";
  const isCustom = operatorType === "custom_template";

  const sparkCfg = job.spark_config || {};
  const pythonCfg = job.python_config || {};

  const datasets = (job.selected_datasets || job.selected_objects || []);

  const scheduleSection = {
    type: job.schedule_type || "manual",
    cron_expression: job.cron_expression || "",
    start_date: job.start_date || "2024-01-01",
    end_date: job.end_date || null,
    catchup: !!job.catchup,
    is_paused_upon_creation: job.is_paused_upon_creation !== false,
    max_active_runs: job.max_active_runs ?? 1,
    concurrency: job.concurrency ?? 16,
    dagrun_timeout_minutes: job.dagrun_timeout ?? 0,
    use_custom_calendar: job.use_custom_calendar || false,
    include_calendar_id: job.include_calendar_id || "",
    exclude_calendar_id: job.exclude_calendar_id || "",
    custom_include_dates: job.custom_include_dates || "",
    custom_exclude_dates: job.custom_exclude_dates || "",
  };

  if (job.schedule_type === "event_driven") {
    scheduleSection.event_sensor = {
      sensor_type: job.event_sensor_type || "",
      config: {
        watch_path: job.event_config?.watch_path || "",
        sql_condition: job.event_config?.sql_condition || "",
        upstream_job: job.event_config?.upstream_job || "",
        poll_interval: job.event_config?.poll_interval || "60",
        timeout_hours: job.event_config?.timeout_hours || "24",
        sensor_mode: job.event_config?.sensor_mode || "reschedule",
        soft_fail: !!job.event_config?.soft_fail,
      },
    };
  }

  return {
    apiVersion: "dataflow/v1",
    kind: "IngestionPipeline",
    metadata: {
      name: job.name,
      description: job.description || "",
      id: job.id,
      generated_at: new Date().toISOString(),
    },
    spec: {
      source: {
        connection_id: job.source_connection_id,
        ...connDetails(sourceConn),
      },
      target: {
        connection_id: job.target_connection_id,
        ...connDetails(targetConn),
      },
      datasets: datasets.map(o => {
        const taskId = `${job.name}__${o.schema}__${o.table}`.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();

        const execution = { task_id: taskId, operator: operatorType };

        if (isCustom) {
          execution.template_override = true;
        } else if (isSparkJob) {
          execution.application = sparkCfg.application || "dataflow_spark_ingestion.py";
          execution.spark_conf = {
            executor_memory: sparkCfg.executor_memory || "4g",
            executor_cores: sparkCfg.executor_cores || "2",
            driver_memory: sparkCfg.driver_memory || "2g",
          };
          execution.py_files = (sparkCfg.py_files || "dataflow_utils.zip").split(",").map(f => f.trim()).filter(Boolean);
        } else {
          execution.python_callable = pythonCfg.callable || "run_ingestion";
          if (pythonCfg.module) execution.module = pythonCfg.module;
        }

        return {
          schema: o.schema,
          table: o.table,
          target_path: o.target_path || "",
          filter_query: o.filter_query || "",
          incremental_column: o.incremental_column || "",
          load_method: o.load_method || job.load_method || "append",
          execution,
        };
      }),
      schedule: scheduleSection,
      retry: {
        max_retries: job.retry_config?.max_retries ?? 3,
        retry_delay_seconds: job.retry_config?.retry_delay_seconds ?? 60,
        exponential_backoff: !!job.retry_config?.retry_exponential_backoff,
      },
      failure_handling: {
        email: job.email || "",
        email_on_retry: !!job.email_on_retry,
        sla_seconds: job.sla_seconds || null,
        execution_timeout: job.execution_timeout || null,
        depends_on_past: !!job.depends_on_past,
        wait_for_downstream: !!job.wait_for_downstream,
      },
      ownership: {
        owner: job.assignment_group || "data-eng",
        priority_weight: job.priority_weight ?? 1,
        pool: job.pool || "",
        tags: ["dataflow", ...(job.dag_tags || [])],
      },
      execution: {
        operator: operatorType,
        operator_override: operatorOverride,
        task_parallelism: "parallel",
        tags: ["dataflow", isCustom ? "custom" : isSparkJob ? "pyspark" : "python", job.schedule_type || "manual"],
        ...(isCustom && job.custom_template ? { custom_template: job.custom_template } : {}),
        ...(isSparkJob ? {
          spark_config: {
            executor_memory: sparkCfg.executor_memory || "4g",
            executor_cores: sparkCfg.executor_cores || "2",
            driver_memory: sparkCfg.driver_memory || "2g",
            application: sparkCfg.application || "dataflow_spark_ingestion.py",
            py_files: (sparkCfg.py_files || "dataflow_utils.zip").split(",").map(f => f.trim()).filter(Boolean),
          },
        } : {}),
        ...(!isSparkJob && !isCustom ? {
          python_config: {
            callable: pythonCfg.callable || "run_ingestion",
            module: pythonCfg.module || "",
          },
        } : {}),
      },
      advanced_features: job.advanced_features || { column_mapping: true },
      column_mappings: job.column_mappings || {},
      data_quality_rules: job.dq_rules || {},
      dag_callable_base_path: job.dag_callable_base_path || "/data/dags/",
    },
  };
}

export default function PipelineSpecExport({ job, connections, onClose }) {
  // legacy standalone dialog — still usable
  const [format, setFormat] = useState("json");
  const [copied, setCopied] = useState(false);

  const spec = buildJobSpec(job, connections);
  const content = format === "json"
    ? JSON.stringify(spec, null, 2)
    : `# DataFlow Pipeline Spec — ${job.name}\n` + toYaml(spec);

  const filename = `${job.name.replace(/[^a-z0-9_-]/gi, "_").toLowerCase()}-pipelinespec.${format === "json" ? "json" : "yaml"}`;

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

  return (
    <Dialog open onOpenChange={onClose}>
      <DialogContent className="max-w-2xl max-h-[90vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <FileJson className="w-5 h-5 text-blue-600" />
            Export Pipeline Spec — {job.name}
          </DialogTitle>
        </DialogHeader>

        <div className="flex items-center gap-2 mb-3">
          <span className="text-xs text-slate-500 font-medium">Format:</span>
          {["json", "yaml"].map(f => (
            <button
              key={f}
              type="button"
              onClick={() => setFormat(f)}
              className={`px-3 py-1 rounded-lg text-xs font-semibold border transition-all ${
                format === f
                  ? "bg-blue-600 text-white border-blue-600"
                  : "bg-white text-slate-600 border-slate-200 hover:border-blue-300"
              }`}
            >
              {f.toUpperCase()}
            </button>
          ))}
          <span className="ml-auto text-xs text-slate-400 font-mono">{filename}</span>
        </div>

        <div className="flex-1 overflow-auto rounded-lg border border-slate-200 bg-slate-950 min-h-0">
          <pre className="p-4 text-xs text-emerald-300 font-mono whitespace-pre leading-relaxed overflow-auto h-full max-h-[55vh]">
            {content}
          </pre>
        </div>

        <div className="flex justify-end gap-2 pt-3 border-t mt-3">
          <Button type="button" variant="outline" onClick={onClose}>Close</Button>
          <Button type="button" variant="outline" onClick={handleCopy} className="gap-1.5">
            {copied ? <Check className="w-4 h-4 text-emerald-600" /> : <Copy className="w-4 h-4" />}
            {copied ? "Copied!" : "Copy"}
          </Button>
          <Button type="button" onClick={handleDownload} className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c]">
            <Download className="w-4 h-4" />
            Download
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}