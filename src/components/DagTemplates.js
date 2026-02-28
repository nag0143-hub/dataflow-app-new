import { FLAT_FILE_PLATFORMS } from "@/components/JobSpecExport";

export const BUILTIN_TEMPLATES = [
  {
    id: "flat_file_landing_to_raw",
    name: "Flat File — Landing to Raw",
    description: "Sensor waits for file arrival, task group ingests N datasets in parallel, optional transform group if column mappings exist",
    sourceType: "flat_file",
    builtin: true,
    template: `{{dag_id}}:
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

    wait_for_source_file:
      operator: airflow.providers.standard.sensors.python.PythonSensor
      python_callable_name: {{sensor_callable}}
      python_callable_file: {{callable_file}}
      poke_interval: {{poke_interval}}
      timeout: {{sensor_timeout}}
      soft_fail: false
      mode: reschedule
{{dataset_ingest_group}}
{{dataset_transform_group}}`,
  },
  {
    id: "db_extract_to_dwh",
    name: "Database — Extract to Data Warehouse",
    description: "Task group with N parallel SparkSubmitOperator tasks — one per dataset table",
    sourceType: "database",
    builtin: true,
    template: `{{dag_id}}:
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
{{dataset_extract_group}}`,
  },
  {
    id: "flat_file_simple_ingest",
    name: "Flat File — Simple Ingest (No Sensor)",
    description: "Task group with N parallel PythonOperator ingestion tasks — no file sensor, for pre-staged files on a fixed schedule",
    sourceType: "flat_file",
    builtin: true,
    template: `{{dag_id}}:
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
{{dataset_ingest_group_no_sensor}}`,
  },
];

function resolveSchedule(job) {
  const st = job.schedule_type || "manual";
  if (st === "manual") return "@once";
  if (st === "hourly") return "@hourly";
  if (st === "daily") return "@daily";
  if (st === "weekly") return "@weekly";
  if (st === "monthly") return "@monthly";
  if (st === "event_driven") return "None";
  if (st === "every_minutes") return job.cron_expression || "*/15 * * * *";
  if (st === "every_hours") return job.cron_expression || "0 */2 * * *";
  if (st === "custom" && job.cron_expression) return job.cron_expression;
  return "@once";
}

function sensorCallableName(platform) {
  const map = {
    sftp: "sftp_file_exists",
    local_fs: "local_file_exists",
    nas: "nas_file_exists",
    flat_file_delimited: "adls_file_exists",
    flat_file_fixed_width: "adls_file_exists",
    cobol_ebcdic: "adls_file_exists",
  };
  return map[platform] || "adls_file_exists";
}

function dsKeyFor(ds, idx) {
  return `${ds.schema || "default"}__${ds.table || `file_${idx}`}`.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
}

function buildIngestTaskBlock(ds, idx, job, callableFile, pad, withSensorDep) {
  const dsKey = dsKeyFor(ds, idx);
  const taskId = `ingest_${dsKey}`;
  const loadMethod = ds.load_method || job.load_method || "append";

  let sourcePath = job.file_source_folder || "/data/inbound/";
  if (job.file_source_mode === "wildcard") sourcePath = job.file_source_wildcard || "/data/inbound/*";
  else if (job.file_source_mode === "file_list") sourcePath = job.file_source_list?.[0]?.file_name || "input_file";

  const lines = [
    `${pad}${taskId}:`,
    `${pad}  operator: airflow.providers.standard.operators.python.PythonOperator`,
    `${pad}  python_callable_name: ingest_function`,
    `${pad}  python_callable_file: ${callableFile}`,
    `${pad}  op_args:`,
    `${pad}    - "${sourcePath}"`,
    `${pad}    - "${dsKey}"`,
    `${pad}    - "${loadMethod}"`,
    `${pad}    - "${ds.target_path || ""}"`,
  ];
  if (withSensorDep) {
    lines.push(`${pad}  dependencies:`);
    lines.push(`${pad}    - wait_for_source_file`);
  }
  return lines.join("\n");
}

function buildDatasetIngestGroup(job, connections, indent, withSensorDep) {
  const datasets = job.selected_datasets || job.selected_objects || [];
  const basePath = (job.dag_callable_base_path || "/data/dags/").replace(/\/+$/, "");
  const pipelineName = (job.name || "pipeline").replace(/[^a-z0-9_]/gi, "_").toLowerCase();
  const callableFile = `${basePath}/${pipelineName}_tasks.py`;
  const pad = " ".repeat(indent);

  if (datasets.length === 0) {
    const lines = [
      ``,
      `${pad}ingest_files:`,
      `${pad}  operator: airflow.providers.standard.operators.python.PythonOperator`,
      `${pad}  python_callable_name: ingest_function`,
      `${pad}  python_callable_file: ${callableFile}`,
      `${pad}  op_args:`,
      `${pad}    - "${job.file_source_folder || job.file_source_wildcard || "/data/inbound/"}"`,
      `${pad}    - "${pipelineName}"`,
      `${pad}    - "${job.load_method || "append"}"`,
    ];
    if (withSensorDep) {
      lines.push(`${pad}  dependencies:`);
      lines.push(`${pad}    - wait_for_source_file`);
    }
    return lines.join("\n");
  }

  if (datasets.length === 1) {
    return `\n${buildIngestTaskBlock(datasets[0], 0, job, callableFile, pad, withSensorDep)}`;
  }

  const innerPad = " ".repeat(indent + 2);
  const taskBlocks = datasets.map((ds, idx) =>
    buildIngestTaskBlock(ds, idx, job, callableFile, innerPad, false)
  );

  const lines = [
    ``,
    `${pad}ingest_datasets:`,
    `${pad}  task_group:`,
    `${pad}    tooltip: "Parallel ingestion of ${datasets.length} datasets"`,
  ];
  if (withSensorDep) {
    lines.push(`${pad}  dependencies:`);
    lines.push(`${pad}    - wait_for_source_file`);
  }
  lines.push(`${pad}  tasks:`);
  lines.push(taskBlocks.join("\n\n"));

  return lines.join("\n");
}

function buildTransformTaskBlock(ds, idx, job, sparkApp, basePath, pipelineName, pad) {
  const dsKey = dsKeyFor(ds, idx);
  const transformTaskId = `transform_${dsKey}`;
  const ingestTaskId = `ingest_${dsKey}`;
  const loadMethod = ds.load_method || job.load_method || "append";

  const lines = [
    `${pad}${transformTaskId}:`,
    `${pad}  operator: airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator`,
    `${pad}  application: ${sparkApp}`,
    `${pad}  application_args:`,
    `${pad}    - "--schema"`,
    `${pad}    - "${ds.schema || "default"}"`,
    `${pad}    - "--table"`,
    `${pad}    - "${ds.table || `file_${idx}`}"`,
    `${pad}    - "--load_method"`,
    `${pad}    - "${loadMethod}"`,
    `${pad}    - "--mapping_file"`,
    `${pad}    - "${basePath}/mappings/${pipelineName}_${dsKey}_mapping.json"`,
  ];
  if (ds.target_path) {
    lines.push(`${pad}    - "--target_path"`);
    lines.push(`${pad}    - "${ds.target_path}"`);
  }
  lines.push(`${pad}  conf:`);
  lines.push(`${pad}    spark.executor.memory: 4g`);
  lines.push(`${pad}    spark.executor.cores: "2"`);
  lines.push(`${pad}    spark.driver.memory: 2g`);
  lines.push(`${pad}  dependencies:`);
  lines.push(`${pad}    - ${ingestTaskId}`);
  return lines.join("\n");
}

function buildDatasetTransformGroup(job, connections, indent) {
  const datasets = job.selected_datasets || job.selected_objects || [];
  const columnMappings = job.column_mappings || {};
  const basePath = (job.dag_callable_base_path || "/data/dags/").replace(/\/+$/, "");
  const pipelineName = (job.name || "pipeline").replace(/[^a-z0-9_]/gi, "_").toLowerCase();
  const sparkApp = `${basePath}/${pipelineName}_spark.py`;
  const pad = " ".repeat(indent);

  const mappedDatasets = datasets.filter((ds) => {
    const dsMapKey = `${ds.schema}.${ds.table}`;
    return columnMappings[dsMapKey] && Array.isArray(columnMappings[dsMapKey]) && columnMappings[dsMapKey].length > 0;
  });

  if (mappedDatasets.length === 0) return "";

  if (mappedDatasets.length === 1) {
    return `\n${buildTransformTaskBlock(mappedDatasets[0], datasets.indexOf(mappedDatasets[0]), job, sparkApp, basePath, pipelineName, pad)}`;
  }

  const innerPad = " ".repeat(indent + 2);
  const taskBlocks = mappedDatasets.map((ds) => {
    const idx = datasets.indexOf(ds);
    return buildTransformTaskBlock(ds, idx, job, sparkApp, basePath, pipelineName, innerPad);
  });

  const lines = [
    ``,
    `${pad}transform_datasets:`,
    `${pad}  task_group:`,
    `${pad}    tooltip: "Parallel transformation of ${mappedDatasets.length} datasets with column mappings"`,
    `${pad}  dependencies:`,
    `${pad}    - ingest_datasets`,
    `${pad}  tasks:`,
  ];
  lines.push(taskBlocks.join("\n\n"));

  return lines.join("\n");
}

function buildExtractTaskBlock(ds, idx, job, connections, sparkApp, pad) {
  const sourceConn = connections.find(c => c.id === job.source_connection_id);
  const targetConn = connections.find(c => c.id === job.target_connection_id);
  const srcPlatform = sourceConn?.platform || "";
  const tgtPlatform = targetConn?.platform || "";
  const dsKey = `${ds.schema || "public"}__${ds.table || `table_${idx}`}`.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
  const taskId = `extract_${dsKey}`;
  const loadMethod = ds.load_method || job.load_method || "append";

  const lines = [
    `${pad}${taskId}:`,
    `${pad}  operator: airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator`,
    `${pad}  application: ${sparkApp}`,
    `${pad}  application_args:`,
    `${pad}    - "--source_platform"`,
    `${pad}    - "${srcPlatform}"`,
    `${pad}    - "--target_platform"`,
    `${pad}    - "${tgtPlatform}"`,
    `${pad}    - "--schema"`,
    `${pad}    - "${ds.schema || "public"}"`,
    `${pad}    - "--table"`,
    `${pad}    - "${ds.table || `table_${idx}`}"`,
    `${pad}    - "--load_method"`,
    `${pad}    - "${loadMethod}"`,
  ];
  if (ds.filter_query) {
    lines.push(`${pad}    - "--filter_query"`);
    lines.push(`${pad}    - "${ds.filter_query}"`);
  }
  if (ds.incremental_column) {
    lines.push(`${pad}    - "--incremental_column"`);
    lines.push(`${pad}    - "${ds.incremental_column}"`);
  }
  if (ds.target_path) {
    lines.push(`${pad}    - "--target_path"`);
    lines.push(`${pad}    - "${ds.target_path}"`);
  }
  lines.push(`${pad}  conf:`);
  lines.push(`${pad}    spark.executor.memory: 4g`);
  lines.push(`${pad}    spark.executor.cores: "2"`);
  lines.push(`${pad}    spark.driver.memory: 2g`);
  return lines.join("\n");
}

function buildDatasetExtractGroup(job, connections, indent) {
  const datasets = job.selected_datasets || job.selected_objects || [];
  const basePath = (job.dag_callable_base_path || "/data/dags/").replace(/\/+$/, "");
  const pipelineName = (job.name || "pipeline").replace(/[^a-z0-9_]/gi, "_").toLowerCase();
  const sparkApp = `${basePath}/${pipelineName}_spark.py`;
  const pad = " ".repeat(indent);

  if (datasets.length === 0) {
    return `\n${pad}extract_data:\n${pad}  operator: airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator\n${pad}  application: ${sparkApp}\n${pad}  conf:\n${pad}    spark.executor.memory: 4g\n${pad}    spark.executor.cores: "2"\n${pad}    spark.driver.memory: 2g`;
  }

  if (datasets.length === 1) {
    return `\n${buildExtractTaskBlock(datasets[0], 0, job, connections, sparkApp, pad)}`;
  }

  const innerPad = " ".repeat(indent + 2);
  const taskBlocks = datasets.map((ds, idx) =>
    buildExtractTaskBlock(ds, idx, job, connections, sparkApp, innerPad)
  );

  const lines = [
    ``,
    `${pad}extract_datasets:`,
    `${pad}  task_group:`,
    `${pad}    tooltip: "Parallel extraction of ${datasets.length} datasets"`,
    `${pad}  tasks:`,
  ];
  lines.push(taskBlocks.join("\n\n"));

  return lines.join("\n");
}

export function getTemplatesForSource(sourcePlatform, customTemplates = []) {
  const isFlatFile = FLAT_FILE_PLATFORMS.includes(sourcePlatform);
  const builtinFiltered = BUILTIN_TEMPLATES.filter(t =>
    t.sourceType === (isFlatFile ? "flat_file" : "database")
  );
  const customFiltered = customTemplates.filter(t =>
    !t.sourceType || t.sourceType === "any" || t.sourceType === (isFlatFile ? "flat_file" : "database")
  );
  return [...builtinFiltered, ...customFiltered];
}

export function getAllTemplates(customTemplates = []) {
  return [...BUILTIN_TEMPLATES, ...customTemplates.map(t => ({
    ...t,
    id: t.templateId || t.id,
    builtin: false,
  }))];
}

export function getDefaultTemplateId(sourcePlatform) {
  const isFlatFile = FLAT_FILE_PLATFORMS.includes(sourcePlatform);
  if (isFlatFile) return "flat_file_landing_to_raw";
  return "db_extract_to_dwh";
}

export function fillTemplate(templateId, job, connections, customTemplates = []) {
  const allTemplates = getAllTemplates(customTemplates);
  const tmpl = allTemplates.find(t => t.id === templateId);
  if (!tmpl) return "# No template selected";

  const sourceConn = connections.find(c => c.id === job.source_connection_id);
  const targetConn = connections.find(c => c.id === job.target_connection_id);
  const srcPlatform = sourceConn?.platform || "";
  const basePath = (job.dag_callable_base_path || "/data/dags/").replace(/\/+$/, "");
  const pipelineName = (job.name || "pipeline").replace(/[^a-z0-9_]/gi, "_").toLowerCase();
  const dagId = `dataflow__${pipelineName}`;
  const datasets = job.selected_datasets || job.selected_objects || [];

  const values = {
    dag_id: dagId,
    description: (job.description || `DataFlow pipeline: ${job.name || "untitled"}`).replace(/"/g, '\\"'),
    schedule: resolveSchedule(job),
    owner: job.assignment_group || "data-eng",
    email: job.email || "alerts@example.com",
    email_on_failure: job.email ? "true" : "false",
    retries: String(job.retry_config?.max_retries ?? 2),
    retry_delay_sec: String(job.retry_config?.retry_delay_seconds ?? 60),
    start_date: "2024-01-01",
    callable_file: `${basePath}/${pipelineName}_tasks.py`,
    spark_app: `${basePath}/${pipelineName}_spark.py`,
    source_platform: srcPlatform,
    target_platform: targetConn?.platform || "",
    source_name: sourceConn?.name || "",
    target_name: targetConn?.name || "",
    sensor_callable: sensorCallableName(srcPlatform),
    poke_interval: String(parseInt(job.event_config?.poll_interval) || 60),
    sensor_timeout: String((parseInt(job.event_config?.timeout_hours) || 10) * 3600),
    dataset_count: String(datasets.length),
    pipeline_name: pipelineName,
    dag_callable_base_path: basePath,
    dataset_ingest_group: buildDatasetIngestGroup(job, connections, 4, true),
    dataset_ingest_group_no_sensor: buildDatasetIngestGroup(job, connections, 4, false),
    dataset_transform_group: buildDatasetTransformGroup(job, connections, 4),
    dataset_extract_group: buildDatasetExtractGroup(job, connections, 4),
  };

  let output = tmpl.template;
  for (const [key, val] of Object.entries(values)) {
    const safeVal = String(val).replace(/\$/g, "$$$$");
    output = output.replace(new RegExp(`\\{\\{${key}\\}\\}`, "g"), safeVal);
  }

  const header = [
    `# dag-factory YAML — Generated by DataFlow`,
    `# Template: ${tmpl.name}`,
    `# Pipeline: ${job.name || "untitled"}`,
    `# Source: ${sourceConn?.name || "unknown"} (${srcPlatform})`,
    `# Target: ${targetConn?.name || "unknown"} (${targetConn?.platform || ""})`,
    `# Datasets: ${datasets.length}`,
    `# Generated: ${new Date().toISOString()}`,
    ``,
  ].join("\n");

  return header + output;
}

export const AVAILABLE_PLACEHOLDERS = [
  { key: "dag_id", desc: "Auto-generated DAG ID (dataflow__<pipeline_name>)" },
  { key: "description", desc: "Pipeline description" },
  { key: "schedule", desc: "Cron or @daily/@hourly/etc" },
  { key: "owner", desc: "Assignment group or 'data-eng'" },
  { key: "email", desc: "Alert email address" },
  { key: "email_on_failure", desc: "true/false" },
  { key: "retries", desc: "Number of retries" },
  { key: "retry_delay_sec", desc: "Retry delay in seconds" },
  { key: "start_date", desc: "DAG start date (YYYY-MM-DD)" },
  { key: "callable_file", desc: "Python callable file path" },
  { key: "spark_app", desc: "Spark application file path" },
  { key: "source_platform", desc: "Source connection platform" },
  { key: "target_platform", desc: "Target connection platform" },
  { key: "source_name", desc: "Source connection name" },
  { key: "target_name", desc: "Target connection name" },
  { key: "sensor_callable", desc: "Sensor function name (sftp_file_exists, etc.)" },
  { key: "poke_interval", desc: "Sensor poke interval in seconds" },
  { key: "sensor_timeout", desc: "Sensor timeout in seconds" },
  { key: "dataset_count", desc: "Number of datasets" },
  { key: "pipeline_name", desc: "Cleaned pipeline name" },
  { key: "dag_callable_base_path", desc: "Base path for DAG callable files" },
  { key: "dataset_ingest_group", desc: "Ingest task group (with sensor dependency)" },
  { key: "dataset_ingest_group_no_sensor", desc: "Ingest task group (no sensor)" },
  { key: "dataset_transform_group", desc: "Transform task group (only if column mappings)" },
  { key: "dataset_extract_group", desc: "DB extract task group (SparkSubmit per dataset)" },
];
