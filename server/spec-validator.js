const VALID_API_VERSION = "dataflow/v1";
const VALID_KIND = "IngestionPipeline";
const VALID_LOAD_METHODS = ["append", "replace", "upsert", "merge"];
const VALID_OPERATORS = ["PythonOperator", "SparkSubmitOperator", "custom_template"];
const VALID_SCHEDULE_TYPES = [
  "manual", "every_minutes", "every_hours", "hourly",
  "daily", "weekly", "monthly", "custom", "event_driven",
];
const VALID_SENSOR_TYPES = [
  "file_watcher", "s3_event", "db_sensor",
  "sftp_sensor", "api_webhook", "upstream_job",
];
const CRON_PART_REGEX = /^(\*|[0-9,\-\/]+)$/;
const CRON_RANGES = [
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "day", min: 1, max: 31 },
  { name: "month", min: 1, max: 12 },
  { name: "weekday", min: 0, max: 7 },
];

function addError(results, path, message) {
  results.errors.push({ path, message, severity: "error" });
}

function addWarning(results, path, message) {
  results.warnings.push({ path, message, severity: "warning" });
}

function isNonEmptyString(val) {
  return typeof val === "string" && val.trim().length > 0;
}

function parseCronValues(part) {
  const values = [];
  const segments = part.split(",");
  for (const seg of segments) {
    if (seg === "*") continue;
    const stepMatch = seg.match(/^(\*|\d+(?:-\d+)?)\/(\d+)$/);
    if (stepMatch) {
      values.push(parseInt(stepMatch[2], 10));
      if (stepMatch[1] !== "*") {
        const range = stepMatch[1].split("-").map(Number);
        values.push(...range);
      }
      continue;
    }
    const rangeMatch = seg.match(/^(\d+)-(\d+)$/);
    if (rangeMatch) {
      values.push(parseInt(rangeMatch[1], 10), parseInt(rangeMatch[2], 10));
      continue;
    }
    const num = parseInt(seg, 10);
    if (!isNaN(num)) values.push(num);
    else return null;
  }
  return values;
}

function validateCron(cron) {
  if (!cron || typeof cron !== "string") return { valid: false, error: "empty cron expression" };
  const parts = cron.trim().split(/\s+/);
  if (parts.length !== 5) return { valid: false, error: "must have exactly 5 parts (min hour day month weekday)" };

  for (let i = 0; i < 5; i++) {
    if (!CRON_PART_REGEX.test(parts[i])) {
      return { valid: false, error: CRON_RANGES[i].name + " field has invalid characters" };
    }
    const vals = parseCronValues(parts[i]);
    if (vals === null) return { valid: false, error: CRON_RANGES[i].name + " field is malformed" };
    for (const v of vals) {
      if (v < CRON_RANGES[i].min || v > CRON_RANGES[i].max) {
        return { valid: false, error: CRON_RANGES[i].name + " value " + v + " out of range (" + CRON_RANGES[i].min + "-" + CRON_RANGES[i].max + ")" };
      }
    }
  }
  return { valid: true };
}

function getCronMinuteInterval(cron) {
  if (!cron) return null;
  const parts = cron.trim().split(/\s+/);
  if (parts.length < 1) return null;
  const stepMatch = parts[0].match(/^\*\/(\d+)$/);
  if (stepMatch) return parseInt(stepMatch[1], 10);
  const allMinuteMatch = parts[0].match(/^(\d+(?:,\d+)*)$/);
  if (allMinuteMatch && parts[1] === "*" && parts[2] === "*") {
    const mins = allMinuteMatch[1].split(",").map(Number);
    if (mins.length > 30) return 1;
  }
  return null;
}

export function validateSpec(spec) {
  const results = { errors: [], warnings: [] };

  if (!spec || typeof spec !== "object") {
    addError(results, "", "Spec must be a non-null object");
    return { valid: false, ...results };
  }

  if (spec.apiVersion !== VALID_API_VERSION) {
    addError(results, "apiVersion", "Must be " + VALID_API_VERSION + ", got " + (spec.apiVersion || "(missing)"));
  }

  if (spec.kind !== VALID_KIND) {
    addError(results, "kind", "Must be " + VALID_KIND + ", got " + (spec.kind || "(missing)"));
  }

  const meta = spec.metadata;
  if (!meta || typeof meta !== "object") {
    addError(results, "metadata", "Metadata section is required");
  } else {
    if (!isNonEmptyString(meta.name)) {
      addError(results, "metadata.name", "Pipeline name is required");
    }
    if (!isNonEmptyString(meta.description)) {
      addWarning(results, "metadata.description", "Description is empty — consider adding one for documentation");
    }
  }

  const s = spec.spec;
  if (!s || typeof s !== "object") {
    addError(results, "spec", "Spec body is required");
    return { valid: results.errors.length === 0, ...results };
  }

  if (!s.source || !s.source.connection_id) {
    addError(results, "spec.source.connection_id", "Source connection ID is required");
  }
  if (!s.target || !s.target.connection_id) {
    addError(results, "spec.target.connection_id", "Target connection ID is required");
  }
  if (s.source?.connection_id && s.target?.connection_id && s.source.connection_id === s.target.connection_id) {
    addError(results, "spec.target.connection_id", "Source and target connections must be different");
  }

  const datasets = s.datasets;
  if (!Array.isArray(datasets) || datasets.length === 0) {
    addError(results, "spec.datasets", "At least one dataset is required");
  } else {
    datasets.forEach((ds, i) => {
      const prefix = `spec.datasets[${i}]`;

      if (!isNonEmptyString(ds.schema)) {
        addError(results, `${prefix}.schema`, "Schema is required");
      }
      if (!isNonEmptyString(ds.table)) {
        addError(results, `${prefix}.table`, "Table name is required");
      }
      if (ds.load_method && !VALID_LOAD_METHODS.includes(ds.load_method)) {
        addError(results, `${prefix}.load_method`, `Must be one of: ${VALID_LOAD_METHODS.join(", ")}`);
      }
      if (!ds.target_path) {
        addWarning(results, `${prefix}.target_path`, "Target path is empty — will use default destination");
      }
      if (!ds.filter_query) {
        addWarning(results, `${prefix}.filter_query`, "No filter query — full table will be ingested");
      }

      if (ds.execution && typeof ds.execution === "object") {
        if (ds.execution.operator && !VALID_OPERATORS.includes(ds.execution.operator)) {
          addError(results, `${prefix}.execution.operator`, `Must be one of: ${VALID_OPERATORS.join(", ")}`);
        }
      }
    });
  }

  const sched = s.schedule;
  if (!sched || typeof sched !== "object") {
    addError(results, "spec.schedule", "Schedule section is required");
  } else {
    if (!sched.type || !VALID_SCHEDULE_TYPES.includes(sched.type)) {
      addError(results, "spec.schedule.type", `Must be one of: ${VALID_SCHEDULE_TYPES.join(", ")}`);
    }

    const needsCron = ["every_minutes", "every_hours", "hourly", "daily", "weekly", "monthly", "custom"];
    if (needsCron.includes(sched.type)) {
      if (!isNonEmptyString(sched.cron_expression)) {
        addError(results, "spec.schedule.cron_expression", "Cron expression is required for this schedule type");
      } else {
        const cronResult = validateCron(sched.cron_expression);
        if (!cronResult.valid) {
          addError(results, "spec.schedule.cron_expression", "Invalid cron: " + cronResult.error);
        } else {
          const minInterval = getCronMinuteInterval(sched.cron_expression);
          if (minInterval !== null && minInterval <= 1) {
            addWarning(results, "spec.schedule.cron_expression", "Cron runs every minute or more frequently — this may cause excessive load");
          }
        }
      }
    }

    if (sched.type === "event_driven") {
      if (!sched.event_sensor || typeof sched.event_sensor !== "object") {
        addError(results, "spec.schedule.event_sensor", "Event sensor configuration is required for event-driven schedules");
      } else {
        if (!sched.event_sensor.sensor_type || !VALID_SENSOR_TYPES.includes(sched.event_sensor.sensor_type)) {
          addError(results, "spec.schedule.event_sensor.sensor_type", `Must be one of: ${VALID_SENSOR_TYPES.join(", ")}`);
        }
        const cfg = sched.event_sensor.config;
        if (cfg && typeof cfg === "object") {
          const sensorType = sched.event_sensor.sensor_type;
          if ((sensorType === "file_watcher" || sensorType === "sftp_sensor" || sensorType === "s3_event") && !isNonEmptyString(cfg.watch_path)) {
            addWarning(results, "spec.schedule.event_sensor.config.watch_path", "Watch path is empty — sensor may not trigger correctly");
          }
          if (sensorType === "db_sensor" && !isNonEmptyString(cfg.sql_condition)) {
            addWarning(results, "spec.schedule.event_sensor.config.sql_condition", "SQL condition is empty — sensor may not trigger correctly");
          }
          if (sensorType === "upstream_job" && !isNonEmptyString(cfg.upstream_job)) {
            addWarning(results, "spec.schedule.event_sensor.config.upstream_job", "Upstream job name is empty");
          }
        }
      }
    }
  }

  const exec = s.execution;
  if (!exec || typeof exec !== "object") {
    addError(results, "spec.execution", "Execution section is required");
  } else {
    if (!exec.operator || !VALID_OPERATORS.includes(exec.operator)) {
      addError(results, "spec.execution.operator", `Must be one of: ${VALID_OPERATORS.join(", ")}`);
    }

    if (exec.operator === "SparkSubmitOperator") {
      if (!exec.spark_config || !isNonEmptyString(exec.spark_config?.application)) {
        addError(results, "spec.execution.spark_config.application", "Spark application script is required for SparkSubmitOperator");
      }
    }

    if (exec.operator === "custom_template") {
      if (!isNonEmptyString(exec.custom_template)) {
        addError(results, "spec.execution.custom_template", "Custom template content is required when using custom_template operator");
      }
    }
  }

  const retry = s.retry;
  if (retry && typeof retry === "object") {
    if (typeof retry.max_retries === "number" && retry.max_retries < 0) {
      addError(results, "spec.retry.max_retries", "Must be 0 or greater");
    }
    if (typeof retry.retry_delay_seconds === "number" && retry.retry_delay_seconds <= 0) {
      addError(results, "spec.retry.retry_delay_seconds", "Must be greater than 0");
    }
  }

  if (!s.column_mappings || Object.keys(s.column_mappings).length === 0) {
    addWarning(results, "spec.column_mappings", "No column mappings defined — source columns will pass through unchanged");
  }

  return { valid: results.errors.length === 0, ...results };
}

export async function validateSpecWithDB(spec, pool, entityNameToTable) {
  const results = validateSpec(spec);

  if (!spec?.spec?.source?.connection_id && !spec?.spec?.target?.connection_id) {
    return results;
  }

  try {
    const table = entityNameToTable("Connection");
    const ids = [spec.spec.source?.connection_id, spec.spec.target?.connection_id].filter(Boolean);

    if (ids.length > 0) {
      const found = new Map();

      const numericIds = ids.map(id => parseInt(id)).filter(id => !isNaN(id));
      if (numericIds.length > 0) {
        const placeholders = numericIds.map((_, i) => "$" + (i + 1)).join(", ");
        const result = await pool.query(
          'SELECT id, data->>\'name\' as name, data->>\'status\' as status FROM "' + table + '" WHERE id IN (' + placeholders + ')',
          numericIds
        );
        result.rows.forEach(r => found.set(String(r.id), r));
      }

      const nonNumericIds = ids.filter(id => isNaN(parseInt(id)));
      if (nonNumericIds.length > 0) {
        const placeholders = nonNumericIds.map((_, i) => "$" + (i + 1)).join(", ");
        const result = await pool.query(
          'SELECT id, data->>\'name\' as name, data->>\'status\' as status FROM "' + table + '" WHERE data->>\'id\' IN (' + placeholders + ')',
          nonNumericIds
        );
        result.rows.forEach(r => {
          const dataId = r.name ? String(r.id) : String(r.id);
          found.set(dataId, r);
          nonNumericIds.forEach(nid => { if (!found.has(nid)) found.set(nid, r); });
        });
      }

      const srcId = String(spec.spec.source?.connection_id);
      const tgtId = String(spec.spec.target?.connection_id);

      if (spec.spec.source?.connection_id && !found.has(srcId)) {
        results.errors.push({ path: "spec.source.connection_id", message: "Source connection (ID: " + srcId + ") not found in database", severity: "error" });
        results.valid = false;
      } else if (spec.spec.source?.connection_id && found.get(srcId)?.status === "inactive") {
        results.warnings.push({ path: "spec.source.connection_id", message: 'Source connection "' + found.get(srcId).name + '" is inactive', severity: "warning" });
      }

      if (spec.spec.target?.connection_id && !found.has(tgtId)) {
        results.errors.push({ path: "spec.target.connection_id", message: "Target connection (ID: " + tgtId + ") not found in database", severity: "error" });
        results.valid = false;
      } else if (spec.spec.target?.connection_id && found.get(tgtId)?.status === "inactive") {
        results.warnings.push({ path: "spec.target.connection_id", message: 'Target connection "' + found.get(tgtId).name + '" is inactive', severity: "warning" });
      }
    }
  } catch (err) {
    results.warnings.push({ path: "database", message: `Could not verify connections: ${err.message}`, severity: "warning" });
  }

  return results;
}
