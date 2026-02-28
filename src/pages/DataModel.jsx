import { useState } from "react";
import { Database, Table2, Link2, ChevronDown, ChevronRight, Key, Hash, Type, Calendar, ToggleLeft, List, Code2, Copy, Check } from "lucide-react";
import { cn } from "@/lib/utils";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

const DDL = `-- ============================================================
-- DataFlow Platform — PostgreSQL DDL (Performance-Optimized)
-- Generated: ${new Date().toISOString().slice(0, 10)}
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ------------------------------------------------------------
-- ENUM TYPES
-- ------------------------------------------------------------

CREATE TYPE platform_enum           AS ENUM ('sql_server','oracle','postgresql','mysql','mongodb','adls2','s3','flat_file_delimited','flat_file_fixed_width','cobol_ebcdic','sftp','nas','local_fs','azure_synapse','snowflake','databricks');
CREATE TYPE auth_method_enum        AS ENUM ('password','key','connection_string','managed_identity','sftp_key','none','vault_credentials');
CREATE TYPE connection_status_enum  AS ENUM ('active','inactive','error','pending_setup');
CREATE TYPE schedule_type_enum      AS ENUM ('manual','hourly','daily','weekly','custom');
CREATE TYPE job_status_enum         AS ENUM ('idle','running','completed','failed','paused');
CREATE TYPE run_status_enum         AS ENUM ('running','completed','failed','cancelled','retrying');
CREATE TYPE triggered_by_enum       AS ENUM ('manual','schedule','retry');
CREATE TYPE log_type_enum           AS ENUM ('info','warning','error','success');
CREATE TYPE log_category_enum       AS ENUM ('connection','job','system','authentication');
CREATE TYPE change_type_enum        AS ENUM ('created','updated','paused','resumed','deleted');
CREATE TYPE prereq_type_enum        AS ENUM ('nsg_rule','egress_rule','nas_path','dba_access','firewall_rule','service_account','vpn_tunnel','vault_credentials','app_id','entitlement','other');
CREATE TYPE assigned_team_enum      AS ENUM ('networking','dba','security','storage','platform','other');
CREATE TYPE prereq_status_enum      AS ENUM ('pending','in_progress','completed','rejected','not_required');
CREATE TYPE priority_enum           AS ENUM ('high','medium','low');
CREATE TYPE audit_action_enum       AS ENUM ('create','update','delete','login','logout','export','import');
CREATE TYPE ingestion_job_type_enum AS ENUM ('full_load','incremental','cdc','snapshot');
CREATE TYPE ingestion_status_enum   AS ENUM ('pending','running','completed','failed','cancelled');
CREATE TYPE function_type_enum      AS ENUM ('transform','validate','enrich','filter','aggregate');
CREATE TYPE template_type_enum      AS ENUM ('dag_factory','custom','jinja','python');

-- ------------------------------------------------------------
-- CONNECTION
-- ------------------------------------------------------------

CREATE TABLE connection (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by      TEXT NOT NULL,

    name            TEXT NOT NULL,
    source_system_name TEXT,
    description     TEXT,
    car_id          TEXT,
    platform        platform_enum NOT NULL,
    host            TEXT,
    port            INTEGER,
    database        TEXT,
    username        TEXT,
    auth_method     auth_method_enum DEFAULT 'password',
    region          TEXT,
    bucket_container TEXT,
    file_config     JSONB,
    vault_config    JSONB,
    tags            TEXT[],
    environment     TEXT,
    status          connection_status_enum DEFAULT 'active',
    last_tested     TIMESTAMPTZ,
    notes           TEXT
);

CREATE INDEX idx_connection_status    ON connection(status);
CREATE INDEX idx_connection_platform  ON connection(platform);
CREATE INDEX idx_connection_env       ON connection(environment);
CREATE INDEX idx_connection_file_cfg  ON connection USING GIN(file_config);
CREATE INDEX idx_connection_vault_cfg ON connection USING GIN(vault_config);
CREATE INDEX idx_connection_tags      ON connection USING GIN(tags);
CREATE INDEX idx_connection_name_trgm ON connection USING GIN(name gin_trgm_ops);
CREATE INDEX idx_connection_fts       ON connection USING GIN(
  to_tsvector('english', coalesce(name,'') || ' ' || coalesce(description,'') || ' ' || coalesce(platform::text,''))
);

-- ------------------------------------------------------------
-- PIPELINE
-- ------------------------------------------------------------

CREATE TABLE pipeline (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by           TEXT NOT NULL,

    name                 TEXT NOT NULL,
    description          TEXT,
    source_connection_id UUID NOT NULL REFERENCES connection(id) ON DELETE RESTRICT,
    target_connection_id UUID NOT NULL REFERENCES connection(id) ON DELETE RESTRICT,
    source_path          TEXT,
    source_format        TEXT,
    selected_objects     JSONB,
    schedule_type        schedule_type_enum DEFAULT 'manual',
    cron_expression      TEXT,
    status               job_status_enum DEFAULT 'idle',
    retry_config         JSONB,
    use_custom_calendar  BOOLEAN DEFAULT false,
    include_calendar_id  TEXT,
    exclude_calendar_id  TEXT,
    column_mappings      JSONB,
    dq_rules             JSONB,
    tags                 TEXT[],
    owner                TEXT,
    dag_id               TEXT,
    last_run             TIMESTAMPTZ,
    next_run             TIMESTAMPTZ,
    total_runs           INTEGER DEFAULT 0,
    successful_runs      INTEGER DEFAULT 0,
    failed_runs          INTEGER DEFAULT 0
);

CREATE INDEX idx_pipeline_status    ON pipeline(status);
CREATE INDEX idx_pipeline_source    ON pipeline(source_connection_id);
CREATE INDEX idx_pipeline_target    ON pipeline(target_connection_id);
CREATE INDEX idx_pipeline_owner     ON pipeline(owner);
CREATE INDEX idx_pipeline_dag_id    ON pipeline(dag_id);
CREATE INDEX idx_pipeline_tags      ON pipeline USING GIN(tags);
CREATE INDEX idx_pipeline_sel_obj   ON pipeline USING GIN(selected_objects);
CREATE INDEX idx_pipeline_col_map   ON pipeline USING GIN(column_mappings);
CREATE INDEX idx_pipeline_dq        ON pipeline USING GIN(dq_rules);
CREATE INDEX idx_pipeline_name_trgm ON pipeline USING GIN(name gin_trgm_ops);
CREATE INDEX idx_pipeline_fts       ON pipeline USING GIN(
  to_tsvector('english', coalesce(name,'') || ' ' || coalesce(description,''))
);

-- ------------------------------------------------------------
-- PIPELINE RUN
-- ------------------------------------------------------------

CREATE TABLE pipeline_run (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date      TIMESTAMPTZ NOT NULL DEFAULT now(),

    pipeline_id       UUID NOT NULL REFERENCES pipeline(id) ON DELETE CASCADE,
    run_number        INTEGER,
    status            run_status_enum NOT NULL,
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    duration_seconds  NUMERIC,
    rows_processed    BIGINT DEFAULT 0,
    bytes_transferred BIGINT DEFAULT 0,
    objects_completed TEXT[],
    objects_failed    TEXT[],
    retry_count       INTEGER DEFAULT 0,
    error_message     TEXT,
    triggered_by      triggered_by_enum DEFAULT 'manual'
);

CREATE INDEX idx_run_pipeline       ON pipeline_run(pipeline_id);
CREATE INDEX idx_run_status         ON pipeline_run(status);
CREATE INDEX idx_run_pipe_status    ON pipeline_run(pipeline_id, status);
CREATE INDEX idx_run_triggered      ON pipeline_run(triggered_by);
CREATE INDEX idx_run_started        ON pipeline_run(started_at DESC);

-- ------------------------------------------------------------
-- ACTIVITY LOG
-- ------------------------------------------------------------

CREATE TABLE activity_log (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date  TIMESTAMPTZ NOT NULL DEFAULT now(),

    log_type      log_type_enum NOT NULL,
    category      log_category_enum NOT NULL,
    message       TEXT NOT NULL,
    job_id        UUID REFERENCES pipeline(id) ON DELETE SET NULL,
    run_id        UUID REFERENCES pipeline_run(id) ON DELETE SET NULL,
    connection_id UUID REFERENCES connection(id) ON DELETE SET NULL,
    object_name   TEXT,
    details       JSONB,
    stack_trace   TEXT
);

CREATE INDEX idx_alog_category ON activity_log(category);
CREATE INDEX idx_alog_log_type ON activity_log(log_type);
CREATE INDEX idx_alog_job      ON activity_log(job_id);
CREATE INDEX idx_alog_conn     ON activity_log(connection_id);
CREATE INDEX idx_alog_created  ON activity_log(created_date DESC);
CREATE INDEX idx_alog_fts      ON activity_log USING GIN(
  to_tsvector('english', coalesce(message,'') || ' ' || coalesce(category::text,''))
);

-- ------------------------------------------------------------
-- PIPELINE VERSION
-- ------------------------------------------------------------

CREATE TABLE pipeline_version (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date   TIMESTAMPTZ NOT NULL DEFAULT now(),

    pipeline_id    UUID NOT NULL REFERENCES pipeline(id) ON DELETE CASCADE,
    version_number INTEGER NOT NULL,
    label          TEXT,
    commit_message TEXT,
    snapshot       JSONB NOT NULL,
    changed_by     TEXT,
    change_type    change_type_enum NOT NULL,

    UNIQUE (pipeline_id, version_number)
);

CREATE INDEX idx_pver_pipeline ON pipeline_version(pipeline_id, version_number DESC);
CREATE INDEX idx_pver_snapshot ON pipeline_version USING GIN(snapshot);

-- ------------------------------------------------------------
-- CONNECTION PREREQUISITE
-- ------------------------------------------------------------

CREATE TABLE connection_prerequisite (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date     TIMESTAMPTZ NOT NULL DEFAULT now(),

    connection_id    UUID NOT NULL REFERENCES connection(id) ON DELETE CASCADE,
    prereq_type      prereq_type_enum NOT NULL,
    title            TEXT NOT NULL,
    description      TEXT,
    assigned_team    assigned_team_enum NOT NULL,
    status           prereq_status_enum NOT NULL DEFAULT 'pending',
    priority         priority_enum DEFAULT 'medium',
    requested_by     TEXT,
    ticket_reference TEXT,
    due_date         TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    notes            TEXT
);

CREATE INDEX idx_prereq_connection ON connection_prerequisite(connection_id);
CREATE INDEX idx_prereq_status     ON connection_prerequisite(status);
CREATE INDEX idx_prereq_type       ON connection_prerequisite(prereq_type);

-- ------------------------------------------------------------
-- AUDIT LOG
-- ------------------------------------------------------------

CREATE TABLE audit_log (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date  TIMESTAMPTZ NOT NULL DEFAULT now(),

    entity_type   TEXT NOT NULL,
    entity_id     UUID,
    action        audit_action_enum NOT NULL,
    changed_by    TEXT NOT NULL,
    changes       JSONB,
    ip_address    TEXT
);

CREATE INDEX idx_audit_entity   ON audit_log(entity_type, entity_id);
CREATE INDEX idx_audit_action   ON audit_log(action);
CREATE INDEX idx_audit_user     ON audit_log(changed_by);
CREATE INDEX idx_audit_created  ON audit_log(created_date DESC);

-- ------------------------------------------------------------
-- INGESTION JOB
-- ------------------------------------------------------------

CREATE TABLE ingestion_job (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date   TIMESTAMPTZ NOT NULL DEFAULT now(),

    pipeline_id    UUID NOT NULL REFERENCES pipeline(id) ON DELETE CASCADE,
    job_type       ingestion_job_type_enum NOT NULL,
    status         ingestion_status_enum NOT NULL DEFAULT 'pending',
    config         JSONB,
    last_run       TIMESTAMPTZ,
    error_message  TEXT
);

CREATE INDEX idx_ijob_pipeline ON ingestion_job(pipeline_id);
CREATE INDEX idx_ijob_status   ON ingestion_job(status);
CREATE INDEX idx_ijob_type     ON ingestion_job(job_type);

-- ------------------------------------------------------------
-- AIRFLOW DAG
-- ------------------------------------------------------------

CREATE TABLE airflow_dag (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date        TIMESTAMPTZ NOT NULL DEFAULT now(),

    pipeline_id         UUID NOT NULL REFERENCES pipeline(id) ON DELETE CASCADE,
    dag_id              TEXT NOT NULL,
    dag_yaml            TEXT,
    dag_factory_config  JSONB,
    deployed_at         TIMESTAMPTZ,
    deployed_by         TEXT,
    version             INTEGER DEFAULT 1
);

CREATE INDEX idx_adag_pipeline  ON airflow_dag(pipeline_id);
CREATE INDEX idx_adag_dag_id    ON airflow_dag(dag_id);

-- ------------------------------------------------------------
-- CUSTOM FUNCTION
-- ------------------------------------------------------------

CREATE TABLE custom_function (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date   TIMESTAMPTZ NOT NULL DEFAULT now(),

    name           TEXT NOT NULL,
    description    TEXT,
    function_type  function_type_enum NOT NULL,
    code           TEXT NOT NULL,
    language       TEXT DEFAULT 'python',
    parameters     JSONB,
    created_by     TEXT NOT NULL
);

CREATE INDEX idx_cfunc_type      ON custom_function(function_type);
CREATE INDEX idx_cfunc_lang      ON custom_function(language);
CREATE INDEX idx_cfunc_name_trgm ON custom_function USING GIN(name gin_trgm_ops);

-- ------------------------------------------------------------
-- CONNECTION PROFILE (reusable template)
-- ------------------------------------------------------------

CREATE TABLE connection_profile (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by       TEXT NOT NULL,

    name             TEXT NOT NULL,
    description      TEXT,
    platform         platform_enum NOT NULL,
    host             TEXT,
    port             INTEGER,
    database         TEXT,
    username         TEXT,
    auth_method      auth_method_enum,
    region           TEXT,
    bucket_container TEXT,
    file_config      JSONB,
    notes            TEXT
);

CREATE INDEX idx_cprof_platform ON connection_profile(platform);

-- ------------------------------------------------------------
-- DATA CATALOG ENTRY
-- ------------------------------------------------------------

CREATE TABLE data_catalog_entry (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date          TIMESTAMPTZ NOT NULL DEFAULT now(),

    name                  TEXT NOT NULL,
    description           TEXT,
    source_connection_id  UUID REFERENCES connection(id) ON DELETE SET NULL,
    schema_name           TEXT,
    table_name            TEXT,
    columns               JSONB,
    row_count             BIGINT,
    last_profiled         TIMESTAMPTZ,
    tags                  TEXT[],
    owner                 TEXT
);

CREATE INDEX idx_dcat_connection ON data_catalog_entry(source_connection_id);
CREATE INDEX idx_dcat_tags       ON data_catalog_entry USING GIN(tags);
CREATE INDEX idx_dcat_name_trgm  ON data_catalog_entry USING GIN(name gin_trgm_ops);

-- ------------------------------------------------------------
-- DAG TEMPLATE
-- ------------------------------------------------------------

CREATE TABLE dag_template (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_date      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_date      TIMESTAMPTZ NOT NULL DEFAULT now(),

    name              TEXT NOT NULL,
    description       TEXT,
    template_type     template_type_enum NOT NULL,
    template_content  TEXT NOT NULL,
    parameters        JSONB,
    created_by        TEXT NOT NULL,
    version           INTEGER DEFAULT 1
);

CREATE INDEX idx_dtpl_type ON dag_template(template_type);

-- ------------------------------------------------------------
-- RETENTION FUNCTIONS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION purge_old_activity_logs(retention_days INTEGER DEFAULT 30)
RETURNS TABLE(deleted_count INTEGER) LANGUAGE plpgsql AS $$
DECLARE
  count_deleted INTEGER;
BEGIN
  DELETE FROM activity_log
  WHERE created_date < (now() - (retention_days || ' days')::INTERVAL);

  GET DIAGNOSTICS count_deleted = ROW_COUNT;
  RETURN QUERY SELECT count_deleted;
END;
$$;

CREATE OR REPLACE FUNCTION purge_old_pipeline_runs(retention_days INTEGER DEFAULT 90)
RETURNS TABLE(deleted_count INTEGER) LANGUAGE plpgsql AS $$
DECLARE
  count_deleted INTEGER;
BEGIN
  DELETE FROM pipeline_run
  WHERE created_date < (now() - (retention_days || ' days')::INTERVAL);

  GET DIAGNOSTICS count_deleted = ROW_COUNT;
  RETURN QUERY SELECT count_deleted;
END;
$$;

-- ------------------------------------------------------------
-- updated_date trigger (apply to all tables with updated_date)
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION set_updated_date()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_date = now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_connection_updated
  BEFORE UPDATE ON connection
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();

CREATE TRIGGER trg_pipeline_updated
  BEFORE UPDATE ON pipeline
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();

CREATE TRIGGER trg_connection_prerequisite_updated
  BEFORE UPDATE ON connection_prerequisite
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();

CREATE TRIGGER trg_connection_profile_updated
  BEFORE UPDATE ON connection_profile
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();

CREATE TRIGGER trg_ingestion_job_updated
  BEFORE UPDATE ON ingestion_job
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();

CREATE TRIGGER trg_airflow_dag_updated
  BEFORE UPDATE ON airflow_dag
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();

CREATE TRIGGER trg_custom_function_updated
  BEFORE UPDATE ON custom_function
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();

CREATE TRIGGER trg_data_catalog_entry_updated
  BEFORE UPDATE ON data_catalog_entry
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();

CREATE TRIGGER trg_dag_template_updated
  BEFORE UPDATE ON dag_template
  FOR EACH ROW EXECUTE FUNCTION set_updated_date();
`;

const typeIcon = (type) => {
  if (type === "string") return <Type className="w-3 h-3 text-emerald-500" />;
  if (type === "number") return <Hash className="w-3 h-3 text-blue-500" />;
  if (type === "boolean") return <ToggleLeft className="w-3 h-3 text-violet-500" />;
  if (type === "array") return <List className="w-3 h-3 text-amber-500" />;
  if (type === "object") return <Database className="w-3 h-3 text-rose-500" />;
  if (type === "date-time") return <Calendar className="w-3 h-3 text-cyan-500" />;
  return <Type className="w-3 h-3 text-slate-400" />;
};

const typeColor = (type) => {
  if (type === "string") return "text-emerald-600 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950/40 border-emerald-200 dark:border-emerald-800";
  if (type === "number") return "text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-950/40 border-blue-200 dark:border-blue-800";
  if (type === "boolean") return "text-violet-600 dark:text-violet-400 bg-violet-50 dark:bg-violet-950/40 border-violet-200 dark:border-violet-800";
  if (type === "array") return "text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-950/40 border-amber-200 dark:border-amber-800";
  if (type === "object") return "text-rose-600 dark:text-rose-400 bg-rose-50 dark:bg-rose-950/40 border-rose-200 dark:border-rose-800";
  if (type === "date-time") return "text-cyan-600 dark:text-cyan-400 bg-cyan-50 dark:bg-cyan-950/40 border-cyan-200 dark:border-cyan-800";
  return "text-slate-500 dark:text-slate-400 bg-slate-50 dark:bg-slate-800 border-slate-200 dark:border-slate-700";
};

const entities = [
  {
    name: "Connection",
    color: "from-blue-500 to-indigo-600",
    description: "Represents a data source or target system with its credentials and configuration.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "created_by", type: "string", required: true, note: "User email (auto)" },
      { name: "name", type: "string", required: true, note: "Connection display name" },
      { name: "source_system_name", type: "string", required: false, note: "Name of the source system" },
      { name: "description", type: "string", required: false, note: "Connection description" },
      { name: "car_id", type: "string", required: false, note: "CarID identifier" },
      { name: "platform", type: "string", required: true, note: 'sql_server, oracle, postgresql, mysql, mongodb, adls2, s3, sftp, nas, azure_synapse, snowflake, databricks, ...' },
      { name: "host", type: "string", required: false, note: "Hostname, IP, or URL" },
      { name: "port", type: "number", required: false, note: "TCP port" },
      { name: "database", type: "string", required: false, note: "Database / container name" },
      { name: "username", type: "string", required: false, note: "Username or access key ID" },
      { name: "auth_method", type: "string", required: false, note: 'password | key | sftp_key | connection_string | managed_identity | vault_credentials | none' },
      { name: "region", type: "string", required: false, note: "Cloud region (e.g. eastus, us-east-1)" },
      { name: "bucket_container", type: "string", required: false, note: "S3 bucket or ADLS container" },
      { name: "file_config", type: "object", required: false, note: "delimiter, encoding, has_header, quote_char, ..." },
      { name: "vault_config", type: "object", required: false, note: "Vault path, secret engine, role config" },
      { name: "tags", type: "array", required: false, note: "Text tags for categorization" },
      { name: "environment", type: "string", required: false, note: "dev | staging | production" },
      { name: "status", type: "string", required: false, note: 'active | inactive | error | pending_setup' },
      { name: "last_tested", type: "date-time", required: false, note: "Timestamp of last connection test" },
      { name: "notes", type: "string", required: false, note: "Free-text notes" },
    ]
  },
  {
    name: "Pipeline",
    color: "from-emerald-500 to-teal-600",
    description: "Defines a data transfer pipeline from a source to a target, including schedule and retry config.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "created_by", type: "string", required: true, note: "User email (auto)" },
      { name: "name", type: "string", required: true, note: "Pipeline display name" },
      { name: "description", type: "string", required: false, note: "Optional description" },
      { name: "source_connection_id", type: "string", required: true, note: "→ Connection.id (source)" },
      { name: "target_connection_id", type: "string", required: true, note: "→ Connection.id (target)" },
      { name: "source_path", type: "string", required: false, note: "Path for flat file source" },
      { name: "source_format", type: "string", required: false, note: "Format for flat file source (e.g. CSV, delimited, fixed-width)" },
      { name: "selected_objects", type: "array", required: false, note: "Array of {schema, table, filter_query, target_path, incremental_column}" },
      { name: "schedule_type", type: "string", required: false, note: 'manual | hourly | daily | weekly | custom' },
      { name: "cron_expression", type: "string", required: false, note: "Used when schedule_type = custom" },
      { name: "status", type: "string", required: false, note: 'idle | running | completed | failed | paused' },
      { name: "retry_config", type: "object", required: false, note: "{max_retries, retry_delay_seconds, exponential_backoff}" },
      { name: "last_run", type: "date-time", required: false, note: "Timestamp of last execution" },
      { name: "next_run", type: "date-time", required: false, note: "Timestamp of next scheduled run" },
      { name: "use_custom_calendar", type: "boolean", required: false, note: "Enable include/exclude calendar rules" },
      { name: "include_calendar_id", type: "string", required: false, note: "Calendar ID — run ONLY on these dates" },
      { name: "exclude_calendar_id", type: "string", required: false, note: "Calendar ID — SKIP runs on these dates" },
      { name: "column_mappings", type: "object", required: false, note: 'Keyed by "schema.table": [{source, target, transformation}]' },
      { name: "dq_rules", type: "object", required: false, note: 'Keyed by "schema.table": {dataset_rules: [], column_rules: []}' },
      { name: "tags", type: "array", required: false, note: "Text tags for categorization" },
      { name: "owner", type: "string", required: false, note: "Pipeline owner / responsible person" },
      { name: "dag_id", type: "string", required: false, note: "Associated Airflow DAG identifier" },
      { name: "total_runs", type: "number", required: false, note: "Cumulative run count" },
      { name: "successful_runs", type: "number", required: false, note: "Cumulative success count" },
      { name: "failed_runs", type: "number", required: false, note: "Cumulative failure count" },
    ]
  },
  {
    name: "PipelineRun",
    color: "from-amber-500 to-orange-600",
    description: "A single execution instance of a Pipeline, capturing metrics and outcomes.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "pipeline_id", type: "string", required: true, note: "→ Pipeline.id" },
      { name: "run_number", type: "number", required: false, note: "Sequential run counter per pipeline" },
      { name: "status", type: "string", required: true, note: 'running | completed | failed | cancelled | retrying' },
      { name: "started_at", type: "date-time", required: false, note: "When execution began" },
      { name: "completed_at", type: "date-time", required: false, note: "When execution finished" },
      { name: "duration_seconds", type: "number", required: false, note: "Total execution time" },
      { name: "rows_processed", type: "number", required: false, note: "Total rows transferred" },
      { name: "bytes_transferred", type: "number", required: false, note: "Total bytes moved" },
      { name: "objects_completed", type: "array", required: false, note: "List of successfully loaded objects" },
      { name: "objects_failed", type: "array", required: false, note: "List of failed objects" },
      { name: "retry_count", type: "number", required: false, note: "Number of retries attempted" },
      { name: "error_message", type: "string", required: false, note: "Error description if failed" },
      { name: "triggered_by", type: "string", required: false, note: 'manual | schedule | retry' },
    ]
  },
  {
    name: "ActivityLog",
    color: "from-violet-500 to-purple-600",
    description: "Audit trail for all system events across connections, jobs, and authentication.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "log_type", type: "string", required: true, note: 'info | warning | error | success' },
      { name: "category", type: "string", required: true, note: 'connection | job | system | authentication' },
      { name: "message", type: "string", required: true, note: "Human-readable description" },
      { name: "job_id", type: "string", required: false, note: "→ Pipeline.id (if pipeline-related)" },
      { name: "run_id", type: "string", required: false, note: "→ PipelineRun.id (if run-related)" },
      { name: "connection_id", type: "string", required: false, note: "→ Connection.id (if conn-related)" },
      { name: "object_name", type: "string", required: false, note: "Specific object involved" },
      { name: "details", type: "object", required: false, note: "Arbitrary JSON metadata" },
      { name: "stack_trace", type: "string", required: false, note: "Error stack trace if applicable" },
    ]
  },
  {
    name: "PipelineVersion",
    color: "from-rose-500 to-pink-600",
    description: "Version snapshot of a Pipeline for audit and rollback purposes.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "pipeline_id", type: "string", required: true, note: "→ Pipeline.id" },
      { name: "version_number", type: "number", required: true, note: "Monotonically increasing" },
      { name: "label", type: "string", required: false, note: 'e.g. "v3", "Restored from v1"' },
      { name: "commit_message", type: "string", required: false, note: "User-provided change description" },
      { name: "snapshot", type: "object", required: true, note: "Full copy of Pipeline fields at that point in time" },
      { name: "changed_by", type: "string", required: false, note: "User email who made the change" },
      { name: "change_type", type: "string", required: false, note: 'created | updated | paused | resumed | deleted' },
    ]
  },
  {
    name: "ConnectionPrerequisite",
    color: "from-teal-500 to-cyan-600",
    description: "Tracks infra/ops tasks (firewall rules, VPN tunnels, DBA access) required before a connection can be used.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "connection_id", type: "string", required: true, note: "→ Connection.id" },
      { name: "prereq_type", type: "string", required: true, note: 'nsg_rule | egress_rule | nas_path | dba_access | firewall_rule | vpn_tunnel | vault_credentials | app_id | entitlement | ...' },
      { name: "title", type: "string", required: true, note: "Short task title" },
      { name: "description", type: "string", required: false, note: "Detailed notes / ticket instructions" },
      { name: "assigned_team", type: "string", required: true, note: 'networking | dba | security | storage | platform | other' },
      { name: "status", type: "string", required: true, note: 'pending | in_progress | completed | rejected | not_required' },
      { name: "priority", type: "string", required: false, note: 'high | medium | low' },
      { name: "requested_by", type: "string", required: false, note: "Requester name" },
      { name: "ticket_reference", type: "string", required: false, note: "JIRA / ServiceNow ticket ID" },
      { name: "due_date", type: "date-time", required: false, note: "Target completion date" },
      { name: "completed_at", type: "date-time", required: false, note: "Actual completion date" },
      { name: "notes", type: "string", required: false, note: "Resolution notes" },
    ]
  },
  {
    name: "AuditLog",
    color: "from-slate-500 to-gray-600",
    description: "Records entity-level changes (create, update, delete) for compliance and traceability.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "entity_type", type: "string", required: true, note: "Entity table name (e.g. connection, pipeline)" },
      { name: "entity_id", type: "string", required: false, note: "UUID of the affected entity" },
      { name: "action", type: "string", required: true, note: 'create | update | delete | login | logout | export | import' },
      { name: "changed_by", type: "string", required: true, note: "User who made the change" },
      { name: "changes", type: "object", required: false, note: "JSON diff of old/new values" },
      { name: "ip_address", type: "string", required: false, note: "Client IP address" },
    ]
  },
  {
    name: "IngestionJob",
    color: "from-orange-500 to-red-600",
    description: "Tracks individual ingestion jobs tied to a pipeline, including type, status, and error details.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "pipeline_id", type: "string", required: true, note: "→ Pipeline.id" },
      { name: "job_type", type: "string", required: true, note: 'full_load | incremental | cdc | snapshot' },
      { name: "status", type: "string", required: true, note: 'pending | running | completed | failed | cancelled' },
      { name: "config", type: "object", required: false, note: "Job-specific configuration (batch size, parallelism, etc.)" },
      { name: "last_run", type: "date-time", required: false, note: "Timestamp of last execution" },
      { name: "error_message", type: "string", required: false, note: "Error details if failed" },
    ]
  },
  {
    name: "AirflowDag",
    color: "from-sky-500 to-blue-600",
    description: "Stores generated Airflow DAG definitions linked to pipelines, with versioning and deployment tracking.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "pipeline_id", type: "string", required: true, note: "→ Pipeline.id" },
      { name: "dag_id", type: "string", required: true, note: "Airflow DAG identifier" },
      { name: "dag_yaml", type: "string", required: false, note: "Generated DAG YAML content" },
      { name: "dag_factory_config", type: "object", required: false, note: "DAG Factory configuration object" },
      { name: "deployed_at", type: "date-time", required: false, note: "When DAG was last deployed" },
      { name: "deployed_by", type: "string", required: false, note: "User who deployed the DAG" },
      { name: "version", type: "number", required: false, note: "DAG version number" },
    ]
  },
  {
    name: "CustomFunction",
    color: "from-fuchsia-500 to-purple-600",
    description: "User-defined transformation, validation, or enrichment functions for use in column mappings.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "name", type: "string", required: true, note: "Function display name" },
      { name: "description", type: "string", required: false, note: "What the function does" },
      { name: "function_type", type: "string", required: true, note: 'transform | validate | enrich | filter | aggregate' },
      { name: "code", type: "string", required: true, note: "Function source code" },
      { name: "language", type: "string", required: false, note: "Programming language (default: python)" },
      { name: "parameters", type: "array", required: false, note: "Parameter definitions [{name, type, default, required}]" },
      { name: "created_by", type: "string", required: true, note: "User who created the function" },
    ]
  },
  {
    name: "ConnectionProfile",
    color: "from-lime-500 to-green-600",
    description: "Reusable connection templates for quickly creating new connections with predefined settings.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "created_by", type: "string", required: true, note: "User email (auto)" },
      { name: "name", type: "string", required: true, note: "Profile display name" },
      { name: "description", type: "string", required: false, note: "Profile description" },
      { name: "platform", type: "string", required: true, note: 'sql_server, oracle, postgresql, mysql, ...' },
      { name: "host", type: "string", required: false, note: "Default hostname" },
      { name: "port", type: "number", required: false, note: "Default TCP port" },
      { name: "database", type: "string", required: false, note: "Default database name" },
      { name: "username", type: "string", required: false, note: "Default username" },
      { name: "auth_method", type: "string", required: false, note: 'password | key | connection_string | managed_identity | vault_credentials | none' },
      { name: "region", type: "string", required: false, note: "Default cloud region" },
      { name: "bucket_container", type: "string", required: false, note: "Default S3 bucket or ADLS container" },
      { name: "file_config", type: "object", required: false, note: "Default file configuration" },
      { name: "notes", type: "string", required: false, note: "Template notes" },
    ]
  },
  {
    name: "DataCatalogEntry",
    color: "from-indigo-500 to-violet-600",
    description: "Cataloged data assets with schema metadata, profiling stats, and ownership information.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "name", type: "string", required: true, note: "Catalog entry display name" },
      { name: "description", type: "string", required: false, note: "Description of the data asset" },
      { name: "source_connection_id", type: "string", required: false, note: "→ Connection.id" },
      { name: "schema_name", type: "string", required: false, note: "Database schema name" },
      { name: "table_name", type: "string", required: false, note: "Table or object name" },
      { name: "columns", type: "array", required: false, note: "Column definitions [{name, type, nullable, description}]" },
      { name: "row_count", type: "number", required: false, note: "Estimated or profiled row count" },
      { name: "last_profiled", type: "date-time", required: false, note: "When data was last profiled" },
      { name: "tags", type: "array", required: false, note: "Classification tags" },
      { name: "owner", type: "string", required: false, note: "Data owner / steward" },
    ]
  },
  {
    name: "DagTemplate",
    color: "from-yellow-500 to-amber-600",
    description: "Reusable DAG generation templates for Airflow, supporting multiple template engines.",
    fields: [
      { name: "id", type: "string", required: true, note: "Auto-generated UUID" },
      { name: "created_date", type: "date-time", required: true, note: "Auto-generated" },
      { name: "updated_date", type: "date-time", required: true, note: "Auto-updated via trigger" },
      { name: "name", type: "string", required: true, note: "Template display name" },
      { name: "description", type: "string", required: false, note: "What the template generates" },
      { name: "template_type", type: "string", required: true, note: 'dag_factory | custom | jinja | python' },
      { name: "template_content", type: "string", required: true, note: "Template source code / content" },
      { name: "parameters", type: "array", required: false, note: "Configurable parameters [{name, type, default}]" },
      { name: "created_by", type: "string", required: true, note: "User who created the template" },
      { name: "version", type: "number", required: false, note: "Template version number" },
    ]
  },
];

const relationships = [
  { from: "Pipeline", to: "Connection", label: "source_connection_id", type: "many-to-one" },
  { from: "Pipeline", to: "Connection", label: "target_connection_id", type: "many-to-one" },
  { from: "PipelineRun", to: "Pipeline", label: "pipeline_id", type: "many-to-one" },
  { from: "ActivityLog", to: "Pipeline", label: "job_id (optional)", type: "many-to-one" },
  { from: "ActivityLog", to: "PipelineRun", label: "run_id (optional)", type: "many-to-one" },
  { from: "ActivityLog", to: "Connection", label: "connection_id (optional)", type: "many-to-one" },
  { from: "PipelineVersion", to: "Pipeline", label: "pipeline_id", type: "many-to-one" },
  { from: "ConnectionPrerequisite", to: "Connection", label: "connection_id", type: "many-to-one" },
  { from: "IngestionJob", to: "Pipeline", label: "pipeline_id", type: "many-to-one" },
  { from: "AirflowDag", to: "Pipeline", label: "pipeline_id", type: "many-to-one" },
  { from: "DataCatalogEntry", to: "Connection", label: "source_connection_id (optional)", type: "many-to-one" },
];

function EntityCard({ entity }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-2xl overflow-hidden shadow-sm hover:shadow-lg dark:hover:shadow-xl dark:hover:shadow-slate-900/30 transition-all">
      {/* Header */}
      <div className={`bg-gradient-to-r ${entity.color} p-4`}>
        <div className="flex items-center gap-2 mb-1">
          <Database className="w-4 h-4 text-white/80" />
          <span className="text-white font-bold text-base">{entity.name}</span>
        </div>
        <p className="text-white/70 text-xs leading-relaxed">{entity.description}</p>
      </div>

      {/* Fields */}
      <div>
        <button
            className="w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700/70 transition-colors"
            onClick={() => setExpanded(!expanded)}
          >
          <span>{entity.fields.length} fields</span>
          {expanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
        </button>

        {expanded && (
          <div className="border-t border-slate-100 dark:border-slate-700">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-slate-50 dark:bg-slate-700/50">
                  <th className="text-left px-4 py-2 text-slate-500 dark:text-slate-400 font-medium">Field</th>
                  <th className="text-left px-4 py-2 text-slate-500 dark:text-slate-400 font-medium">Type</th>
                  <th className="text-left px-4 py-2 text-slate-500 dark:text-slate-400 font-medium hidden sm:table-cell">Notes</th>
                  <th className="text-center px-4 py-2 text-slate-500 dark:text-slate-400 font-medium">Req</th>
                </tr>
              </thead>
              <tbody>
                {entity.fields.map((field, i) => (
                  <tr key={i} className={cn(
                    "border-t border-slate-50 dark:border-slate-700/50",
                    i % 2 === 0 ? "bg-white dark:bg-slate-800" : "bg-slate-50/50 dark:bg-slate-700/20"
                  )}>
                    <td className="px-4 py-2">
                      <div className="flex items-center gap-1.5">
                        {field.name === "id" && <Key className="w-3 h-3 text-amber-500 shrink-0" />}
                        {field.name.endsWith("_id") && field.name !== "id" && <Link2 className="w-3 h-3 text-blue-400 shrink-0" />}
                        <span className={cn("font-mono", field.required && field.name !== "id" && field.name !== "created_date" && field.name !== "created_by" ? "text-slate-900 dark:text-slate-100 font-semibold" : "text-slate-500 dark:text-slate-400")}>
                          {field.name}
                        </span>
                      </div>
                    </td>
                    <td className="px-4 py-2">
                      <span className={cn("inline-flex items-center gap-1 px-1.5 py-0.5 rounded border text-xs font-medium", typeColor(field.type))}>
                        {typeIcon(field.type)}
                        {field.type}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-slate-500 dark:text-slate-400 hidden sm:table-cell max-w-xs truncate">{field.note}</td>
                    <td className="px-4 py-2 text-center">
                      {field.required ? (
                        <span className="text-emerald-600 font-bold">✓</span>
                      ) : (
                        <span className="text-slate-300 dark:text-slate-600">–</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

function DDLModal({ open, onClose }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(DDL);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <div className="flex items-center justify-between">
            <DialogTitle className="flex items-center gap-2">
              <Code2 className="w-4 h-4 text-blue-600" />
              PostgreSQL DDL — DataFlow Platform
            </DialogTitle>
            <Button size="sm" variant="outline" onClick={handleCopy} className="gap-1.5 text-xs mr-6">
              {copied ? <Check className="w-3.5 h-3.5 text-emerald-600" /> : <Copy className="w-3.5 h-3.5" />}
              {copied ? "Copied!" : "Copy"}
            </Button>
          </div>
        </DialogHeader>
        <pre className="overflow-auto flex-1 text-xs font-mono bg-slate-950 text-slate-200 rounded-xl p-5 leading-relaxed">
          {DDL}
        </pre>
      </DialogContent>
    </Dialog>
  );
}

export default function DataModel() {
  const [showRel, setShowRel] = useState(true);
  const [showDDL, setShowDDL] = useState(false);

  return (
    <div className="space-y-8 max-w-7xl mx-auto">
      <DDLModal open={showDDL} onClose={() => setShowDDL(false)} />

      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground tracking-tight flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-teal-500 to-cyan-600 flex items-center justify-center">
              <Database className="w-5 h-5 text-white" />
            </div>
            Data Model
          </h1>
          <p className="text-slate-500 dark:text-slate-400 mt-1">Entity schemas and relationships for the DataFlow platform</p>
        </div>
        <Button onClick={() => setShowDDL(true)} variant="outline" className="gap-2 hover:bg-teal-50 dark:hover:bg-slate-700 hover:border-teal-300 dark:hover:border-teal-600">
          <Code2 className="w-4 h-4" />
          Generate DDL
        </Button>
      </div>

      {/* Relationships */}
      <div className="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-2xl overflow-hidden hover:shadow-lg dark:hover:shadow-xl dark:hover:shadow-slate-900/50 transition-all">
        <button
          className="w-full flex items-center justify-between px-6 py-4 hover:bg-teal-50/50 dark:hover:bg-slate-700/70 transition-colors"
          onClick={() => setShowRel(!showRel)}
        >
          <div className="flex items-center gap-2">
            <Link2 className="w-5 h-5 text-teal-600 dark:text-teal-400" />
            <span className="font-semibold text-slate-900 dark:text-white">Entity Relationships</span>
            <span className="text-xs bg-teal-100 dark:bg-teal-900/30 text-teal-700 dark:text-teal-300 px-2 py-0.5 rounded-full">{relationships.length}</span>
          </div>
          {showRel ? <ChevronDown className="w-4 h-4 text-slate-400" /> : <ChevronRight className="w-4 h-4 text-slate-400" />}
        </button>

        {showRel && (
          <div className="border-t border-slate-100 dark:border-slate-700 p-6">
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
              {relationships.map((rel, i) => (
                <div key={i} className="flex items-center gap-2 bg-slate-50 dark:bg-slate-700/50 rounded-xl px-3 py-2.5 text-sm">
                  <span className="font-semibold text-slate-800 dark:text-slate-200 text-xs">{rel.from}</span>
                  <div className="flex-1 flex items-center gap-1 min-w-0">
                    <div className="h-px flex-1 bg-blue-300 dark:bg-blue-600" />
                    <span className="text-blue-500 text-xs shrink-0">→</span>
                  </div>
                  <span className="font-semibold text-slate-800 dark:text-slate-200 text-xs">{rel.to}</span>
                </div>
              ))}
            </div>
            <div className="mt-4 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-2">
              {relationships.map((rel, i) => (
                <div key={i} className="text-xs text-slate-500 dark:text-slate-400 flex items-center gap-1">
                  <span className="font-mono text-slate-400 dark:text-slate-500">{rel.from}.{rel.label}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Entity Cards */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        {entities.map((entity) => (
          <EntityCard key={entity.name} entity={entity} />
        ))}
      </div>

      {/* Legend */}
      <div className="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-2xl p-5 hover:shadow-lg dark:hover:shadow-xl dark:hover:shadow-slate-900/30 transition-all">
        <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-3 flex items-center gap-2">
          <div className="w-1 h-1 rounded-full bg-teal-500"></div>
          Legend
        </h3>
        <div className="flex flex-wrap gap-4 text-xs">
          <div className="flex items-center gap-1.5"><Key className="w-3.5 h-3.5 text-amber-500" /> <span className="text-slate-600 dark:text-slate-400">Primary key</span></div>
          <div className="flex items-center gap-1.5"><Link2 className="w-3.5 h-3.5 text-blue-400" /> <span className="text-slate-600 dark:text-slate-400">Foreign key reference</span></div>
          <div className="flex items-center gap-1.5"><span className="font-mono font-semibold text-slate-900 dark:text-slate-100 text-xs">field</span> <span className="text-slate-600 dark:text-slate-400">= required</span></div>
          <div className="flex items-center gap-1.5"><span className="font-mono text-slate-400 dark:text-slate-500 text-xs">field</span> <span className="text-slate-600 dark:text-slate-400">= optional</span></div>
          {[["string","emerald"], ["number","blue"], ["boolean","violet"], ["array","amber"], ["object","rose"], ["date-time","cyan"]].map(([t,c]) => (
            <div key={t} className={`flex items-center gap-1 px-2 py-0.5 rounded border text-xs font-medium text-${c}-600 bg-${c}-50 border-${c}-200`}>
              {typeIcon(t)} {t}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
