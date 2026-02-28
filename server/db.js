import pg from 'pg';
import config from '../config/index.js';

const { Pool } = pg;

const pool = new Pool({
  connectionString: config.database.connectionString,
  ssl: config.database.ssl,
  min: config.database.poolMin,
  max: config.database.poolMax,
  idleTimeoutMillis: config.database.idleTimeoutMs,
  connectionTimeoutMillis: config.database.connectionTimeoutMs,
});

const ENTITY_TABLES = [
  'pipeline',
  'connection',
  'pipeline_run',
  'activity_log',
  'audit_log',
  'ingestion_job',
  'airflow_dag',
  'custom_function',
  'connection_profile',
  'connection_prerequisite',
  'pipeline_version',
  'data_catalog_entry',
  'dag_template'
];

function entityNameToTable(entityName) {
  const table = entityName
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    .replace(/([a-z\d])([A-Z])/g, '$1_$2')
    .toLowerCase();

  if (!ENTITY_TABLES.includes(table)) {
    throw new Error(`Unknown entity: ${entityName}`);
  }
  return table;
}

function sanitizeFieldName(field) {
  const sanitized = field.replace(/[^a-zA-Z0-9_]/g, '');
  if (!sanitized) throw new Error(`Invalid field name: ${field}`);
  return sanitized;
}

async function initializeDatabase() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS "workspace" (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        slug TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        settings JSONB NOT NULL DEFAULT '{}',
        created_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        created_by TEXT DEFAULT 'system'
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS "idx_workspace_slug" ON "workspace" (slug)`);

    for (const table of ENTITY_TABLES) {
      await client.query(`
        CREATE TABLE IF NOT EXISTS "${table}" (
          id SERIAL PRIMARY KEY,
          data JSONB NOT NULL DEFAULT '{}',
          workspace_id INTEGER REFERENCES workspace(id) ON DELETE CASCADE,
          created_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          updated_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          created_by TEXT DEFAULT 'system'
        )
      `);

      const colCheck = await client.query(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = $1 AND column_name = 'workspace_id'
      `, [table]);
      if (colCheck.rows.length === 0) {
        await client.query(`ALTER TABLE "${table}" ADD COLUMN workspace_id INTEGER REFERENCES workspace(id) ON DELETE CASCADE`);
        console.log(`[db] Added workspace_id column to ${table}`);
      }

      await client.query(`
        CREATE INDEX IF NOT EXISTS "idx_${table}_created_date" ON "${table}" (created_date DESC)
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS "idx_${table}_data_gin" ON "${table}" USING GIN (data)
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS "idx_${table}_updated_date" ON "${table}" (updated_date DESC)
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS "idx_${table}_workspace_id" ON "${table}" (workspace_id)
      `);
    }

    const highQueryTables = ['pipeline', 'connection', 'pipeline_run', 'ingestion_job'];
    for (const table of highQueryTables) {
      await client.query(`
        CREATE INDEX IF NOT EXISTS "idx_${table}_status" ON "${table}" ((data->>'status'))
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS "idx_${table}_name" ON "${table}" ((data->>'name'))
      `);
    }

    await client.query(`CREATE INDEX IF NOT EXISTS "idx_activity_log_category" ON "activity_log" ((data->>'category'))`);
    await client.query(`CREATE INDEX IF NOT EXISTS "idx_activity_log_log_type" ON "activity_log" ((data->>'log_type'))`);
    await client.query(`CREATE INDEX IF NOT EXISTS "idx_activity_log_job_id" ON "activity_log" ((data->>'job_id'))`);
    await client.query(`CREATE INDEX IF NOT EXISTS "idx_activity_log_connection_id" ON "activity_log" ((data->>'connection_id'))`);

    await client.query(`CREATE INDEX IF NOT EXISTS "idx_pipeline_run_pipeline_id" ON "pipeline_run" ((data->>'pipeline_id'))`);
    await client.query(`CREATE INDEX IF NOT EXISTS "idx_pipeline_run_triggered_by" ON "pipeline_run" ((data->>'triggered_by'))`);

    await client.query(`CREATE INDEX IF NOT EXISTS "idx_connection_prerequisite_connection_id" ON "connection_prerequisite" ((data->>'connection_id'))`);
    await client.query(`CREATE INDEX IF NOT EXISTS "idx_connection_prerequisite_prereq_type" ON "connection_prerequisite" ((data->>'prereq_type'))`);
    await client.query(`CREATE INDEX IF NOT EXISTS "idx_connection_prerequisite_status" ON "connection_prerequisite" ((data->>'status'))`);

    await client.query(`CREATE INDEX IF NOT EXISTS "idx_ingestion_job_pipeline_id" ON "ingestion_job" ((data->>'pipeline_id'))`);

    await client.query(`CREATE INDEX IF NOT EXISTS "idx_pipeline_version_pipeline_id" ON "pipeline_version" ((data->>'pipeline_id'))`);

    await client.query(`CREATE INDEX IF NOT EXISTS "idx_connection_platform" ON "connection" ((data->>'platform'))`);
    await client.query(`CREATE INDEX IF NOT EXISTS "idx_connection_name" ON "connection" ((data->>'name'))`);

    await client.query(`
      CREATE INDEX IF NOT EXISTS "idx_activity_log_search" ON "activity_log" USING GIN (to_tsvector('english', coalesce(data->>'message','') || ' ' || coalesce(data->>'category','')))
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS "idx_pipeline_search" ON "pipeline" USING GIN (to_tsvector('english', coalesce(data->>'name','') || ' ' || coalesce(data->>'description','')))
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS "idx_connection_search" ON "connection" USING GIN (to_tsvector('english', coalesce(data->>'name','') || ' ' || coalesce(data->>'description','') || ' ' || coalesce(data->>'platform','')))
    `);

    try {
      await client.query(`CREATE EXTENSION IF NOT EXISTS pg_trgm`);
      await client.query(`CREATE INDEX IF NOT EXISTS "idx_pipeline_name_trgm" ON "pipeline" USING GIN ((data->>'name') gin_trgm_ops)`);
      await client.query(`CREATE INDEX IF NOT EXISTS "idx_connection_name_trgm" ON "connection" USING GIN ((data->>'name') gin_trgm_ops)`);
      console.log('pg_trgm indexes created');
    } catch (trgmErr) {
      console.warn('pg_trgm extension not available, skipping trigram indexes:', trgmErr.message);
    }

    await ensureDefaultWorkspace(client);

    console.log('Database tables and indexes initialized');
  } finally {
    client.release();
  }
}

async function ensureDefaultWorkspace(client) {
  const existing = await client.query(`SELECT id FROM workspace WHERE slug = 'default' LIMIT 1`);
  let defaultWorkspaceId;

  if (existing.rows.length === 0) {
    const result = await client.query(
      `INSERT INTO workspace (name, slug, description, settings) VALUES ($1, $2, $3, $4) RETURNING id`,
      ['Default Workspace', 'default', 'Default workspace for all users', JSON.stringify({ is_default: true })]
    );
    defaultWorkspaceId = result.rows[0].id;
    console.log(`[db] Created default workspace (id: ${defaultWorkspaceId})`);
  } else {
    defaultWorkspaceId = existing.rows[0].id;
  }

  for (const table of ENTITY_TABLES) {
    const unscoped = await client.query(
      `UPDATE "${table}" SET workspace_id = $1 WHERE workspace_id IS NULL RETURNING id`,
      [defaultWorkspaceId]
    );
    if (unscoped.rowCount > 0) {
      console.log(`[db] Migrated ${unscoped.rowCount} rows in ${table} to default workspace`);
    }
  }

  return defaultWorkspaceId;
}

async function getDefaultWorkspaceId() {
  const result = await pool.query(`SELECT id FROM workspace WHERE slug = 'default' LIMIT 1`);
  return result.rows.length > 0 ? result.rows[0].id : null;
}

export { pool, initializeDatabase, entityNameToTable, sanitizeFieldName, ENTITY_TABLES, getDefaultWorkspaceId };
export { default as config } from '../config/index.js';
