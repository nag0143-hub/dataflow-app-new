import express from 'express';
import cookieParser from 'cookie-parser';
import { initializeDatabase, pool, entityNameToTable, sanitizeFieldName, config, ENTITY_TABLES } from './db.js';
import { testConnection } from './test-connection.js';
import airflowRouter from './airflow-proxy.js';
import ldapAuthRouter, { resolveSessionFull } from './ldap-auth.js';
import { gitlabCommitFiles, gitlabCheckStatus, getGitLabConfig } from './gitlab.js';
import { connectRedis, healthCheck as redisHealthCheck } from './redis.js';
import logger, { createHttpLogger } from './logger.js';
import {
  validateBody, validateParams,
  entityParamsSchema, entityIdParamsSchema,
  filterBodySchema, batchBodySchema,
  introspectSchemaBody, gitlabStatusSchema, gitlabCommitSchema,
  testConnectionSchema, purgLogsSchema,
} from './validation.js';
import { requireRole, entityWriteGuard } from './rbac.js';

function formatRecord(row, tableName) {
  const record = {
    id: String(row.id),
    ...row.data,
    workspace_id: row.workspace_id,
    created_date: row.created_date,
    updated_date: row.updated_date,
    created_by: row.created_by
  };
  if (tableName === 'connection' && record.vault_config) {
    record.vault_config = {
      ...record.vault_config,
      vault_role_id: record.vault_config.vault_role_id ? '••••••••' : '',
      vault_secret_id: record.vault_config.vault_secret_id ? '••••••••' : '',
    };
  }
  if (tableName === 'connection') {
    if (record.password) record.password = '••••••••';
    if (record.connection_string) record.connection_string = '••••••••';
  }
  return record;
}

function buildFilterClause(query, params) {
  if (!query || Object.keys(query).length === 0) return { where: '', params };

  const conditions = [];
  let paramIndex = params.length;

  function processFilter(filter) {
    for (const [key, value] of Object.entries(filter)) {
      if (key === '$or' && Array.isArray(value)) {
        const orConditions = [];
        for (const subFilter of value) {
          const subConditions = [];
          for (const [subKey, subValue] of Object.entries(subFilter)) {
            if (subValue && typeof subValue === 'object' && '$regex' in subValue) {
              paramIndex++;
              subConditions.push(`data->>'${sanitizeFieldName(subKey)}' ILIKE '%' || $${paramIndex} || '%'`);
              params.push(subValue.$regex);
            } else {
              paramIndex++;
              subConditions.push(`data->>'${sanitizeFieldName(subKey)}' = $${paramIndex}`);
              params.push(String(subValue));
            }
          }
          if (subConditions.length > 0) {
            orConditions.push(`(${subConditions.join(' AND ')})`);
          }
        }
        if (orConditions.length > 0) {
          conditions.push(`(${orConditions.join(' OR ')})`);
        }
      } else if (value && typeof value === 'object' && '$regex' in value) {
        paramIndex++;
        conditions.push(`data->>'${sanitizeFieldName(key)}' ILIKE '%' || $${paramIndex} || '%'`);
        params.push(value.$regex);
      } else if (value && typeof value === 'object' && '$in' in value) {
        paramIndex++;
        conditions.push(`data->>'${sanitizeFieldName(key)}' = ANY($${paramIndex})`);
        params.push(value.$in.map(String));
      } else if (value && typeof value === 'object' && '$ne' in value) {
        paramIndex++;
        conditions.push(`(data->>'${sanitizeFieldName(key)}' IS NULL OR data->>'${sanitizeFieldName(key)}' != $${paramIndex})`);
        params.push(String(value.$ne));
      } else if (value && typeof value === 'object' && '$exists' in value) {
        if (value.$exists) {
          conditions.push(`data ? '${sanitizeFieldName(key)}'`);
        } else {
          conditions.push(`NOT (data ? '${sanitizeFieldName(key)}')`);
        }
      } else {
        paramIndex++;
        conditions.push(`data->>'${sanitizeFieldName(key)}' = $${paramIndex}`);
        params.push(String(value));
      }
    }
  }

  processFilter(query);

  return {
    where: conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '',
    params
  };
}

function buildSortClause(sort) {
  if (!sort) return 'ORDER BY created_date DESC';
  const desc = sort.startsWith('-');
  const field = desc ? sort.substring(1) : sort;
  if (field === 'created_date' || field === 'updated_date') {
    return `ORDER BY ${field} ${desc ? 'DESC' : 'ASC'}`;
  }
  return `ORDER BY data->>'${sanitizeFieldName(field)}' ${desc ? 'DESC' : 'ASC'} NULLS LAST`;
}

function workspaceFilter(params, workspaceId) {
  if (!workspaceId) return { clause: '', params };
  params.push(workspaceId);
  return { clause: `workspace_id = $${params.length}`, params };
}

function injectWorkspaceWhere(existingWhere, wsClause) {
  if (!wsClause) return existingWhere;
  if (!existingWhere) return `WHERE ${wsClause}`;
  return `${existingWhere} AND ${wsClause}`;
}

async function searchEntities(table, searchTerm, filters, limit = 50, workspaceId = null) {
  const params = [];
  const conditions = [];
  let paramIndex = 0;

  if (workspaceId) {
    paramIndex++;
    conditions.push(`workspace_id = $${paramIndex}`);
    params.push(workspaceId);
  }

  if (filters) {
    for (const [key, value] of Object.entries(filters)) {
      if (value !== undefined && value !== null && value !== '') {
        paramIndex++;
        conditions.push(`data->>'${sanitizeFieldName(key)}' = $${paramIndex}`);
        params.push(String(value));
      }
    }
  }

  if (searchTerm) {
    paramIndex++;
    let ftsExpr;
    if (table === 'activity_log') {
      ftsExpr = `to_tsvector('english', coalesce(data->>'message','') || ' ' || coalesce(data->>'category',''))`;
    } else if (table === 'pipeline') {
      ftsExpr = `to_tsvector('english', coalesce(data->>'name','') || ' ' || coalesce(data->>'description',''))`;
    } else if (table === 'connection') {
      ftsExpr = `to_tsvector('english', coalesce(data->>'name','') || ' ' || coalesce(data->>'description','') || ' ' || coalesce(data->>'platform',''))`;
    } else {
      ftsExpr = `to_tsvector('english', data::text)`;
    }
    conditions.push(`${ftsExpr} @@ plainto_tsquery('english', $${paramIndex})`);
    params.push(searchTerm);
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  paramIndex++;
  params.push(parseInt(limit));

  const result = await pool.query(
    `SELECT * FROM "${table}" ${where} ORDER BY created_date DESC LIMIT $${paramIndex}`,
    params
  );

  return result.rows.map(r => formatRecord(r, table));
}

function clampPageSize(value, fallback) {
  const num = parseInt(value) || fallback || config.api.defaultPageSize;
  return Math.min(Math.max(1, num), config.api.maxPageSize);
}

function requireWorkspace(handler) {
  return async (req, res, next) => {
    try {
      const session = await resolveSessionFull(req);
      if (!session?.workspace?.id) {
        return res.status(401).json({ error: 'Authentication required' });
      }
      req.workspaceId = session.workspace.id;
      req.sessionUser = session.user || null;
      return handler(req, res, next);
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  };
}

export function createApiMiddleware() {
  const app = express();
  app.use(cookieParser());
  app.use(express.json({ limit: config.api.bodyLimit }));

  if (config.logging.requests) {
    app.use(createHttpLogger());
  }

  let dbReady = false;
  const startTime = Date.now();

  (async () => {
    try {
      await connectRedis();
      logger.info('redis connected');
    } catch (err) {
      logger.warn({ err: err.message }, 'redis not available, using in-memory fallback');
    }
  })();

  initializeDatabase().then(() => {
    dbReady = true;
    logger.info('database ready');
  }).catch(err => {
    logger.error({ err: err.message }, 'database init failed');
  });

  app.get('/api/health', async (req, res) => {
    try {
      const dbCheck = await pool.query('SELECT 1');
      const redisStatus = await redisHealthCheck();
      res.json({
        status: 'ok',
        uptime: Math.floor((Date.now() - startTime) / 1000),
        database: dbCheck ? 'connected' : 'error',
        redis: redisStatus,
        dbReady,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      res.status(503).json({
        status: 'degraded',
        uptime: Math.floor((Date.now() - startTime) / 1000),
        database: 'disconnected',
        dbReady,
        error: err.message,
        timestamp: new Date().toISOString(),
      });
    }
  });

  app.use('/api/auth', ldapAuthRouter);

  app.get('/api/apps/public/prod/public-settings/by-id/:appId', (req, res) => {
    res.json({
      appId: req.params.appId,
      name: 'DataFlow',
      requiresAuth: false,
      status: 'active'
    });
  });

  app.get('/api/entities/:entityName', requireWorkspace(async (req, res) => {
    try {
      const table = entityNameToTable(req.params.entityName);
      const { sort, limit, skip = '0', cursor } = req.query;
      const pageSize = clampPageSize(limit);
      const workspaceId = req.workspaceId;

      if (cursor) {
        const cursorId = parseInt(cursor);
        if (isNaN(cursorId)) return res.status(400).json({ error: 'Invalid cursor' });
        const params = [cursorId, pageSize, workspaceId];
        const result = await pool.query(
          `SELECT * FROM "${table}" WHERE id < $1 AND workspace_id = $3 ORDER BY id DESC LIMIT $2`,
          params
        );
        const items = result.rows.map(r => formatRecord(r, table));
        const lastItem = items[items.length - 1];
        res.json({
          items,
          nextCursor: lastItem ? lastItem.id : null,
          hasMore: items.length === pageSize
        });
      } else {
        const sortClause = buildSortClause(sort);
        const params = [pageSize, parseInt(skip) || 0, workspaceId];
        const result = await pool.query(
          `SELECT * FROM "${table}" WHERE workspace_id = $3 ${sortClause} LIMIT $1 OFFSET $2`,
          params
        );
        res.json(result.rows.map(r => formatRecord(r, table)));
      }
    } catch (err) {
      if (err.code === '42P01') return res.json([]);
      logger.error({ err: err.message, entity: req.params.entityName }, 'list error');
      res.status(500).json({ error: err.message });
    }
  }));

  app.post('/api/entities/:entityName/filter', requireWorkspace(async (req, res) => {
    try {
      const table = entityNameToTable(req.params.entityName);
      const { query: filterQuery, sort, limit, skip = 0 } = req.body;
      const workspaceId = req.workspaceId;
      const params = [];
      const { where, params: filterParams } = buildFilterClause(filterQuery, params);
      filterParams.push(workspaceId);
      const wsClause = `workspace_id = $${filterParams.length}`;
      const finalWhere = injectWorkspaceWhere(where, wsClause);
      const sortClause = buildSortClause(sort);
      const paramIdx = filterParams.length;
      filterParams.push(clampPageSize(limit), parseInt(skip) || 0);
      const result = await pool.query(
        `SELECT * FROM "${table}" ${finalWhere} ${sortClause} LIMIT $${paramIdx + 1} OFFSET $${paramIdx + 2}`,
        filterParams
      );
      res.json(result.rows.map(r => formatRecord(r, table)));
    } catch (err) {
      if (err.code === '42P01') return res.json([]);
      logger.error({ err: err.message, entity: req.params.entityName }, 'filter error');
      res.status(500).json({ error: err.message });
    }
  }));

  app.post('/api/entities/:entityName/batch', requireWorkspace(async (req, res) => {
    try {
      if (req.sessionUser && req.sessionUser.role === 'viewer') {
        return res.status(403).json({ error: 'Forbidden — viewers have read-only access', your_role: 'viewer' });
      }
      const table = entityNameToTable(req.params.entityName);
      const { items } = req.body;
      if (!items || !Array.isArray(items) || items.length === 0) {
        return res.status(400).json({ error: 'Request body must contain a non-empty items array' });
      }
      if (items.length > 100) {
        return res.status(400).json({ error: 'Batch size limited to 100 items' });
      }
      const workspaceId = req.workspaceId;
      const sessionEmail = req.sessionUser?.email || 'system';
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const values = [];
        const placeholders = [];
        let paramIdx = 0;
        for (const item of items) {
          const data = { ...item };
          const createdBy = sessionEmail;
          delete data.created_by;
          paramIdx++;
          const dataIdx = paramIdx;
          paramIdx++;
          const byIdx = paramIdx;
          paramIdx++;
          const wsIdx = paramIdx;
          placeholders.push(`($${dataIdx}, $${byIdx}, $${wsIdx})`);
          values.push(JSON.stringify(data), createdBy, workspaceId);
        }
        const result = await client.query(
          `INSERT INTO "${table}" (data, created_by, workspace_id) VALUES ${placeholders.join(', ')} RETURNING *`,
          values
        );
        await client.query('COMMIT');
        res.status(201).json(result.rows.map(r => formatRecord(r, table)));
      } catch (txErr) {
        await client.query('ROLLBACK');
        throw txErr;
      } finally {
        client.release();
      }
    } catch (err) {
      logger.error({ err: err.message, entity: req.params.entityName }, 'batch create error');
      res.status(500).json({ error: err.message });
    }
  }));

  app.get('/api/entities/:entityName/:id', requireWorkspace(async (req, res) => {
    try {
      const table = entityNameToTable(req.params.entityName);
      const workspaceId = req.workspaceId;
      const result = await pool.query(
        `SELECT * FROM "${table}" WHERE id = $1 AND workspace_id = $2`,
        [parseInt(req.params.id), workspaceId]
      );
      if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
      res.json(formatRecord(result.rows[0], table));
    } catch (err) {
      logger.error({ err: err.message, entity: req.params.entityName }, 'get error');
      res.status(500).json({ error: err.message });
    }
  }));

  app.post('/api/entities/:entityName', requireWorkspace(async (req, res) => {
    try {
      if (req.sessionUser && req.sessionUser.role === 'viewer') {
        return res.status(403).json({ error: 'Forbidden — viewers have read-only access', your_role: 'viewer' });
      }
      const table = entityNameToTable(req.params.entityName);
      const data = { ...req.body };
      const createdBy = req.sessionUser?.email || data.created_by || 'system';
      delete data.created_by;
      const workspaceId = req.workspaceId;

      if ((table === 'pipeline' || table === 'connection') && data.name?.trim()) {
        const existing = await pool.query(
          `SELECT id FROM "${table}" WHERE LOWER(data->>'name') = LOWER($1) AND workspace_id = $2 LIMIT 1`,
          [data.name.trim(), workspaceId]
        );
        if (existing.rows.length > 0) {
          return res.status(409).json({ error: `A ${table} with the name "${data.name}" already exists.` });
        }
      }

      const result = await pool.query(
        `INSERT INTO "${table}" (data, created_by, workspace_id) VALUES ($1, $2, $3) RETURNING *`,
        [JSON.stringify(data), createdBy, workspaceId]
      );
      res.status(201).json(formatRecord(result.rows[0], table));
    } catch (err) {
      logger.error({ err: err.message, entity: req.params.entityName }, 'create error');
      res.status(500).json({ error: err.message });
    }
  }));

  app.put('/api/entities/:entityName/:id', requireWorkspace(async (req, res) => {
    try {
      if (req.sessionUser && req.sessionUser.role === 'viewer') {
        return res.status(403).json({ error: 'Forbidden — viewers have read-only access', your_role: 'viewer' });
      }
      const table = entityNameToTable(req.params.entityName);
      const data = { ...req.body };
      delete data.id;
      delete data.created_date;
      delete data.updated_date;
      delete data.created_by;
      delete data.workspace_id;
      const workspaceId = req.workspaceId;

      if (table === 'connection') {
        const REDACTED = '••••••••';
        if (data.password === REDACTED) delete data.password;
        if (data.connection_string === REDACTED) delete data.connection_string;
        if (data.vault_config) {
          if (data.vault_config.vault_role_id === REDACTED) delete data.vault_config.vault_role_id;
          if (data.vault_config.vault_secret_id === REDACTED) delete data.vault_config.vault_secret_id;
        }
      }

      if ((table === 'pipeline' || table === 'connection') && data.name?.trim()) {
        const existing = await pool.query(
          `SELECT id FROM "${table}" WHERE LOWER(data->>'name') = LOWER($1) AND id != $2 AND workspace_id = $3 LIMIT 1`,
          [data.name.trim(), parseInt(req.params.id), workspaceId]
        );
        if (existing.rows.length > 0) {
          return res.status(409).json({ error: `A ${table} with the name "${data.name}" already exists.` });
        }
      }

      const result = await pool.query(
        `UPDATE "${table}" SET data = data || $1, updated_date = NOW() WHERE id = $2 AND workspace_id = $3 RETURNING *`,
        [JSON.stringify(data), parseInt(req.params.id), workspaceId]
      );
      if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
      res.json(formatRecord(result.rows[0], table));
    } catch (err) {
      logger.error({ err: err.message, entity: req.params.entityName, id: req.params.id }, 'update error');
      res.status(500).json({ error: err.message });
    }
  }));

  app.delete('/api/entities/:entityName/:id', requireWorkspace(async (req, res) => {
    try {
      if (req.sessionUser && req.sessionUser.role === 'viewer') {
        return res.status(403).json({ error: 'Forbidden — viewers have read-only access', your_role: 'viewer' });
      }
      const table = entityNameToTable(req.params.entityName);
      const workspaceId = req.workspaceId;
      await pool.query(`DELETE FROM "${table}" WHERE id = $1 AND workspace_id = $2`, [parseInt(req.params.id), workspaceId]);
      res.json({ success: true });
    } catch (err) {
      logger.error({ err: err.message, entity: req.params.entityName, id: req.params.id }, 'delete error');
      res.status(500).json({ error: err.message });
    }
  }));

  app.use('/api/airflow', airflowRouter);

  app.post('/api/test-connection', async (req, res) => {
    try {
      const result = await testConnection(req.body);
      res.json(result);
    } catch (err) {
      res.status(500).json({
        success: false,
        error_code: 'INTERNAL_ERROR',
        error_message: err.message,
        latency_ms: 0,
      });
    }
  });

  app.post('/api/test-vault', async (req, res) => {
    try {
      const { fetchVaultCredentials } = await import('./vault.js');
      const result = await fetchVaultCredentials(req.body);
      res.json(result);
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.post('/api/validate-spec', async (req, res) => {
    try {
      const { validateSpecWithDB } = await import('./spec-validator.js');
      const spec = req.body.spec || req.body;
      const results = await validateSpecWithDB(spec, pool, entityNameToTable);
      res.json({ ...results, checked_at: new Date().toISOString() });
    } catch (err) {
      res.status(500).json({ valid: false, errors: [{ path: "", message: err.message, severity: "error" }], warnings: [], checked_at: new Date().toISOString() });
    }
  });

  app.post('/api/introspect-schema', requireWorkspace(async (req, res) => {
    try {
      const { connectionId } = req.body;
      const parsedId = parseInt(connectionId);
      if (!connectionId || isNaN(parsedId)) {
        return res.status(400).json({ success: false, error: 'A valid numeric connectionId is required' });
      }
      const workspaceId = req.workspaceId;
      const table = entityNameToTable('Connection');
      const result = await pool.query(`SELECT data FROM "${table}" WHERE id = $1 AND workspace_id = $2`, [parsedId, workspaceId]);
      if (result.rows.length === 0) {
        return res.status(404).json({ success: false, error: 'Connection not found' });
      }
      const connData = result.rows[0].data;
      const { introspectSchema } = await import('./introspect-schema.js');
      const schemas = await introspectSchema(connData);
      res.json(schemas);
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  }));

  app.post('/api/functions/:functionName', requireWorkspace(async (req, res) => {
    const { functionName } = req.params;
    const workspaceId = req.workspaceId;
    try {
      switch (functionName) {
        case 'searchPipelines': {
          const { searchTerm, filters, limit } = req.body;
          const items = await searchEntities('pipeline', searchTerm, filters, limit, workspaceId);
          res.json({ items, nextCursor: null, hasMore: false });
          break;
        }
        case 'searchConnections': {
          const { searchTerm, filters, limit } = req.body;
          const items = await searchEntities('connection', searchTerm, filters, limit, workspaceId);
          res.json({ items, nextCursor: null, hasMore: false });
          break;
        }
        case 'searchActivityLogs': {
          const { searchTerm, filters, limit } = req.body;
          const items = await searchEntities('activity_log', searchTerm, filters, limit, workspaceId);
          res.json({ items, nextCursor: null, hasMore: false });
          break;
        }
        case 'fetchVaultCredentials': {
          const { fetchVaultCredentials: fetchVault } = await import('./vault.js');
          const vaultResult = await fetchVault(req.body);
          res.json(vaultResult);
          break;
        }
        case 'generateLineage':
          res.json({ error: 'Lineage feature has been removed' });
          break;
        case 'syncAirflowDagsAsync':
          res.json({ status: 'sync_not_available', message: 'Airflow sync not configured in local environment' });
          break;
        case 'triggerDependentPipelines':
          res.json({ triggered: [] });
          break;
        default:
          res.status(404).json({ error: `Function '${functionName}' not found` });
      }
    } catch (err) {
      logger.error({ err: err.message, function: functionName }, 'function invocation error');
      res.status(500).json({ error: err.message });
    }
  }));

  const ALLOWED_PATH_PREFIX = 'specs/';

  app.get('/api/gitlab/config', (req, res) => {
    res.json(getGitLabConfig());
  });

  app.post('/api/gitlab/status', async (req, res) => {
    try {
      const { username, password, gitlabUrl, project } = req.body;
      if (!username || !password) {
        return res.status(400).json({ connected: false, error: 'Username and password required' });
      }
      const status = await gitlabCheckStatus({ username, password, gitlabUrl, project });
      res.json(status);
    } catch (err) {
      res.json({ connected: false, error: err.message });
    }
  });

  app.post('/api/gitlab/commit', async (req, res) => {
    try {
      const { username, password, branch, files, commitMessage, gitlabUrl, project } = req.body;
      if (!username || !password) {
        return res.status(400).json({ success: false, error: 'LDAP credentials required' });
      }
      if (!files || !Array.isArray(files) || files.length === 0) {
        return res.status(400).json({ success: false, error: 'Missing required field: files' });
      }
      for (const f of files) {
        if (!f.path || !f.path.startsWith(ALLOWED_PATH_PREFIX)) {
          return res.status(400).json({ success: false, error: `File path must start with "${ALLOWED_PATH_PREFIX}": ${f.path}` });
        }
        if (f.path.includes('..')) {
          return res.status(400).json({ success: false, error: 'Path traversal not allowed' });
        }
      }
      const safeBranch = (branch || 'main').replace(/[^a-zA-Z0-9_\-/.]/g, '_');
      const result = await gitlabCommitFiles({
        username,
        password,
        project,
        gitlabUrl,
        branch: safeBranch,
        files,
        commitMessage: (commitMessage || 'DataFlow pipeline deployment').substring(0, 500),
      });
      res.json({ success: true, ...result });
    } catch (err) {
      logger.error({ err: err.message }, 'gitlab commit error');
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.post('/api/admin/purge-logs', requireWorkspace(async (req, res) => {
    try {
      const days = parseInt(req.body?.days) || 30;
      const workspaceId = req.workspaceId;
      const result = await pool.query(
        `DELETE FROM "activity_log" WHERE created_date < NOW() - INTERVAL '1 day' * $1 AND workspace_id = $2`,
        [days, workspaceId]
      );
      res.json({ deleted: result.rowCount });
    } catch (err) {
      logger.error({ err: err.message }, 'purge logs error');
      res.status(500).json({ error: err.message });
    }
  }));

  app.get('/api/admin/data-model', async (req, res) => {
    try {
      const allTables = [...ENTITY_TABLES, 'workspace'];
      const tableNames = allTables.map(t => `'${t}'`).join(',');
      const columnsResult = await pool.query(
        `SELECT table_name, column_name, data_type, is_nullable
         FROM information_schema.columns
         WHERE table_name IN (${tableNames})
         ORDER BY table_name, ordinal_position`
      );
      const indexesResult = await pool.query(
        `SELECT tablename, indexname, indexdef
         FROM pg_indexes
         WHERE tablename IN (${tableNames})
         ORDER BY tablename, indexname`
      );
      const tablesMap = {};
      for (const row of columnsResult.rows) {
        if (!tablesMap[row.table_name]) {
          tablesMap[row.table_name] = { name: row.table_name, columns: [] };
        }
        tablesMap[row.table_name].columns.push({
          column_name: row.column_name,
          data_type: row.data_type,
          is_nullable: row.is_nullable
        });
      }
      res.json({
        tables: Object.values(tablesMap),
        indexes: indexesResult.rows.map(r => ({
          tablename: r.tablename,
          indexname: r.indexname,
          indexdef: r.indexdef
        }))
      });
    } catch (err) {
      logger.error({ err: err.message }, 'data model error');
      res.status(500).json({ error: err.message });
    }
  });

  return (req, res, next) => {
    if (!req.url.startsWith('/api')) {
      return next();
    }
    app(req, res, next);
  };
}
