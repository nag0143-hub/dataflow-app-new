import express from 'express';
import { pool, entityNameToTable } from './db.js';
import fs from 'fs';
import path from 'path';
import { generatePythonTasksScript } from './scripts/generate-pipeline-tasks.js';
import { generateSparkScript } from './scripts/generate-spark-script.js';
import { generateBootstrapDagLoader, generateMappingFiles } from './scripts/generate-bootstrap.js';

const router = express.Router();

const jwtCache = new Map();
const JWT_TTL_MS = 25 * 60 * 1000;

function validateAirflowHost(host) {
  if (!host || typeof host !== 'string') throw new Error('Airflow URL is required');
  let url;
  try { url = new URL(host); } catch { throw new Error('Invalid Airflow URL format'); }
  if (!['http:', 'https:'].includes(url.protocol)) throw new Error('Airflow URL must use http or https');
  return url.origin;
}

function formatRecord(row) {
  return { id: String(row.id), ...row.data, created_date: row.created_date, updated_date: row.updated_date };
}

async function getAirflowConnection(connectionId) {
  const table = entityNameToTable('Connection');
  const result = await pool.query(`SELECT * FROM "${table}" WHERE id = $1`, [parseInt(connectionId)]);
  if (result.rows.length === 0) return null;
  const rec = formatRecord(result.rows[0]);
  return rec;
}

function resolveHost(host) {
  if (process.env.DOCKER_ENV === 'true' || process.env.RUNNING_IN_DOCKER === 'true') {
    return host.replace(/\/\/localhost([:\/])/i, '//host.docker.internal$1')
               .replace(/\/\/127\.0\.0\.1([:\/])/i, '//host.docker.internal$1');
  }
  return host;
}

function getApiVersion(connection) {
  return connection.api_version || process.env.AIRFLOW_DEFAULT_API_VERSION || 'v2';
}

function getHealthPath(apiVersion) {
  return apiVersion === 'v1' ? '/health' : '/monitor/health';
}

async function acquireJwt(baseUrl, username, password) {
  const cacheKey = `${baseUrl}:${username}`;
  const cached = jwtCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.token;
  }

  const resp = await fetch(`${baseUrl}/auth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
    signal: AbortSignal.timeout(10000),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`JWT auth failed (${resp.status}): ${text.substring(0, 200)}`);
  }

  const data = await resp.json();
  const token = data.access_token;
  if (!token) throw new Error('JWT response missing access_token');

  jwtCache.set(cacheKey, { token, expiresAt: Date.now() + JWT_TTL_MS });
  return token;
}

async function airflowFetch(connection, apiPath, options = {}) {
  const baseUrl = resolveHost(validateAirflowHost(connection.host));
  const apiVersion = getApiVersion(connection);

  const url = `${baseUrl}/api/${apiVersion}${apiPath}`;
  const authUser = connection.airflow_username || connection.username;
  const authPass = connection.airflow_password || connection.password;

  const headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    ...(options.headers || {}),
  };

  if (connection.auth_method === 'basic' && authUser && authPass) {
    if (apiVersion === 'v2') {
      try {
        const jwt = await acquireJwt(baseUrl, authUser, authPass);
        headers['Authorization'] = `Bearer ${jwt}`;
      } catch {
        headers['Authorization'] = `Basic ${Buffer.from(`${authUser}:${authPass}`).toString('base64')}`;
      }
    } else {
      headers['Authorization'] = `Basic ${Buffer.from(`${authUser}:${authPass}`).toString('base64')}`;
    }
  } else {
    const token = connection.api_token || connection.password || connection.username;
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }
  }

  const resp = await fetch(url, {
    method: options.method || 'GET',
    headers,
    body: options.body ? JSON.stringify(options.body) : undefined,
    signal: AbortSignal.timeout(parseInt(process.env.AIRFLOW_API_TIMEOUT_MS) || 15000),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    const status = resp.status;
    if (status === 401) {
      const cacheKey = `${baseUrl}:${authUser}`;
      jwtCache.delete(cacheKey);
      throw new Error('Authentication failed (401) — verify your username/password or API token are correct');
    }
    if (status === 403) throw new Error('Access denied (403) — the credentials are valid but lack permission. Check the user role in Airflow');
    if (status === 404) throw new Error('Airflow API endpoint not found (404) — verify the URL includes the correct base path (e.g., https://airflow.example.com)');
    if (status === 422) throw new Error(`Validation error (422): ${text.substring(0, 300)}`);
    throw new Error(`Airflow API returned ${status}: ${text.substring(0, 200)}`);
  }

  return resp.json();
}

router.get('/connections', async (req, res) => {
  try {
    const table = entityNameToTable('Connection');
    const result = await pool.query(
      `SELECT * FROM "${table}" WHERE data->>'platform' = 'airflow' ORDER BY created_date DESC`
    );
    const conns = result.rows.map(r => {
      const rec = formatRecord(r);
      delete rec.password;
      delete rec.api_token;
      delete rec.airflow_password;
      return rec;
    });
    res.json(conns);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/connections', async (req, res) => {
  try {
    const { name, host, auth_method, username, password, api_token, dags_folder, api_version } = req.body;
    if (!name?.trim() || !host?.trim()) return res.status(400).json({ error: 'Name and Airflow URL are required' });

    const validatedHost = validateAirflowHost(host);

    const table = entityNameToTable('Connection');
    const data = {
      name, host: validatedHost, platform: 'airflow',
      auth_method: auth_method || 'bearer', username, password, api_token, status: 'active',
      dags_folder: dags_folder || '',
      api_version: api_version || 'v2',
    };
    const result = await pool.query(
      `INSERT INTO "${table}" (data, created_by) VALUES ($1, $2) RETURNING *`,
      [JSON.stringify(data), 'user@local']
    );
    const rec = formatRecord(result.rows[0]);
    delete rec.password; delete rec.api_token; delete rec.airflow_password;
    res.status(201).json(rec);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

router.delete('/connections/:id', async (req, res) => {
  try {
    const table = entityNameToTable('Connection');
    await pool.query(`DELETE FROM "${table}" WHERE id = $1`, [parseInt(req.params.id)]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/connections/test', async (req, res) => {
  const start = Date.now();
  try {
    const { host, auth_method, username, password, api_token, api_version } = req.body;
    if (!host?.trim()) return res.status(400).json({ success: false, error: 'Airflow URL is required' });

    const validatedHost = validateAirflowHost(host);
    const tempConn = { host: validatedHost, auth_method, username, password, api_token, api_version: api_version || 'v2' };

    const healthPath = getHealthPath(getApiVersion(tempConn));
    const data = await airflowFetch(tempConn, healthPath);
    res.json({ success: true, latency_ms: Date.now() - start, details: data, api_version: getApiVersion(tempConn) });
  } catch (err) {
    let errorMsg = err.message;
    if (err.cause?.code === 'ECONNREFUSED') {
      errorMsg = 'Connection refused — the Airflow server is not reachable at this URL. Check that the host and port are correct and the server is running.';
    } else if (err.cause?.code === 'ENOTFOUND' || err.cause?.code === 'EAI_AGAIN') {
      errorMsg = 'Could not resolve hostname — check the Airflow URL for typos.';
    } else if (err.name === 'AbortError' || err.message?.includes('timed out')) {
      errorMsg = 'Connection timed out after 15 seconds — the Airflow server may be unreachable or behind a firewall.';
    } else if (err.cause?.code === 'UNABLE_TO_VERIFY_LEAF_SIGNATURE' || err.message?.includes('certificate')) {
      errorMsg = 'SSL certificate error — the Airflow server has an untrusted certificate. Try using http:// instead of https://, or ensure the certificate is valid.';
    }
    res.json({ success: false, error: errorMsg, latency_ms: Date.now() - start });
  }
});

router.post('/connections/:id/test', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.id);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const start = Date.now();
    const healthPath = getHealthPath(getApiVersion(conn));
    const data = await airflowFetch(conn, healthPath);
    res.json({ success: true, latency_ms: Date.now() - start, details: data, api_version: getApiVersion(conn) });
  } catch (err) {
    res.json({ success: false, error: err.message, latency_ms: 0 });
  }
});

router.get('/:connectionId/dags', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    const offset = parseInt(req.query.offset) || 0;
    const onlyActive = req.query.only_active === 'true';
    const search = req.query.search || '';

    let apiPath = `/dags?limit=${limit}&offset=${offset}&order_by=-last_parsed_time`;
    if (onlyActive) apiPath += '&only_active=true';
    if (search) apiPath += `&dag_id_pattern=${encodeURIComponent(search)}`;

    const data = await airflowFetch(conn, apiPath);
    res.json({
      dags: (data.dags || []).map(d => ({
        dag_id: d.dag_id,
        description: d.description,
        file_token: d.file_token,
        is_paused: d.is_paused,
        is_active: d.is_active,
        owners: d.owners,
        schedule: d.schedule || d.timetable_description || d.schedule_interval?.value || null,
        schedule_interval: d.schedule || d.timetable_description || d.schedule_interval?.value || null,
        tags: (d.tags || []).map(t => t.name || t),
        last_parsed_time: d.last_parsed_time,
        next_dagrun: d.next_dagrun,
        has_task_concurrency_limits: d.has_task_concurrency_limits,
      })),
      total_entries: data.total_entries || 0,
    });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

router.get('/:connectionId/dags/:dagId', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const data = await airflowFetch(conn, `/dags/${encodeURIComponent(req.params.dagId)}`);
    res.json(data);
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

router.get('/:connectionId/dags/:dagId/dagRuns', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const offset = parseInt(req.query.offset) || 0;
    const orderField = getApiVersion(conn) === 'v1' ? '-execution_date' : '-logical_date';
    const data = await airflowFetch(conn,
      `/dags/${encodeURIComponent(req.params.dagId)}/dagRuns?limit=${limit}&offset=${offset}&order_by=${orderField}`
    );
    res.json({
      dag_runs: (data.dag_runs || []).map(r => ({
        dag_run_id: r.dag_run_id,
        dag_id: r.dag_id,
        state: r.state,
        logical_date: r.logical_date || r.execution_date,
        execution_date: r.logical_date || r.execution_date,
        start_date: r.start_date,
        end_date: r.end_date,
        external_trigger: r.external_trigger,
        conf: r.conf,
      })),
      total_entries: data.total_entries || 0,
    });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

router.get('/:connectionId/dags/:dagId/tasks', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const data = await airflowFetch(conn, `/dags/${encodeURIComponent(req.params.dagId)}/tasks`);
    res.json({
      tasks: (data.tasks || []).map(t => ({
        task_id: t.task_id,
        operator_name: t.operator_name,
        downstream_task_ids: t.downstream_task_ids,
        pool: t.pool,
        retries: t.retries,
      })),
    });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

router.get('/:connectionId/dags/:dagId/dagRuns/:runId/taskInstances', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const data = await airflowFetch(conn,
      `/dags/${encodeURIComponent(req.params.dagId)}/dagRuns/${encodeURIComponent(req.params.runId)}/taskInstances`
    );
    res.json({
      task_instances: (data.task_instances || []).map(ti => ({
        task_id: ti.task_id,
        state: ti.state,
        start_date: ti.start_date,
        end_date: ti.end_date,
        duration: ti.duration,
        try_number: ti.try_number,
        operator: ti.operator,
      })),
    });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

router.get('/:connectionId/dags/:dagId/dagRuns/:runId/taskInstances/:taskId/logs/:tryNumber', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const { dagId, runId, taskId, tryNumber } = req.params;
    const baseUrl = resolveHost((conn.host || '').replace(/\/+$/, ''));
    const apiVersion = getApiVersion(conn);
    const url = `${baseUrl}/api/${apiVersion}/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(runId)}/taskInstances/${encodeURIComponent(taskId)}/logs/${tryNumber}`;

    const authUser = conn.airflow_username || conn.username;
    const authPass = conn.airflow_password || conn.password;
    const headers = { 'Accept': 'text/plain' };

    if (conn.auth_method === 'basic' && authUser && authPass) {
      if (apiVersion === 'v2') {
        try {
          const jwt = await acquireJwt(baseUrl, authUser, authPass);
          headers['Authorization'] = `Bearer ${jwt}`;
        } catch {
          headers['Authorization'] = `Basic ${Buffer.from(`${authUser}:${authPass}`).toString('base64')}`;
        }
      } else {
        headers['Authorization'] = `Basic ${Buffer.from(`${authUser}:${authPass}`).toString('base64')}`;
      }
    } else {
      const token = conn.api_token || conn.password || conn.username;
      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }
    }

    const resp = await fetch(url, { headers, signal: AbortSignal.timeout(parseInt(process.env.AIRFLOW_API_TIMEOUT_MS) || 15000) });
    if (!resp.ok) return res.status(resp.status).json({ error: 'Failed to fetch logs' });
    const text = await resp.text();
    res.type('text/plain').send(text);
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

router.post('/:connectionId/dags/:dagId/dagRuns', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const data = await airflowFetch(conn,
      `/dags/${encodeURIComponent(req.params.dagId)}/dagRuns`,
      { method: 'POST', body: { conf: req.body.conf || {} } }
    );
    res.json({
      dag_run_id: data.dag_run_id,
      state: data.state,
      logical_date: data.logical_date || data.execution_date,
      execution_date: data.logical_date || data.execution_date,
    });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

router.patch('/:connectionId/dags/:dagId', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const data = await airflowFetch(conn,
      `/dags/${encodeURIComponent(req.params.dagId)}`,
      { method: 'PATCH', body: { is_paused: req.body.is_paused } }
    );
    res.json({ dag_id: data.dag_id, is_paused: data.is_paused });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

router.post('/:connectionId/dags/checkin', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const { filename, content, subfolder } = req.body;
    if (!filename?.trim()) return res.status(400).json({ error: 'Filename is required' });
    if (!content?.trim()) return res.status(400).json({ error: 'DAG content is required' });

    const safeFilename = filename.replace(/[^a-zA-Z0-9._-]/g, '_');
    if (!safeFilename.endsWith('.yaml') && !safeFilename.endsWith('.yml') && !safeFilename.endsWith('.py')) {
      return res.status(400).json({ error: 'Filename must end with .yaml, .yml, or .py' });
    }

    const dagsFolder = path.resolve(conn.dags_folder || process.env.AIRFLOW_DAGS_FOLDER || '/opt/airflow/dags');
    let targetDir = dagsFolder;
    if (subfolder?.trim()) {
      const safeSub = subfolder.replace(/\.\./g, '').replace(/^[/\\]+/, '').replace(/[^a-zA-Z0-9/_-]/g, '_');
      targetDir = path.resolve(dagsFolder, safeSub);
    }

    if (!targetDir.startsWith(dagsFolder)) {
      return res.status(400).json({ error: 'Subfolder must be within the DAGs folder' });
    }

    await fs.promises.mkdir(targetDir, { recursive: true });
    const filePath = path.resolve(targetDir, safeFilename);
    if (!filePath.startsWith(dagsFolder)) {
      return res.status(400).json({ error: 'File path must be within the DAGs folder' });
    }
    await fs.promises.writeFile(filePath, content, 'utf8');

    res.json({
      success: true,
      file_path: filePath,
      dags_folder: dagsFolder,
      message: `DAG file written to ${filePath}`,
    });
  } catch (err) {
    if (err.code === 'EACCES') {
      res.status(403).json({ error: `Permission denied writing to DAGs folder. Ensure the application has write access to the configured path.` });
    } else if (err.code === 'ENOENT') {
      res.status(400).json({ error: `DAGs folder path does not exist and could not be created: ${err.message}` });
    } else {
      res.status(500).json({ error: err.message });
    }
  }
});

router.get('/:connectionId/dags-folder', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const dagsFolder = path.resolve(conn.dags_folder || process.env.AIRFLOW_DAGS_FOLDER || '/opt/airflow/dags');
    let exists = false;
    let writable = false;
    let files = [];
    try {
      await fs.promises.access(dagsFolder, fs.constants.R_OK);
      exists = true;
      await fs.promises.access(dagsFolder, fs.constants.W_OK);
      writable = true;
      const entries = await fs.promises.readdir(dagsFolder);
      files = entries.filter(f => f.endsWith('.yaml') || f.endsWith('.yml') || f.endsWith('.py')).sort();
    } catch { }

    res.json({ dags_folder: dagsFolder, exists, writable, dag_files: files });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.delete('/:connectionId/dags-folder/:filename', async (req, res) => {
  try {
    const conn = await getAirflowConnection(req.params.connectionId);
    if (!conn) return res.status(404).json({ error: 'Connection not found' });

    const dagsFolder = path.resolve(conn.dags_folder || process.env.AIRFLOW_DAGS_FOLDER || '/opt/airflow/dags');
    const safeFilename = req.params.filename.replace(/[^a-zA-Z0-9._-]/g, '_');
    const filePath = path.resolve(dagsFolder, safeFilename);

    if (!filePath.startsWith(dagsFolder)) {
      return res.status(400).json({ error: 'Invalid filename' });
    }

    await fs.promises.unlink(filePath);
    res.json({ success: true, message: `Deleted ${safeFilename}` });
  } catch (err) {
    if (err.code === 'ENOENT') res.status(404).json({ error: 'File not found' });
    else res.status(500).json({ error: err.message });
  }
});

function toDagId(pipelineName) {
  return `dataflow__${(pipelineName || 'pipeline').replace(/[^a-zA-Z0-9]/g, '_').toLowerCase()}`;
}

function airflowStateToStatus(state) {
  if (!state) return null;
  const map = {
    success: 'completed',
    failed: 'failed',
    running: 'running',
    queued: 'running',
    up_for_retry: 'running',
    up_for_reschedule: 'running',
  };
  return map[state] || null;
}

router.get('/pipeline/:pipelineId/dag-status', async (req, res) => {
  try {
    const pipeTable = entityNameToTable('Pipeline');
    const pipeResult = await pool.query(`SELECT * FROM "${pipeTable}" WHERE id = $1`, [parseInt(req.params.pipelineId)]);
    if (pipeResult.rows.length === 0) return res.status(404).json({ error: 'Pipeline not found' });

    const pipeline = formatRecord(pipeResult.rows[0]);
    const dagId = toDagId(pipeline.name);

    const connTable = entityNameToTable('Connection');
    const airflowConns = await pool.query(
      `SELECT * FROM "${connTable}" WHERE data->>'platform' = 'airflow' AND data->>'status' = 'active' ORDER BY created_date`
    );

    if (airflowConns.rows.length === 0) {
      return res.json({ found: false, dag_id: dagId, message: 'No active Airflow connections' });
    }

    for (const connRow of airflowConns.rows) {
      const conn = formatRecord(connRow);
      try {
        const dagData = await airflowFetch(conn, `/dags/${encodeURIComponent(dagId)}`);

        let runs = [];
        try {
          const runsOrderField = getApiVersion(conn) === 'v1' ? '-execution_date' : '-logical_date';
          const runsData = await airflowFetch(conn,
            `/dags/${encodeURIComponent(dagId)}/dagRuns?limit=5&order_by=${runsOrderField}`
          );
          runs = (runsData.dag_runs || []).map(r => ({
            dag_run_id: r.dag_run_id,
            state: r.state,
            logical_date: r.logical_date || r.execution_date,
            start_date: r.start_date,
            end_date: r.end_date,
            duration: r.start_date && r.end_date
              ? Math.round((new Date(r.end_date) - new Date(r.start_date)) / 1000)
              : null,
          }));
        } catch {}

        let tasks = [];
        let taskStates = [];
        try {
          const tasksData = await airflowFetch(conn, `/dags/${encodeURIComponent(dagId)}/tasks`);
          tasks = (tasksData.tasks || []).map(t => ({
            task_id: t.task_id,
            operator_name: t.operator_name,
            downstream_task_ids: t.downstream_task_ids,
          }));

          if (runs.length > 0) {
            const latestRunId = runs[0].dag_run_id;
            try {
              const tiData = await airflowFetch(conn,
                `/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(latestRunId)}/taskInstances`
              );
              taskStates = (tiData.task_instances || []).map(ti => ({
                task_id: ti.task_id,
                state: ti.state,
                duration: ti.duration,
                try_number: ti.try_number,
              }));
            } catch {}
          }
        } catch {}

        return res.json({
          found: true,
          dag_id: dagId,
          connection_id: conn.id,
          connection_name: conn.name,
          airflow_host: conn.host,
          is_paused: dagData.is_paused,
          is_active: dagData.is_active,
          schedule: dagData.schedule || dagData.timetable_description || dagData.schedule_interval?.value || null,
          owners: dagData.owners || [],
          last_parsed_time: dagData.last_parsed_time,
          next_dagrun: dagData.next_dagrun,
          runs,
          tasks,
          task_states: taskStates,
          latest_state: runs.length > 0 ? runs[0].state : null,
        });
      } catch (dagErr) {
        if (dagErr.message?.includes('404')) continue;
        continue;
      }
    }

    res.json({ found: false, dag_id: dagId, message: 'DAG not found in any Airflow instance' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/pipeline/:pipelineId/trigger', async (req, res) => {
  try {
    const pipeTable = entityNameToTable('Pipeline');
    const pipeResult = await pool.query(`SELECT * FROM "${pipeTable}" WHERE id = $1`, [parseInt(req.params.pipelineId)]);
    if (pipeResult.rows.length === 0) return res.status(404).json({ error: 'Pipeline not found' });

    const pipeline = formatRecord(pipeResult.rows[0]);
    const dagId = toDagId(pipeline.name);

    const connTable = entityNameToTable('Connection');
    const airflowConns = await pool.query(
      `SELECT * FROM "${connTable}" WHERE data->>'platform' = 'airflow' AND data->>'status' = 'active' ORDER BY created_date`
    );

    if (airflowConns.rows.length === 0) {
      return res.status(400).json({ error: 'No active Airflow connections configured' });
    }

    for (const connRow of airflowConns.rows) {
      const conn = formatRecord(connRow);
      try {
        await airflowFetch(conn, `/dags/${encodeURIComponent(dagId)}`);

        const data = await airflowFetch(conn,
          `/dags/${encodeURIComponent(dagId)}/dagRuns`,
          { method: 'POST', body: { conf: req.body.conf || {} } }
        );

        return res.json({
          success: true,
          dag_run_id: data.dag_run_id,
          dag_id: dagId,
          state: data.state,
          logical_date: data.logical_date || data.execution_date,
          connection_name: conn.name,
        });
      } catch (dagErr) {
        if (dagErr.message?.includes('404')) continue;
        return res.status(502).json({ error: dagErr.message });
      }
    }

    res.status(404).json({ error: `DAG "${dagId}" not found in any Airflow instance` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/sync-pipeline-status', async (req, res) => {
  try {
    const connTable = entityNameToTable('Connection');
    const pipeTable = entityNameToTable('Pipeline');

    const airflowConns = await pool.query(
      `SELECT * FROM "${connTable}" WHERE data->>'platform' = 'airflow' AND data->>'status' = 'active'`
    );

    if (airflowConns.rows.length === 0) {
      return res.json({ synced: 0, message: 'No active Airflow connections' });
    }

    const pipelines = await pool.query(`SELECT * FROM "${pipeTable}"`);
    if (pipelines.rows.length === 0) {
      return res.json({ synced: 0, message: 'No pipelines' });
    }

    const pipelinesByDagId = new Map();
    for (const row of pipelines.rows) {
      const data = row.data || {};
      const dagId = toDagId(data.name);
      pipelinesByDagId.set(dagId, { id: row.id, data });
    }

    let synced = 0;
    const results = [];

    for (const connRow of airflowConns.rows) {
      const conn = formatRecord(connRow);
      try {
        const dagIds = [...pipelinesByDagId.keys()];

        for (const dagId of dagIds) {
          try {
            const syncOrderField = getApiVersion(conn) === 'v1' ? '-execution_date' : '-logical_date';
            const runData = await airflowFetch(conn,
              `/dags/${encodeURIComponent(dagId)}/dagRuns?limit=1&order_by=${syncOrderField}`
            );
            const latestRun = (runData.dag_runs || [])[0];
            if (!latestRun) continue;

            const newStatus = airflowStateToStatus(latestRun.state);
            if (!newStatus) continue;

            const pipeline = pipelinesByDagId.get(dagId);
            if (pipeline.data.status !== newStatus) {
              const updatedData = { ...pipeline.data, status: newStatus };
              await pool.query(
                `UPDATE "${pipeTable}" SET data = $1, updated_date = NOW() WHERE id = $2`,
                [JSON.stringify(updatedData), pipeline.id]
              );
              synced++;
              results.push({ dagId, oldStatus: pipeline.data.status, newStatus, airflowState: latestRun.state });
            }
          } catch (dagErr) {
            if (!dagErr.message?.includes('404')) {
              results.push({ dagId, error: dagErr.message });
            }
          }
        }
      } catch (connErr) {
        results.push({ connection: conn.name, error: connErr.message });
      }
    }

    res.json({ synced, total: pipelines.rows.length, results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const FLAT_FILE_PLATFORMS = ['flat_file_delimited', 'flat_file_fixed_width', 'cobol_ebcdic', 'sftp', 'nas', 'local_fs'];

router.post('/pipeline/:pipelineId/generate-deploy-package', async (req, res) => {
  try {
    const pipeTable = entityNameToTable('Pipeline');
    const pipeResult = await pool.query(`SELECT * FROM "${pipeTable}" WHERE id = $1`, [parseInt(req.params.pipelineId)]);
    if (pipeResult.rows.length === 0) return res.status(404).json({ error: 'Pipeline not found' });

    const pipeline = formatRecord(pipeResult.rows[0]);
    const connTable = entityNameToTable('Connection');
    const allConns = await pool.query(`SELECT * FROM "${connTable}"`);
    const connections = allConns.rows.map(formatRecord);

    const sourceConn = connections.find(c => c.id === String(pipeline.source_connection_id));
    const targetConn = connections.find(c => c.id === String(pipeline.target_connection_id));
    const srcPlatform = sourceConn?.platform || '';
    const tgtPlatform = targetConn?.platform || '';
    const isFlatFile = FLAT_FILE_PLATFORMS.includes(srcPlatform);

    const pipelineName = (pipeline.name || 'pipeline').replace(/[^a-z0-9_]/gi, '_').toLowerCase();
    const basePath = (pipeline.dag_callable_base_path || '/data/dags/').replace(/\/+$/, '');
    const subfolder = 'dataflow';

    const files = [];

    if (isFlatFile) {
      const tasksScript = generatePythonTasksScript(pipeline, sourceConn, targetConn);
      files.push({
        filename: `${pipelineName}_tasks.py`,
        content: tasksScript,
        subfolder,
        type: 'python_tasks',
        description: 'Python sensor + ingestion script for flat file pipeline',
      });
    }

    const advancedFeatures = pipeline.advanced_features || {};
    const needsSpark = !isFlatFile || advancedFeatures.data_cleansing || advancedFeatures.data_quality || advancedFeatures.security || Object.keys(pipeline.column_mappings || {}).length > 0;

    if (needsSpark) {
      const sparkScript = generateSparkScript(pipeline, sourceConn, targetConn, advancedFeatures);
      files.push({
        filename: `${pipelineName}_spark.py`,
        content: sparkScript,
        subfolder,
        type: 'pyspark_script',
        description: 'PySpark extraction/transformation script',
      });
    }

    const columnMappings = pipeline.column_mappings || {};
    const mappingFiles = generateMappingFiles(pipeline.name, columnMappings);
    for (const mf of mappingFiles) {
      files.push({
        filename: mf.filename,
        content: mf.content,
        subfolder: `${subfolder}/mappings`,
        type: 'mapping_json',
        description: `Column mapping for ${mf.filename}`,
      });
    }

    const bootstrapScript = generateBootstrapDagLoader(subfolder);
    files.push({
      filename: 'generate_dags.py',
      content: bootstrapScript,
      subfolder: null,
      type: 'bootstrap',
      description: 'dag-factory bootstrap loader (dags root)',
    });

    res.json({
      success: true,
      pipeline_id: pipeline.id,
      pipeline_name: pipeline.name,
      source_platform: srcPlatform,
      target_platform: tgtPlatform,
      is_flat_file: isFlatFile,
      needs_spark: needsSpark,
      files,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
