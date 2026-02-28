const DEFAULT_GITLAB_URL = (process.env.GITLAB_URL || 'https://gitlab.com').replace(/\/+$/, '');
const DEFAULT_GITLAB_PROJECT = process.env.GITLAB_PROJECT || '';

const ALLOWED_GITLAB_HOSTS = (process.env.GITLAB_ALLOWED_HOSTS || '')
  .split(',')
  .map(h => h.trim().toLowerCase())
  .filter(Boolean);

function validateGitlabUrl(url) {
  if (!url) return DEFAULT_GITLAB_URL;
  const cleaned = url.replace(/\/+$/, '');
  let parsed;
  try {
    parsed = new URL(cleaned);
  } catch {
    throw new Error('Invalid GitLab URL format');
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new Error('GitLab URL must use https or http protocol');
  }
  if (parsed.username || parsed.password) {
    throw new Error('GitLab URL must not contain credentials');
  }
  const hostname = parsed.hostname.toLowerCase();
  const blocked = ['localhost', '127.0.0.1', '0.0.0.0', '[::1]', 'metadata.google.internal', '169.254.169.254'];
  if (blocked.includes(hostname)) {
    throw new Error('GitLab URL points to a blocked host');
  }
  if (/^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.)/.test(hostname)) {
    throw new Error('GitLab URL must not point to private IP ranges');
  }
  if (ALLOWED_GITLAB_HOSTS.length > 0 && !ALLOWED_GITLAB_HOSTS.includes(hostname)) {
    throw new Error(`GitLab host '${hostname}' is not in the allowed hosts list`);
  }
  return cleaned;
}

async function authenticateWithLDAP(username, password, gitlabUrl) {
  const baseUrl = (gitlabUrl || DEFAULT_GITLAB_URL).replace(/\/+$/, '');
  const tokenUrl = `${baseUrl}/oauth/token`;
  const params = new URLSearchParams();
  params.set('grant_type', 'password');
  params.set('username', username);
  params.set('password', password);
  if (process.env.GITLAB_CLIENT_ID) {
    params.set('client_id', process.env.GITLAB_CLIENT_ID);
  }
  if (process.env.GITLAB_CLIENT_SECRET) {
    params.set('client_secret', process.env.GITLAB_CLIENT_SECRET);
  }

  const res = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
  });

  if (!res.ok) {
    const errText = await res.text().catch(() => '');
    if (res.status === 401 || res.status === 403) {
      throw new Error('LDAP authentication failed: invalid credentials');
    }
    throw new Error(`GitLab authentication failed (${res.status}): ${errText}`);
  }

  const data = await res.json();
  return data.access_token;
}

async function gitlabApi(token, method, path, body, gitlabUrl) {
  const baseUrl = (gitlabUrl || DEFAULT_GITLAB_URL).replace(/\/+$/, '');
  const url = `${baseUrl}/api/v4${path}`;
  const opts = {
    method,
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
  };
  if (body) opts.body = JSON.stringify(body);

  const res = await fetch(url, opts);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }

  if (!res.ok) {
    const msg = data?.message || data?.error || text;
    throw new Error(`GitLab API ${method} ${path} failed (${res.status}): ${typeof msg === 'object' ? JSON.stringify(msg) : msg}`);
  }
  return data;
}

export async function gitlabCommitFiles({ username, password, project, branch, files, commitMessage, gitlabUrl }) {
  const baseUrl = validateGitlabUrl(gitlabUrl);
  const token = await authenticateWithLDAP(username, password, baseUrl);
  const projectPath = project || DEFAULT_GITLAB_PROJECT;
  if (!projectPath) throw new Error('GitLab project path not configured');

  const encodedProject = encodeURIComponent(projectPath);

  let targetBranch;
  try {
    const projectInfo = await gitlabApi(token, 'GET', `/projects/${encodedProject}`, null, baseUrl);
    targetBranch = branch || projectInfo.default_branch || 'main';
  } catch {
    targetBranch = branch || 'main';
  }

  if (branch) {
    try {
      await gitlabApi(token, 'GET', `/projects/${encodedProject}/repository/branches/${encodeURIComponent(branch)}`, null, baseUrl);
    } catch {
      const projectInfo = await gitlabApi(token, 'GET', `/projects/${encodedProject}`, null, baseUrl);
      await gitlabApi(token, 'POST', `/projects/${encodedProject}/repository/branches`, {
        branch,
        ref: projectInfo.default_branch || 'main',
      }, baseUrl);
    }
    targetBranch = branch;
  }
  const defaultBranch = targetBranch;

  const actions = [];
  for (const f of files) {
    let action = 'create';
    try {
      await gitlabApi(token, 'GET', `/projects/${encodedProject}/repository/files/${encodeURIComponent(f.path)}?ref=${encodeURIComponent(defaultBranch)}`, null, baseUrl);
      action = 'update';
    } catch {}

    actions.push({
      action,
      file_path: f.path,
      content: f.content,
    });
  }

  const result = await gitlabApi(token, 'POST', `/projects/${encodedProject}/repository/commits`, {
    branch: defaultBranch,
    commit_message: commitMessage || 'DataFlow pipeline deployment',
    actions,
  }, baseUrl);

  return {
    sha: result.id,
    short_sha: result.short_id,
    branch: defaultBranch,
    url: `${baseUrl}/${projectPath}/-/commit/${result.id}`,
    files: files.map(f => f.path),
    author: result.author_name,
    message: result.message,
  };
}

export async function gitlabCheckStatus({ username, password, project, gitlabUrl }) {
  const baseUrl = validateGitlabUrl(gitlabUrl);
  const token = await authenticateWithLDAP(username, password, baseUrl);
  const user = await gitlabApi(token, 'GET', '/user', null, baseUrl);
  const projectPath = project || DEFAULT_GITLAB_PROJECT;
  let projectInfo = null;

  if (projectPath) {
    try {
      projectInfo = await gitlabApi(token, 'GET', `/projects/${encodeURIComponent(projectPath)}`, null, baseUrl);
    } catch {}
  }

  return {
    connected: true,
    login: user.username,
    name: user.name,
    email: user.email,
    gitlabUrl: baseUrl,
    project: projectPath,
    projectFound: !!projectInfo,
    projectName: projectInfo?.name_with_namespace || null,
    defaultBranch: projectInfo?.default_branch || null,
  };
}

export function getGitLabConfig() {
  return {
    url: DEFAULT_GITLAB_URL,
    project: DEFAULT_GITLAB_PROJECT,
  };
}
