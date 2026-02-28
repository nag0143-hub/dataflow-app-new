import express from 'express';
import { setSession, getSession, deleteSession, isRedisConnected } from './redis.js';
import { pool, getDefaultWorkspaceId } from './db.js';
import logger from './logger.js';
import { loginSchema, workspaceCreateSchema, workspaceUpdateSchema, switchWorkspaceSchema, validateBody } from './validation.js';

const router = express.Router();

const sessions = new Map();

const SESSION_TTL_SECONDS = parseInt(process.env.LDAP_SESSION_TTL_HOURS || '8', 10) * 60 * 60;
const COOKIE_NAME = 'dataflow_sid';
const IS_PROD = process.env.NODE_ENV === 'production';

function setSessionCookie(res, sessionId) {
  res.cookie(COOKIE_NAME, sessionId, {
    httpOnly: true,
    secure: IS_PROD,
    sameSite: IS_PROD ? 'strict' : 'lax',
    maxAge: SESSION_TTL_SECONDS * 1000,
    path: '/',
  });
}

function clearSessionCookie(res) {
  res.clearCookie(COOKIE_NAME, {
    httpOnly: true,
    secure: IS_PROD,
    sameSite: IS_PROD ? 'strict' : 'lax',
    path: '/',
  });
}

function extractSessionId(req) {
  return req.cookies?.[COOKIE_NAME] || req.headers['x-session-id'] || null;
}

function generateSessionId() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let id = '';
  for (let i = 0; i < 48; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

const LDAP_CONFIG = {
  url: process.env.LDAP_URI || '',
  baseDN: process.env.LDAP_BASEDN || '',
  domain: process.env.LDAP_DOMAIN || '',
  adminEntitlement: process.env.LDAP_ROLES_MAPPING_ADMIN || '',
  devEntitlement: process.env.LDAP_ROLES_MAPPING_DEV || '',
  tlsRejectUnauthorized: process.env.LDAP_TLS_REJECT_UNAUTHORIZED !== 'false',
};

function determineRole(memberOf) {
  if (!memberOf) return 'viewer';
  const groups = Array.isArray(memberOf) ? memberOf : [memberOf];
  if (LDAP_CONFIG.adminEntitlement && groups.some(g => g.includes(LDAP_CONFIG.adminEntitlement))) {
    return 'admin';
  }
  if (LDAP_CONFIG.devEntitlement && groups.some(g => g.includes(LDAP_CONFIG.devEntitlement))) {
    return 'developer';
  }
  return 'viewer';
}

async function storeSession(sessionId, sessionData) {
  const stored = await setSession(sessionId, sessionData, SESSION_TTL_SECONDS);
  if (!stored) {
    sessions.set(sessionId, sessionData);
  }
}

async function retrieveSession(sessionId) {
  if (!sessionId) return null;

  const redisSession = await getSession(sessionId);
  if (redisSession) return redisSession;

  if (sessions.has(sessionId)) {
    const session = sessions.get(sessionId);
    const MAX_SESSION_AGE = SESSION_TTL_SECONDS * 1000;
    if (Date.now() - session.createdAt > MAX_SESSION_AGE) {
      sessions.delete(sessionId);
      return null;
    }
    return session;
  }
  return null;
}

async function removeSession(sessionId) {
  if (!sessionId) return;
  const deleted = await deleteSession(sessionId);
  if (!deleted) {
    sessions.delete(sessionId);
  }
}

async function resolveWorkspaceForUser(user) {
  try {
    const defaultWsId = await getDefaultWorkspaceId();
    if (defaultWsId) {
      const ws = await pool.query('SELECT id, name, slug FROM workspace WHERE id = $1', [defaultWsId]);
      if (ws.rows.length > 0) {
        return ws.rows[0];
      }
    }
  } catch (err) {
    logger.error({ err: err.message, userId: user?.id }, 'failed to resolve workspace');
  }
  return { id: 1, name: 'Default Workspace', slug: 'default' };
}

router.post('/login', validateBody(loginSchema), async (req, res) => {
  const { username, password } = req.validatedBody;

  const isDemoMode = process.env.LDAP_DEMO_MODE === 'true' || !LDAP_CONFIG.url || !LDAP_CONFIG.baseDN;

  if (isDemoMode && IS_PROD) {
    logger.error('demo mode is disabled in production — set LDAP_DEMO_MODE=false and configure LDAP_URI/LDAP_BASEDN');
    return res.status(503).json({ error: 'Demo mode is disabled in production. Contact your administrator to configure LDAP.' });
  }

  if (isDemoMode) {
    if (username === 'admin' && password === 'admin') {
      const sessionId = generateSessionId();
      const user = {
        id: '1',
        username: 'admin',
        name: 'Admin User',
        email: 'admin@dataflow.local',
        role: 'admin',
        firstName: 'Admin',
        memberOf: [],
      };
      const workspace = await resolveWorkspaceForUser(user);
      const sessionData = { user, workspace, createdAt: Date.now() };
      await storeSession(sessionId, sessionData);
      setSessionCookie(res, sessionId);
      logger.info({ username, workspace: workspace.slug, mode: 'demo' }, 'user authenticated');
      return res.json({ success: true, sessionId, user, workspace });
    }
    logger.warn({ username, mode: 'demo' }, 'login failed — invalid credentials');
    return res.status(401).json({ error: 'Invalid credentials. Demo mode: use admin / admin' });
  }

  try {
    const { authenticate } = await import('ldap-authentication');

    const userDn = `${username}@${LDAP_CONFIG.domain}`;

    const options = {
      ldapOpts: {
        url: LDAP_CONFIG.url,
        tlsOptions: { rejectUnauthorized: LDAP_CONFIG.tlsRejectUnauthorized },
      },
      userDn: userDn,
      userPassword: password,
      userSearchBase: LDAP_CONFIG.baseDN,
      usernameAttribute: 'sAMAccountName',
      username: username,
    };

    const ldapUser = await authenticate(options);

    if (!ldapUser) {
      return res.status(401).json({ error: 'Authentication failed — invalid credentials' });
    }

    const role = determineRole(ldapUser.memberOf);
    const sessionId = generateSessionId();
    const user = {
      id: ldapUser.uid || ldapUser.sAMAccountName || username,
      username: ldapUser.cn || username,
      name: ldapUser.cn || ldapUser.displayName || username,
      email: ldapUser.mail || ldapUser.userPrincipalName || `${username}@${LDAP_CONFIG.domain}`,
      role,
      firstName: ldapUser.givenName || ldapUser.cn?.split(' ')[0] || username,
      memberOf: ldapUser.memberOf || [],
    };

    const workspace = await resolveWorkspaceForUser(user);
    const sessionData = { user, workspace, createdAt: Date.now() };
    await storeSession(sessionId, sessionData);
    setSessionCookie(res, sessionId);

    logger.info({ username: user.username, role, workspace: workspace.slug, mode: 'ldap' }, 'user authenticated');
    res.json({ success: true, sessionId, user, workspace });
  } catch (err) {
    logger.error({ err: err.message, username, mode: 'ldap' }, 'LDAP authentication error');

    let errorMsg = 'Authentication failed';
    if (err.message?.includes('ECONNREFUSED') || err.message?.includes('ENOTFOUND')) {
      errorMsg = 'LDAP server is unreachable. Please contact your administrator.';
    } else if (err.message?.includes('Invalid credentials') || err.message?.includes('52e')) {
      errorMsg = 'Invalid username or password';
    } else if (err.message?.includes('timeout') || err.message?.includes('ETIMEDOUT')) {
      errorMsg = 'LDAP server connection timed out. Please try again.';
    } else if (err.message?.includes('certificate') || err.message?.includes('CERT')) {
      errorMsg = 'LDAP SSL certificate error. Please contact your administrator.';
    }

    res.status(401).json({ error: errorMsg });
  }
});

router.post('/logout', async (req, res) => {
  const sessionId = extractSessionId(req);
  await removeSession(sessionId);
  clearSessionCookie(res);
  logger.info({ sessionId: sessionId?.slice(0, 8) }, 'user logged out');
  res.json({ success: true });
});

router.get('/me', async (req, res) => {
  const sessionId = extractSessionId(req);
  const session = await retrieveSession(sessionId);
  if (session) {
    return res.json({
      ...session.user,
      workspace: session.workspace,
      is_authenticated: true,
    });
  }
  res.status(401).json({ error: 'Not authenticated' });
});

router.get('/workspaces', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, name, slug, description, created_date FROM workspace ORDER BY created_date ASC'
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/workspaces', validateBody(workspaceCreateSchema), async (req, res) => {
  try {
    const { name, description } = req.validatedBody;
    const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
    const existing = await pool.query('SELECT id FROM workspace WHERE slug = $1', [slug]);
    if (existing.rows.length > 0) {
      return res.status(409).json({ error: `A workspace with slug "${slug}" already exists` });
    }
    const result = await pool.query(
      'INSERT INTO workspace (name, slug, description) VALUES ($1, $2, $3) RETURNING *',
      [name, slug, description]
    );
    logger.info({ workspace: slug }, 'workspace created');
    res.status(201).json(result.rows[0]);
  } catch (err) {
    logger.error({ err: err.message }, 'workspace create failed');
    res.status(500).json({ error: err.message });
  }
});

router.put('/workspaces/:id', validateBody(workspaceUpdateSchema), async (req, res) => {
  try {
    const { name, description } = req.validatedBody;
    const result = await pool.query(
      'UPDATE workspace SET name = COALESCE($1, name), description = COALESCE($2, description), updated_date = NOW() WHERE id = $3 RETURNING *',
      [name, description, parseInt(req.params.id)]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Workspace not found' });
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/switch-workspace', validateBody(switchWorkspaceSchema), async (req, res) => {
  const sessionId = extractSessionId(req);
  const { workspace_id } = req.validatedBody;

  const session = await retrieveSession(sessionId);
  if (!session) {
    return res.status(401).json({ error: 'Not authenticated' });
  }

  try {
    const ws = await pool.query('SELECT id, name, slug FROM workspace WHERE id = $1', [workspace_id]);
    if (ws.rows.length === 0) {
      return res.status(404).json({ error: 'Workspace not found' });
    }

    session.workspace = ws.rows[0];
    await storeSession(sessionId, session);
    logger.info({ workspace: ws.rows[0].slug, user: session.user?.username }, 'workspace switched');
    res.json({ success: true, workspace: ws.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

setInterval(() => {
  if (isRedisConnected()) return;
  const MAX_AGE = SESSION_TTL_SECONDS * 1000;
  const now = Date.now();
  for (const [id, session] of sessions) {
    if (now - session.createdAt > MAX_AGE) sessions.delete(id);
  }
}, 30 * 60 * 1000);

async function resolveSessionFull(req) {
  const sessionId = req.cookies?.[COOKIE_NAME] || req.headers['x-session-id'] || null;
  if (!sessionId) return null;
  return retrieveSession(sessionId);
}

export { retrieveSession, resolveSessionFull, extractSessionId };
export default router;
