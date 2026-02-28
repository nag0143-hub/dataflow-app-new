import logger from './logger.js';

const ROLE_HIERARCHY = { admin: 3, developer: 2, viewer: 1 };

const PERMISSIONS = {
  'entity:read':    ['viewer', 'developer', 'admin'],
  'entity:create':  ['developer', 'admin'],
  'entity:update':  ['developer', 'admin'],
  'entity:delete':  ['developer', 'admin'],
  'entity:batch':   ['developer', 'admin'],

  'pipeline:run':   ['developer', 'admin'],
  'pipeline:deploy': ['developer', 'admin'],

  'connection:test': ['developer', 'admin'],
  'connection:introspect': ['developer', 'admin'],

  'admin:purge-logs':  ['admin'],
  'admin:data-model':  ['admin'],
  'admin:workspace:create': ['admin'],
  'admin:workspace:update': ['admin'],

  'airflow:read':    ['developer', 'admin'],
  'airflow:write':   ['admin'],

  'gitlab:commit':   ['developer', 'admin'],
};

function hasPermission(role, permission) {
  const allowed = PERMISSIONS[permission];
  if (!allowed) return true;
  return allowed.includes(role);
}

function hasMinRole(role, minRole) {
  return (ROLE_HIERARCHY[role] || 0) >= (ROLE_HIERARCHY[minRole] || 0);
}

function requireRole(...roles) {
  return (req, res, next) => {
    const userRole = req.sessionUser?.role;
    if (!userRole) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    if (!roles.includes(userRole)) {
      logger.warn({
        user: req.sessionUser?.username,
        role: userRole,
        requiredRoles: roles,
        path: req.originalUrl,
        method: req.method,
      }, 'access denied — insufficient role');
      return res.status(403).json({
        error: 'Forbidden — insufficient permissions',
        required_roles: roles,
        your_role: userRole,
      });
    }
    next();
  };
}

function requirePermission(permission) {
  return (req, res, next) => {
    const userRole = req.sessionUser?.role;
    if (!userRole) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    if (!hasPermission(userRole, permission)) {
      logger.warn({
        user: req.sessionUser?.username,
        role: userRole,
        permission,
        path: req.originalUrl,
        method: req.method,
      }, 'access denied — missing permission');
      return res.status(403).json({
        error: 'Forbidden — insufficient permissions',
        required_permission: permission,
        your_role: userRole,
      });
    }
    next();
  };
}

function entityWriteGuard(req, res, next) {
  const userRole = req.sessionUser?.role;
  if (!userRole) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  const method = req.method;
  if (method === 'GET') return next();

  if (method === 'POST' && req.path.endsWith('/filter')) return next();

  if (!hasMinRole(userRole, 'developer')) {
    logger.warn({
      user: req.sessionUser?.username,
      role: userRole,
      path: req.originalUrl,
      method,
    }, 'viewer attempted write operation');
    return res.status(403).json({
      error: 'Forbidden — viewers have read-only access',
      your_role: userRole,
    });
  }
  next();
}

export {
  ROLE_HIERARCHY,
  PERMISSIONS,
  hasPermission,
  hasMinRole,
  requireRole,
  requirePermission,
  entityWriteGuard,
};
