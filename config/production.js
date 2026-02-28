export default {
  env: 'production',

  server: {
    port: parseInt(process.env.PORT || '5000', 10),
    host: process.env.HOST || '0.0.0.0',
  },

  database: {
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DB_SSL === 'false' ? false : { rejectUnauthorized: process.env.DB_SSL_REJECT_UNAUTHORIZED !== 'false' },
    poolMin: parseInt(process.env.DB_POOL_MIN || '5', 10),
    poolMax: parseInt(process.env.DB_POOL_MAX || '20', 10),
    idleTimeoutMs: parseInt(process.env.DB_IDLE_TIMEOUT_MS || '10000', 10),
    connectionTimeoutMs: parseInt(process.env.DB_CONNECTION_TIMEOUT_MS || '5000', 10),
  },

  auth: {
    enabled: process.env.AUTH_ENABLED === 'true',
  },

  cors: {
    origin: process.env.CORS_ORIGIN || '*',
    credentials: true,
  },

  api: {
    bodyLimit: process.env.API_BODY_LIMIT || '10mb',
    defaultPageSize: parseInt(process.env.API_DEFAULT_PAGE_SIZE || '100', 10),
    maxPageSize: parseInt(process.env.API_MAX_PAGE_SIZE || '1000', 10),
  },

  logging: {
    level: process.env.LOG_LEVEL || 'warn',
    requests: process.env.LOG_REQUESTS === 'true',
  },
};
