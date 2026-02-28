import pg from 'pg';
import { resolveCredentials } from './vault.js';

const DRIVER_TIMEOUT = 10000;

async function testPostgres(config) {
  const { Pool } = pg;
  const pool = new Pool({
    host: config.host,
    port: parseInt(config.port) || 5432,
    database: config.database || 'postgres',
    user: config.username,
    password: config.password,
    ssl: config.ssl === true ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: DRIVER_TIMEOUT,
  });
  try {
    const client = await pool.connect();
    try {
      const versionResult = await client.query('SELECT version()');
      const version = versionResult.rows[0]?.version || 'PostgreSQL';
      const dbResult = await client.query('SELECT current_database(), current_user');
      const { current_database, current_user } = dbResult.rows[0] || {};
      return {
        success: true,
        server_version: version.split(',')[0],
        details: { database: current_database, user: current_user },
      };
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

async function testMySQL(config) {
  const mysql = await import('mysql2/promise');
  const connection = await mysql.default.createConnection({
    host: config.host,
    port: parseInt(config.port) || 3306,
    database: config.database,
    user: config.username,
    password: config.password,
    connectTimeout: DRIVER_TIMEOUT,
    ssl: config.ssl === true ? { rejectUnauthorized: false } : undefined,
  });
  try {
    const [rows] = await connection.query('SELECT VERSION() AS version');
    const version = rows[0]?.version || 'MySQL';
    const [dbRows] = await connection.query('SELECT DATABASE() AS db, USER() AS user');
    return {
      success: true,
      server_version: `MySQL ${version}`,
      details: { database: dbRows[0]?.db, user: dbRows[0]?.user },
    };
  } finally {
    await connection.end();
  }
}

async function testSQLServer(config) {
  const sql = await import('mssql');
  const sqlConfig = {
    server: config.host,
    port: parseInt(config.port) || 1433,
    database: config.database || 'master',
    user: config.username,
    password: config.password,
    options: {
      encrypt: config.ssl !== false,
      trustServerCertificate: true,
      connectTimeout: DRIVER_TIMEOUT,
      requestTimeout: DRIVER_TIMEOUT,
    },
  };
  const pool = await sql.default.connect(sqlConfig);
  try {
    const result = await pool.request().query('SELECT @@VERSION AS version, DB_NAME() AS db, SUSER_SNAME() AS username');
    const row = result.recordset[0] || {};
    const versionLine = (row.version || 'SQL Server').split('\n')[0];
    return {
      success: true,
      server_version: versionLine,
      details: { database: row.db, user: row.username },
    };
  } finally {
    await pool.close();
  }
}

async function testMongoDB(config) {
  const { MongoClient } = await import('mongodb');
  let uri;
  if (config.connection_string) {
    uri = config.connection_string;
  } else {
    const auth = config.username && config.password
      ? `${encodeURIComponent(config.username)}:${encodeURIComponent(config.password)}@`
      : '';
    const host = config.host || 'localhost';
    const port = config.port || 27017;
    const db = config.database || 'admin';
    uri = `mongodb://${auth}${host}:${port}/${db}`;
  }
  const client = new MongoClient(uri, {
    serverSelectionTimeoutMS: DRIVER_TIMEOUT,
    connectTimeoutMS: DRIVER_TIMEOUT,
    tls: config.ssl === true,
    tlsAllowInvalidCertificates: true,
  });
  try {
    await client.connect();
    const admin = client.db().admin();
    const info = await admin.serverInfo();
    const dbList = await admin.listDatabases();
    return {
      success: true,
      server_version: `MongoDB ${info.version}`,
      details: { databases: dbList.databases.length },
    };
  } finally {
    await client.close();
  }
}

async function testOracle(config) {
  let oracledb;
  try {
    oracledb = await import('oracledb');
  } catch {
    return {
      success: false,
      error_code: 'DRIVER_NOT_AVAILABLE',
      error_message: 'Oracle Instant Client driver (oracledb) is not installed. Install with: npm install oracledb. Note: Oracle requires the Instant Client libraries to be available on the system.',
    };
  }
  const connectString = config.connection_string || `${config.host}:${config.port || 1521}/${config.database || 'ORCL'}`;
  const connection = await oracledb.default.getConnection({
    user: config.username,
    password: config.password,
    connectString,
    connectTimeout: DRIVER_TIMEOUT / 1000,
  });
  try {
    const result = await connection.execute('SELECT * FROM V$VERSION WHERE ROWNUM = 1');
    const version = result.rows?.[0]?.[0] || 'Oracle Database';
    return {
      success: true,
      server_version: version,
      details: {},
    };
  } finally {
    await connection.close();
  }
}

export async function testConnection(connectionDataRaw) {
  const startTime = Date.now();
  const platform = connectionDataRaw.platform;

  if (!platform) {
    return {
      success: false,
      latency_ms: 0,
      error_code: 'NO_PLATFORM',
      error_message: 'No platform specified for this connection.',
    };
  }

  let connectionData = connectionDataRaw;
  if (connectionData.auth_method === 'vault_credentials') {
    try {
      connectionData = await resolveCredentials(connectionDataRaw);
    } catch (err) {
      return {
        success: false,
        latency_ms: Date.now() - startTime,
        error_code: 'VAULT_ERROR',
        error_message: err.message,
      };
    }
  }

  const dbPlatforms = ['postgresql', 'mysql', 'sql_server', 'oracle', 'mongodb'];
  const hasConnectionString = !!connectionData.connection_string;
  if (dbPlatforms.includes(platform)) {
    if (!connectionData.host && !hasConnectionString) {
      return {
        success: false,
        latency_ms: 0,
        error_code: 'MISSING_HOST',
        error_message: 'Host or connection string is required for database connections.',
      };
    }
    if (!hasConnectionString && platform !== 'mongodb' && connectionData.auth_method === 'password' && !connectionData.password && !connectionData._vault_resolved) {
      return {
        success: false,
        latency_ms: 0,
        error_code: 'MISSING_PASSWORD',
        error_message: 'Password is required when using password authentication.',
      };
    }
  }

  try {
    let result;
    switch (platform) {
      case 'postgresql':
        result = await testPostgres(connectionData);
        break;
      case 'mysql':
        result = await testMySQL(connectionData);
        break;
      case 'sql_server':
        result = await testSQLServer(connectionData);
        break;
      case 'oracle':
        result = await testOracle(connectionData);
        break;
      case 'mongodb':
        result = await testMongoDB(connectionData);
        break;
      case 'sftp':
        result = {
          success: false,
          error_code: 'NOT_IMPLEMENTED',
          error_message: 'SFTP connection testing requires ssh2 package. Install with: npm install ssh2',
        };
        break;
      case 'adls2':
      case 's3':
        result = {
          success: false,
          error_code: 'NOT_IMPLEMENTED',
          error_message: `Cloud storage (${platform.toUpperCase()}) testing requires the respective SDK. Configure proper credentials and SDK packages.`,
        };
        break;
      default:
        const filePlatforms = ['flat_file_delimited', 'flat_file_fixed_width', 'cobol_ebcdic', 'nas', 'local_fs'];
        if (filePlatforms.includes(platform)) {
          const filePath = connectionData.file_config?.nas_path;
          if (filePath) {
            const pathMod = await import('path');
            const resolved = pathMod.default.resolve(filePath);
            const blockedPrefixes = ['/etc', '/proc', '/sys', '/dev', '/root', '/var/run'];
            const isBlocked = blockedPrefixes.some(p => resolved.startsWith(p));
            if (isBlocked) {
              result = {
                success: false,
                error_code: 'PATH_RESTRICTED',
                error_message: 'The specified path is in a restricted system directory.',
              };
            } else {
              const fs = await import('fs');
              try {
                await fs.promises.access(resolved);
                const stat = await fs.promises.stat(resolved);
                result = {
                  success: true,
                  server_version: `File system path accessible`,
                  details: { type: stat.isDirectory() ? 'directory' : 'file' },
                };
              } catch {
                result = {
                  success: false,
                  error_code: 'PATH_NOT_FOUND',
                  error_message: `The specified path is not accessible. Verify it exists and has the correct permissions.`,
                };
              }
            }
          } else {
            result = {
              success: false,
              error_code: 'MISSING_PATH',
              error_message: 'Source path is required for file-based connections.',
            };
          }
        } else {
          result = {
            success: false,
            error_code: 'UNSUPPORTED_PLATFORM',
            error_message: `Platform "${platform}" is not supported for connection testing.`,
          };
        }
    }

    return {
      ...result,
      latency_ms: Date.now() - startTime,
    };
  } catch (err) {
    const latency = Date.now() - startTime;
    let errorCode = 'CONNECTION_FAILED';
    let errorMessage = 'Could not establish a connection. Check your configuration and try again.';

    console.error(`[test-connection] ${platform} error:`, err.message);

    if (err.code === 'ECONNREFUSED') {
      errorCode = 'CONNECTION_REFUSED';
      errorMessage = `Connection refused at the specified host and port. Verify the host and port are correct and the database server is running.`;
    } else if (err.code === 'ENOTFOUND' || err.code === 'EAI_AGAIN') {
      errorCode = 'HOST_NOT_FOUND';
      errorMessage = `Could not resolve the specified hostname. Check the hostname or IP address.`;
    } else if (err.code === 'ETIMEDOUT' || err.message?.includes('timeout')) {
      errorCode = 'CONNECTION_TIMEOUT';
      errorMessage = `Connection timed out after ${DRIVER_TIMEOUT / 1000}s. The host may be unreachable or firewalled.`;
    } else if (err.message?.includes('password') || err.message?.includes('authentication') || err.message?.includes('login') || err.message?.includes('denied')) {
      errorCode = 'AUTH_FAILED';
      errorMessage = `Authentication failed. Verify your username, password, and that the user has access to the specified database.`;
    } else if (err.message?.includes('does not exist') || err.message?.includes('Unknown database')) {
      errorCode = 'DATABASE_NOT_FOUND';
      errorMessage = `The specified database was not found. Check the database name.`;
    } else if (err.message?.includes('SSL') || err.message?.includes('certificate') || err.message?.includes('TLS')) {
      errorCode = 'SSL_ERROR';
      errorMessage = `SSL/TLS connection error. Try toggling the SSL setting, or verify the server supports encrypted connections.`;
    }

    return {
      success: false,
      latency_ms: latency,
      error_code: errorCode,
      error_message: errorMessage,
    };
  }
}
