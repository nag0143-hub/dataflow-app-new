import pg from 'pg';

const INTROSPECT_TIMEOUT = 15000;

async function introspectPostgres(config) {
  const { Pool } = pg;
  const pool = new Pool({
    host: config.host,
    port: parseInt(config.port) || 5432,
    database: config.database || 'postgres',
    user: config.username,
    password: config.password,
    ssl: config.ssl === true ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: INTROSPECT_TIMEOUT,
  });

  try {
    const client = await pool.connect();
    try {
      const result = await client.query(
        "SELECT table_schema, table_name, column_name, data_type, ordinal_position, is_nullable, " +
        "character_maximum_length, numeric_precision " +
        "FROM information_schema.columns " +
        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') " +
        "ORDER BY table_schema, table_name, ordinal_position"
      );

      const schemaMap = {};
      for (const row of result.rows) {
        const schemaName = row.table_schema;
        const tableName = row.table_name;

        if (!schemaMap[schemaName]) {
          schemaMap[schemaName] = {};
        }
        if (!schemaMap[schemaName][tableName]) {
          schemaMap[schemaName][tableName] = [];
        }

        let typeDisplay = row.data_type;
        if (row.character_maximum_length) {
          typeDisplay += "(" + row.character_maximum_length + ")";
        } else if (row.numeric_precision) {
          typeDisplay += "(" + row.numeric_precision + ")";
        }

        schemaMap[schemaName][tableName].push({
          name: row.column_name,
          type: typeDisplay,
          nullable: row.is_nullable === "YES",
          position: row.ordinal_position,
        });
      }

      const schemas = Object.entries(schemaMap).map(([schemaName, tables]) => ({
        name: schemaName,
        tables: Object.entries(tables).map(([tableName, columns]) => ({
          name: tableName,
          columns,
        })),
      }));

      return { success: true, schemas, database: config.database || "postgres" };
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

async function introspectMySQL(config) {
  const mysql = await import('mysql2/promise');
  const connection = await mysql.default.createConnection({
    host: config.host,
    port: parseInt(config.port) || 3306,
    database: config.database,
    user: config.username,
    password: config.password,
    connectTimeout: INTROSPECT_TIMEOUT,
    ssl: config.ssl === true ? { rejectUnauthorized: false } : undefined,
  });

  try {
    const [rows] = await connection.query(
      "SELECT table_schema, table_name, column_name, data_type, ordinal_position, is_nullable, " +
      "character_maximum_length, numeric_precision " +
      "FROM information_schema.columns " +
      "WHERE table_schema = ? " +
      "ORDER BY table_name, ordinal_position",
      [config.database]
    );

    const schemaMap = {};
    for (const row of rows) {
      const schemaName = row.TABLE_SCHEMA || row.table_schema;
      const tableName = row.TABLE_NAME || row.table_name;
      const colName = row.COLUMN_NAME || row.column_name;
      const dataType = row.DATA_TYPE || row.data_type;
      const pos = row.ORDINAL_POSITION || row.ordinal_position;
      const nullable = (row.IS_NULLABLE || row.is_nullable) === "YES";
      const charLen = row.CHARACTER_MAXIMUM_LENGTH || row.character_maximum_length;
      const numPrec = row.NUMERIC_PRECISION || row.numeric_precision;

      if (!schemaMap[schemaName]) schemaMap[schemaName] = {};
      if (!schemaMap[schemaName][tableName]) schemaMap[schemaName][tableName] = [];

      let typeDisplay = dataType;
      if (charLen) typeDisplay += "(" + charLen + ")";
      else if (numPrec) typeDisplay += "(" + numPrec + ")";

      schemaMap[schemaName][tableName].push({ name: colName, type: typeDisplay, nullable, position: pos });
    }

    const schemas = Object.entries(schemaMap).map(([name, tables]) => ({
      name,
      tables: Object.entries(tables).map(([tName, columns]) => ({ name: tName, columns })),
    }));

    return { success: true, schemas, database: config.database };
  } finally {
    await connection.end();
  }
}

async function introspectSQLServer(config) {
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
      connectTimeout: INTROSPECT_TIMEOUT,
      requestTimeout: INTROSPECT_TIMEOUT,
    },
  };

  const pool = await sql.default.connect(sqlConfig);
  try {
    const result = await pool.request().query(
      "SELECT s.name AS table_schema, t.name AS table_name, c.name AS column_name, " +
      "ty.name AS data_type, c.column_id AS ordinal_position, c.is_nullable, " +
      "c.max_length AS character_maximum_length, c.precision AS numeric_precision " +
      "FROM sys.tables t " +
      "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
      "JOIN sys.columns c ON t.object_id = c.object_id " +
      "JOIN sys.types ty ON c.user_type_id = ty.user_type_id " +
      "ORDER BY s.name, t.name, c.column_id"
    );

    const schemaMap = {};
    for (const row of result.recordset) {
      if (!schemaMap[row.table_schema]) schemaMap[row.table_schema] = {};
      if (!schemaMap[row.table_schema][row.table_name]) schemaMap[row.table_schema][row.table_name] = [];

      let typeDisplay = row.data_type;
      if (row.character_maximum_length && row.character_maximum_length > 0) typeDisplay += "(" + row.character_maximum_length + ")";
      else if (row.numeric_precision) typeDisplay += "(" + row.numeric_precision + ")";

      schemaMap[row.table_schema][row.table_name].push({
        name: row.column_name, type: typeDisplay, nullable: row.is_nullable === 1, position: row.ordinal_position
      });
    }

    const schemas = Object.entries(schemaMap).map(([name, tables]) => ({
      name,
      tables: Object.entries(tables).map(([tName, columns]) => ({ name: tName, columns })),
    }));

    return { success: true, schemas, database: config.database || "master" };
  } finally {
    await pool.close();
  }
}

export async function introspectSchema(connectionData) {
  const platform = connectionData.platform;
  const startTime = Date.now();

  try {
    let config = connectionData;

    if (connectionData.auth_method === "connection_string" && connectionData.connection_string) {
      const url = new URL(connectionData.connection_string);
      config = {
        ...connectionData,
        host: url.hostname,
        port: url.port || connectionData.port,
        database: url.pathname.replace("/", "") || connectionData.database,
        username: decodeURIComponent(url.username) || connectionData.username,
        password: decodeURIComponent(url.password) || connectionData.password,
      };
    }

    let result;

    switch (platform) {
      case "postgresql":
        result = await introspectPostgres(config);
        break;
      case "mysql":
      case "mariadb":
        result = await introspectMySQL(config);
        break;
      case "sql_server":
      case "azure_sql":
        result = await introspectSQLServer(config);
        break;
      default:
        return {
          success: false,
          error: "Schema introspection is not supported for platform: " + platform,
          latency_ms: Date.now() - startTime,
        };
    }

    return { ...result, latency_ms: Date.now() - startTime };
  } catch (err) {
    console.error("[introspect-schema] " + platform + " error:", err.message);
    let errorMessage = "Failed to introspect database schema.";
    if (err.code === "ECONNREFUSED") errorMessage = "Connection refused. Verify the host and port are correct.";
    else if (err.code === "ENOTFOUND") errorMessage = "Could not resolve hostname.";
    else if (err.message?.includes("password") || err.message?.includes("authentication")) errorMessage = "Authentication failed. Check credentials.";
    else if (err.message?.includes("timeout")) errorMessage = "Connection timed out.";
    else errorMessage = err.message;

    return { success: false, error: errorMessage, latency_ms: Date.now() - startTime };
  }
}
