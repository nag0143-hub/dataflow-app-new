const VAULT_TIMEOUT = 10000;

export async function fetchVaultCredentials(vaultConfig) {
  const {
    vault_url,
    vault_namespace,
    vault_role_id,
    vault_secret_id,
    vault_mount_point = 'secret',
    vault_secret_path,
  } = vaultConfig;

  if (!vault_url || !vault_role_id || !vault_secret_id || !vault_secret_path) {
    return {
      success: false,
      error: 'Missing required vault configuration (URL, Role ID, Secret ID, Secret Path)',
    };
  }

  const baseUrl = vault_url.replace(/\/+$/, '');
  const headers = {
    'Content-Type': 'application/json',
    ...(vault_namespace ? { 'X-Vault-Namespace': vault_namespace } : {}),
  };

  try {
    const loginRes = await fetch(`${baseUrl}/v1/auth/approle/login`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ role_id: vault_role_id, secret_id: vault_secret_id }),
      signal: AbortSignal.timeout(VAULT_TIMEOUT),
    });

    if (!loginRes.ok) {
      const body = await loginRes.text();
      return {
        success: false,
        error: `Vault AppRole login failed (${loginRes.status}): ${body}`,
      };
    }

    const loginData = await loginRes.json();
    const clientToken = loginData?.auth?.client_token;

    if (!clientToken) {
      return {
        success: false,
        error: 'Vault AppRole login returned no client token',
      };
    }

    const secretPath = vault_secret_path.replace(/^\/+/, '');
    const secretUrl = `${baseUrl}/v1/${vault_mount_point}/data/${secretPath}`;

    const secretRes = await fetch(secretUrl, {
      headers: { ...headers, 'X-Vault-Token': clientToken },
      signal: AbortSignal.timeout(VAULT_TIMEOUT),
    });

    if (!secretRes.ok) {
      const body = await secretRes.text();
      return {
        success: false,
        error: `Vault secret read failed (${secretRes.status}): ${body}`,
      };
    }

    const secretData = await secretRes.json();
    const data = secretData?.data?.data || secretData?.data || {};

    return {
      success: true,
      credentials: {
        username: data.username || data.user || data.db_username || '',
        password: data.password || data.pass || data.db_password || data.secret || '',
        host: data.host || data.hostname || data.db_host || '',
        port: data.port || data.db_port || '',
        database: data.database || data.db_name || data.dbname || '',
        connection_string: data.connection_string || data.uri || data.dsn || '',
      },
      raw_keys: Object.keys(data),
    };
  } catch (err) {
    if (err.name === 'TimeoutError' || err.name === 'AbortError') {
      return {
        success: false,
        error: `Vault connection timed out after ${VAULT_TIMEOUT / 1000}s. Check the Vault URL and network connectivity.`,
      };
    }
    return {
      success: false,
      error: `Vault error: ${err.message}`,
    };
  }
}

export async function resolveCredentials(connectionData) {
  if (connectionData.auth_method !== 'vault_credentials') {
    return connectionData;
  }

  const vaultResult = await fetchVaultCredentials(connectionData.vault_config || {});

  if (!vaultResult.success) {
    throw new Error(`Vault credential resolution failed: ${vaultResult.error}`);
  }

  const creds = vaultResult.credentials;
  return {
    ...connectionData,
    username: creds.username || connectionData.username,
    password: creds.password || connectionData.password,
    host: creds.host || connectionData.host,
    port: creds.port || connectionData.port,
    database: creds.database || connectionData.database,
    connection_string: creds.connection_string || connectionData.connection_string,
    _vault_resolved: true,
  };
}
