const SENSOR_FUNCTIONS = {
  adls2: {
    name: 'adls_file_exists',
    body: `def adls_file_exists(**kwargs):
    """Check if a file exists in Azure ADLS Gen2."""
    from azure.storage.filedatalake import DataLakeServiceClient
    import os

    account_name = os.environ.get('ADLS_ACCOUNT_NAME', Variable.get('adls_account_name', default_var=''))
    account_key = os.environ.get('ADLS_ACCOUNT_KEY', Variable.get('adls_account_key', default_var=''))
    container = os.environ.get('ADLS_CONTAINER', Variable.get('adls_container', default_var='landing'))
    watch_path = os.environ.get('SOURCE_WATCH_PATH', Variable.get('source_watch_path', default_var='/inbound/'))

    if not account_name or not account_key:
        logger.warning("ADLS credentials not configured, skipping sensor check")
        return False

    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key
        )
        fs_client = service_client.get_file_system_client(container)
        paths = list(fs_client.get_paths(path=watch_path, recursive=False))
        found = len(paths) > 0
        logger.info(f"ADLS sensor check: container={container}, path={watch_path}, found={found}")
        return found
    except Exception as e:
        logger.error(f"ADLS sensor error: {e}")
        return False`,
  },

  s3: {
    name: 's3_file_exists',
    body: `def s3_file_exists(**kwargs):
    """Check if a file exists in AWS S3."""
    import boto3
    import os

    bucket = os.environ.get('S3_BUCKET', Variable.get('s3_bucket', default_var=''))
    prefix = os.environ.get('S3_PREFIX', Variable.get('s3_prefix', default_var='inbound/'))
    region = os.environ.get('AWS_REGION', Variable.get('aws_region', default_var='us-east-1'))

    if not bucket:
        logger.warning("S3 bucket not configured, skipping sensor check")
        return False

    try:
        s3 = boto3.client('s3', region_name=region)
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        found = response.get('KeyCount', 0) > 0
        logger.info(f"S3 sensor check: bucket={bucket}, prefix={prefix}, found={found}")
        return found
    except Exception as e:
        logger.error(f"S3 sensor error: {e}")
        return False`,
  },

  sftp: {
    name: 'sftp_file_exists',
    body: `def sftp_file_exists(**kwargs):
    """Check if a file exists on an SFTP server."""
    import paramiko
    import os

    host = os.environ.get('SFTP_HOST', Variable.get('sftp_host', default_var=''))
    port = int(os.environ.get('SFTP_PORT', Variable.get('sftp_port', default_var='22')))
    username = os.environ.get('SFTP_USERNAME', Variable.get('sftp_username', default_var=''))
    password = os.environ.get('SFTP_PASSWORD', Variable.get('sftp_password', default_var=''))
    key_path = os.environ.get('SFTP_KEY_PATH', Variable.get('sftp_key_path', default_var=''))
    watch_path = os.environ.get('SOURCE_WATCH_PATH', Variable.get('source_watch_path', default_var='/inbound/'))

    if not host or not username:
        logger.warning("SFTP credentials not configured, skipping sensor check")
        return False

    try:
        transport = paramiko.Transport((host, port))
        if key_path:
            pkey = paramiko.RSAKey.from_private_key_file(key_path)
            transport.connect(username=username, pkey=pkey)
        else:
            transport.connect(username=username, password=password)

        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            entries = sftp.listdir(watch_path)
            found = len(entries) > 0
            logger.info(f"SFTP sensor check: host={host}, path={watch_path}, found={found}")
            return found
        finally:
            sftp.close()
            transport.close()
    except Exception as e:
        logger.error(f"SFTP sensor error: {e}")
        return False`,
  },

  local_fs: {
    name: 'local_file_exists',
    body: `def local_file_exists(**kwargs):
    """Check if a file exists on the local filesystem."""
    import glob as glob_module
    import os

    watch_path = os.environ.get('SOURCE_WATCH_PATH', Variable.get('source_watch_path', default_var='/data/inbound/*'))

    try:
        if '*' in watch_path or '?' in watch_path:
            matches = glob_module.glob(watch_path)
            found = len(matches) > 0
        else:
            found = os.path.exists(watch_path)
        logger.info(f"Local filesystem sensor check: path={watch_path}, found={found}")
        return found
    except Exception as e:
        logger.error(f"Local filesystem sensor error: {e}")
        return False`,
  },

  nas: {
    name: 'nas_file_exists',
    body: `def nas_file_exists(**kwargs):
    """Check if a file exists on a NAS / network share."""
    import glob as glob_module
    import os

    watch_path = os.environ.get('SOURCE_WATCH_PATH', Variable.get('source_watch_path', default_var='/mnt/nas/inbound/*'))

    try:
        if '*' in watch_path or '?' in watch_path:
            matches = glob_module.glob(watch_path)
            found = len(matches) > 0
        else:
            found = os.path.exists(watch_path)
        logger.info(f"NAS sensor check: path={watch_path}, found={found}")
        return found
    except Exception as e:
        logger.error(f"NAS sensor error: {e}")
        return False`,
  },
};

const PLATFORM_TO_SENSOR = {
  adls2: 'adls2',
  s3: 's3',
  sftp: 'sftp',
  local_fs: 'local_fs',
  nas: 'nas',
  flat_file_delimited: 'adls2',
  flat_file_fixed_width: 'adls2',
  cobol_ebcdic: 'adls2',
};

function getSensorKey(platform) {
  return PLATFORM_TO_SENSOR[platform] || 'adls2';
}

function generateIngestFunction(sourceConn, targetConn) {
  const srcPlatform = sourceConn?.platform || 'local_fs';
  const tgtPlatform = targetConn?.platform || 'local_fs';

  const readBlock = generateReadBlock(srcPlatform);
  const writeBlock = generateWriteBlock(tgtPlatform);

  return `def ingest_function(source_path, dataset_key, load_method="append", target_path="", **kwargs):
    """
    Ingest flat files from source to target.

    Args:
        source_path: Path or pattern for source file(s)
        dataset_key: Unique dataset identifier
        load_method: One of 'append', 'overwrite', 'upsert'
        target_path: Target location override
    """
    import os

    logger.info(f"Starting ingestion: dataset={dataset_key}, source={source_path}, method={load_method}")

${readBlock}

${writeBlock}

    logger.info(f"Ingestion complete: dataset={dataset_key}, records={len(df)}, method={load_method}")
    return {"dataset": dataset_key, "records": len(df), "load_method": load_method}`;
}

function generateReadBlock(srcPlatform) {
  if (srcPlatform === 'cobol_ebcdic') {
    return `    # Read EBCDIC/COBOL file
    import codecs

    file_path = source_path
    encoding = os.environ.get('EBCDIC_ENCODING', Variable.get('ebcdic_encoding', default_var='cp500'))

    if '*' in file_path or '?' in file_path:
        import glob as glob_module
        files = sorted(glob_module.glob(file_path))
        if not files:
            raise FileNotFoundError(f"No files matched pattern: {file_path}")
        file_path = files[0]
        logger.info(f"Matched {len(files)} file(s), processing first: {file_path}")

    logger.info(f"Reading EBCDIC file: {file_path} with encoding {encoding}")
    with codecs.open(file_path, 'r', encoding=encoding) as f:
        lines = f.readlines()
    import pandas as pd
    df = pd.DataFrame({'raw_line': [line.rstrip('\\n') for line in lines]})
    logger.info(f"Read {len(df)} records from EBCDIC file")`;
  }

  if (srcPlatform === 'flat_file_fixed_width') {
    return `    # Read fixed-width file
    import pandas as pd

    file_path = source_path
    if '*' in file_path or '?' in file_path:
        import glob as glob_module
        files = sorted(glob_module.glob(file_path))
        if not files:
            raise FileNotFoundError(f"No files matched pattern: {file_path}")
        file_path = files[0]
        logger.info(f"Matched {len(files)} file(s), processing first: {file_path}")

    colspecs_str = os.environ.get('FIXED_WIDTH_COLSPECS', Variable.get('fixed_width_colspecs', default_var=''))
    if colspecs_str:
        import json
        colspecs = json.loads(colspecs_str)
        df = pd.read_fwf(file_path, colspecs=colspecs)
    else:
        df = pd.read_fwf(file_path)
    logger.info(f"Read {len(df)} records from fixed-width file: {file_path}")`;
  }

  return `    # Read delimited file (CSV/TSV)
    import pandas as pd

    file_path = source_path
    delimiter = os.environ.get('FILE_DELIMITER', Variable.get('file_delimiter', default_var=','))
    encoding = os.environ.get('FILE_ENCODING', Variable.get('file_encoding', default_var='utf-8'))

    if '*' in file_path or '?' in file_path:
        import glob as glob_module
        files = sorted(glob_module.glob(file_path))
        if not files:
            raise FileNotFoundError(f"No files matched pattern: {file_path}")
        file_path = files[0]
        logger.info(f"Matched {len(files)} file(s), processing first: {file_path}")

    logger.info(f"Reading CSV file: {file_path}")
    df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)
    logger.info(f"Read {len(df)} records from: {file_path}")`;
}

function generateWriteBlock(tgtPlatform) {
  if (tgtPlatform === 'adls2') {
    return `    # Write to Azure ADLS Gen2
    from azure.storage.filedatalake import DataLakeServiceClient
    import io

    account_name = os.environ.get('ADLS_ACCOUNT_NAME', Variable.get('adls_account_name', default_var=''))
    account_key = os.environ.get('ADLS_ACCOUNT_KEY', Variable.get('adls_account_key', default_var=''))
    container = os.environ.get('ADLS_TARGET_CONTAINER', Variable.get('adls_target_container', default_var='raw'))

    dest_path = target_path if target_path else f"raw/{dataset_key}"
    if load_method == 'overwrite':
        filename = f"{dest_path}/{dataset_key}.parquet"
    else:
        from datetime import datetime as dt
        ts = dt.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{dest_path}/{dataset_key}_{ts}.parquet"

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )
    fs_client = service_client.get_file_system_client(container)
    file_client = fs_client.get_file_client(filename)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    file_client.upload_data(buffer.read(), overwrite=(load_method == 'overwrite'))
    logger.info(f"Uploaded to ADLS: {container}/{filename}")`;
  }

  if (tgtPlatform === 's3') {
    return `    # Write to AWS S3
    import boto3
    import io

    bucket = os.environ.get('S3_TARGET_BUCKET', Variable.get('s3_target_bucket', default_var=''))
    region = os.environ.get('AWS_REGION', Variable.get('aws_region', default_var='us-east-1'))

    dest_path = target_path if target_path else f"raw/{dataset_key}"
    if load_method == 'overwrite':
        filename = f"{dest_path}/{dataset_key}.parquet"
    else:
        from datetime import datetime as dt
        ts = dt.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{dest_path}/{dataset_key}_{ts}.parquet"

    s3 = boto3.client('s3', region_name=region)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, bucket, filename)
    logger.info(f"Uploaded to S3: s3://{bucket}/{filename}")`;
  }

  return `    # Write to local filesystem
    import shutil
    from pathlib import Path

    dest_dir = target_path if target_path else f"/data/output/{dataset_key}"
    Path(dest_dir).mkdir(parents=True, exist_ok=True)

    if load_method == 'overwrite':
        output_file = os.path.join(dest_dir, f"{dataset_key}.parquet")
    else:
        from datetime import datetime as dt
        ts = dt.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(dest_dir, f"{dataset_key}_{ts}.parquet")

    df.to_parquet(output_file, index=False)
    logger.info(f"Written to local: {output_file}")`;
}

function generatePythonTasksScript(pipeline, sourceConn, targetConn) {
  const pipelineName = (pipeline.name || pipeline || 'pipeline')
    .toString()
    .replace(/[^a-z0-9_]/gi, '_')
    .toLowerCase();

  const srcPlatform = sourceConn?.platform || 'local_fs';
  const tgtPlatform = targetConn?.platform || 'local_fs';
  const sensorKey = getSensorKey(srcPlatform);
  const sensorFunc = SENSOR_FUNCTIONS[sensorKey];

  const neededSensors = new Set([sensorKey]);
  const allSensorKeys = Object.keys(SENSOR_FUNCTIONS);
  const sensorBlocks = allSensorKeys
    .filter(k => neededSensors.has(k))
    .map(k => SENSOR_FUNCTIONS[k].body);

  const additionalSensors = allSensorKeys
    .filter(k => !neededSensors.has(k))
    .map(k => SENSOR_FUNCTIONS[k]);

  const ingestBlock = generateIngestFunction(sourceConn, targetConn);

  const lines = [];
  lines.push(`"""
Pipeline tasks for: ${pipelineName}
Auto-generated by DataFlow

Source: ${sourceConn?.name || srcPlatform} (${srcPlatform})
Target: ${targetConn?.name || tgtPlatform} (${tgtPlatform})

Required packages:
  - pandas
${srcPlatform === 'adls2' || tgtPlatform === 'adls2' ? '  - azure-storage-file-datalake\n' : ''}${srcPlatform === 's3' || tgtPlatform === 's3' ? '  - boto3\n' : ''}${srcPlatform === 'sftp' ? '  - paramiko\n' : ''}"""

import logging
from airflow.models import Variable

logger = logging.getLogger("airflow.task.${pipelineName}")
`);

  lines.push('');
  lines.push(`# --- Sensor callable: ${sensorFunc.name} ---`);
  lines.push('');
  lines.push(sensorFunc.body);

  for (const extra of additionalSensors) {
    lines.push('');
    lines.push('');
    lines.push(`# --- Sensor callable: ${extra.name} ---`);
    lines.push('');
    lines.push(extra.body);
  }

  lines.push('');
  lines.push('');
  lines.push('# --- Ingest function ---');
  lines.push('');
  lines.push(ingestBlock);
  lines.push('');

  return lines.join('\n');
}

export { generatePythonTasksScript };
