function generateBootstrapDagLoader(subdir) {
  const scanDir = subdir || "dataflow";

  return `"""
Bootstrap DAG loader for dag-factory.
Scans YAML files in the dags/${scanDir}/ directory and generates DAGs via dag-factory.
Deploy this file to the root of your Airflow dags folder.

Prerequisite: pip install dag-factory>=1.0.0
"""
import os
import glob
import logging

try:
    from dagfactory import load_yaml_dags
except ImportError:
    raise ImportError(
        "dag-factory is required but not installed. "
        "Install it with: pip install dag-factory>=1.0.0"
    )

logger = logging.getLogger("dataflow.bootstrap")

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_YAML_DIR = os.path.join(_THIS_DIR, "${scanDir}")

if os.path.isdir(_YAML_DIR):
    _yaml_files = sorted(glob.glob(os.path.join(_YAML_DIR, "*.yaml"))) + \\
                  sorted(glob.glob(os.path.join(_YAML_DIR, "*.yml")))
    logger.info("dataflow bootstrap: found %d YAML files in %s", len(_yaml_files), _YAML_DIR)
    load_yaml_dags(globals_dict=globals(), dags_folder=_YAML_DIR)
else:
    logger.warning("dataflow bootstrap: YAML directory not found: %s", _YAML_DIR)
`;
}

function generateMappingFiles(pipeline, columnMappings) {
  if (!columnMappings || typeof columnMappings !== "object") return [];

  const pipelineClean = (pipeline || "pipeline")
    .replace(/[^a-z0-9_]/gi, "_")
    .toLowerCase();

  const results = [];

  for (const [datasetKey, mappings] of Object.entries(columnMappings)) {
    if (!Array.isArray(mappings) || mappings.length === 0) continue;

    const dsKey = datasetKey.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
    const filename = `${pipelineClean}_${dsKey}_mapping.json`;

    const columns = mappings.map((m) => {
      const entry = {
        source_column: m.source || null,
        target_column: m.target || m.source || null,
        transformation: m.transformation || "direct",
      };

      if (m.sourceDataType) entry.source_data_type = m.sourceDataType;
      if (m.sourceLength) entry.source_length = m.sourceLength;
      if (m.targetDataType) entry.target_data_type = m.targetDataType;
      if (m.targetLength) entry.target_length = m.targetLength;
      if (m.derived) entry.derived = true;
      if (m.is_audit) entry.is_audit = true;
      if (m.expression) entry.expression = m.expression;
      if (m.params && Object.keys(m.params).length > 0) {
        entry.transformation_params = m.params;
      }

      if (m.dq_rules && Array.isArray(m.dq_rules) && m.dq_rules.length > 0) {
        entry.dq_rules = m.dq_rules.map((rule) => {
          if (typeof rule === "string") return { rule_type: rule };
          return {
            rule_type: rule.rule_type || rule.type || rule.value,
            ...(rule.params || {}),
          };
        });
      }

      if (m.encryption_type || m.masking_type) {
        entry.masking = {
          type: m.encryption_type || m.masking_type,
        };
        if (m.masking_params && typeof m.masking_params === "object") {
          Object.assign(entry.masking, m.masking_params);
        }
      }

      return entry;
    });

    const content = JSON.stringify(
      {
        pipeline: pipelineClean,
        dataset: datasetKey,
        generated: new Date().toISOString(),
        columns,
      },
      null,
      2
    );

    results.push({ filename, content });
  }

  return results;
}

export { generateBootstrapDagLoader, generateMappingFiles };
