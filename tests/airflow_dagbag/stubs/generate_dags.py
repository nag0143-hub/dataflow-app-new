"""
Bootstrap DAG loader for dag-factory.
Scans YAML files in the dags/dataflow/ directory and generates DAGs via dag-factory.
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
_YAML_DIR = os.path.join(_THIS_DIR, "dataflow")

if os.path.isdir(_YAML_DIR):
    _yaml_files = sorted(glob.glob(os.path.join(_YAML_DIR, "*.yaml"))) + \
                  sorted(glob.glob(os.path.join(_YAML_DIR, "*.yml")))
    logger.info("dataflow bootstrap: found %d YAML files in %s", len(_yaml_files), _YAML_DIR)
    load_yaml_dags(globals_dict=globals(), dags_folder=_YAML_DIR)
else:
    logger.warning("dataflow bootstrap: YAML directory not found: %s", _YAML_DIR)
