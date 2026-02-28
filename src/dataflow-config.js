import yaml from 'js-yaml';
import mainYaml from '../dataflow.yaml?raw';
import wizardYaml from '../config/pipeline-wizard.yaml?raw';
import platformsYaml from '../config/platforms.yaml?raw';

function deepMerge(target, source) {
  const result = { ...target };
  for (const key of Object.keys(source)) {
    if (
      source[key] &&
      typeof source[key] === 'object' &&
      !Array.isArray(source[key]) &&
      target[key] &&
      typeof target[key] === 'object' &&
      !Array.isArray(target[key])
    ) {
      result[key] = deepMerge(target[key], source[key]);
    } else {
      result[key] = source[key];
    }
  }
  return result;
}

function loadConfig() {
  const main = yaml.load(mainYaml);
  const wizard = yaml.load(wizardYaml);
  const platforms = yaml.load(platformsYaml);

  const { environments, ...defaults } = main;

  defaults.pipeline_wizard = wizard;
  defaults.platforms = platforms;

  const env = import.meta.env.VITE_DATAFLOW_ENV || import.meta.env.MODE || 'development';
  const envOverrides = environments?.[env] || {};
  return deepMerge(defaults, envOverrides);
}

const dataflowConfig = loadConfig();

export default dataflowConfig;
