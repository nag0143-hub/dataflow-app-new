import dataflowConfig from './dataflow-config';

function toFlag(item) {
  return {
    enabled: item?.enabled !== false,
    comingSoon: item?.coming_soon === true,
  };
}

const nav = dataflowConfig.navigation || {};
const feat = dataflowConfig.features || {};

const featureFlags = {
  dashboard:        toFlag(nav.main?.dashboard),
  connections:      toFlag(nav.main?.connections),
  pipelines:        toFlag(nav.main?.pipelines),
  dataCatalog:      toFlag(nav.main?.data_catalog),
  userGuide:        toFlag(nav.main?.user_guide),

  dataModel:        toFlag(nav.admin?.data_model),
  auditTrail:       toFlag(nav.admin?.audit_trail),
  activityLogs:     toFlag(nav.admin?.activity_logs),
  airflow:          toFlag(nav.admin?.airflow),
  airflowConsole:   toFlag(nav.admin?.airflow_console),
  customFunctions:  toFlag(nav.admin?.custom_functions),

  vaultCredentials:         toFlag(feat.vault_credentials),
  gitlabDeploy:             toFlag(feat.gitlab_deploy),
  orchestrationPanel:       toFlag(feat.orchestration_panel),
  advancedPipelineFeatures: toFlag(feat.advanced_features),
  pipelineListView:         toFlag(feat.pipeline_list_view),
  schemaIntrospection:      toFlag(feat.schema_introspection),
  connectionTesting:        toFlag(feat.connection_testing),
  dagCheckin:               toFlag(feat.dag_checkin),
  specValidation:           toFlag(feat.spec_validation),
  airflowStatusSync:        toFlag(feat.airflow_status_sync),
};

export default featureFlags;
