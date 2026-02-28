import dataflowConfig from '@/dataflow-config';

const wiz = dataflowConfig.pipeline_wizard || {};

export const GLOBAL_RULES = (wiz.global_rules || []).map(r => ({
  ...r,
  pattern: r.pattern ? new RegExp(r.pattern, 'i') : undefined,
}));

export const DQ_RULES = wiz.dq_rules?.summary_rules || [];

export const ENCRYPTION_TYPES = wiz.encryption_types || [];

export const TRANSFORMATIONS = wiz.transformations || [];

const paramsDef = wiz.transformation_params || {};
export const TRANSFORMATION_PARAMS = Object.fromEntries(
  Object.entries(paramsDef).map(([key, fields]) => [key, { fields }])
);

export const COLUMN_DQ_RULES = wiz.dq_rules?.column_rules || [];

export const PAGE_SIZE = 50;
export const MAPPING_PAGE_SIZE = 50;

export const DATA_TYPES = wiz.data_types || [];
