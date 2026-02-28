import { useState, useEffect } from "react";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Card } from "@/components/ui/card";
import { ShieldCheck, Zap, Lock, FileText, Shield, Columns } from "lucide-react";
import ColumnMapper from "@/components/ColumnMapper";
import DataQualityRules from "@/components/DataQualityRules";
import DataCleansing from "@/components/DataCleansing";
import DataMaskingConfig from "@/components/DataMaskingConfig";
import SLAConfig from "@/components/SLAConfig";
import { cn } from "@/lib/utils";
import dataflowConfig from "@/dataflow-config";

const ICON_MAP = { column_mapping: Columns, data_cleansing: Zap, data_quality: FileText, security: Shield, sla: ShieldCheck };
const COLOR_MAP = { column_mapping: "text-[#0060AF]", data_cleansing: "text-amber-600", data_quality: "text-emerald-600", security: "text-[#0060AF]", sla: "text-blue-600" };
const ID_MAP = { column_mapping: "column_mapping", data_cleansing: "cleansing", data_quality: "quality", security: "security", sla: "sla" };

const advCfg = dataflowConfig.pipeline_wizard?.advanced_features || {};
const ALL_NAV_SECTIONS = Object.entries(advCfg)
  .filter(([, v]) => v.enabled !== false)
  .map(([key, v]) => ({
    id: ID_MAP[key] || key,
    featureKey: key,
    label: v.label?.replace(/ & Transformations$/, '').replace(/Rules$/, '').replace(/Configuration$/, '') || key,
    icon: ICON_MAP[key] || FileText,
    color: COLOR_MAP[key] || "text-slate-600",
  }));

export default function AdvancedTabContent({ formData, setFormData }) {
  const features = formData.advanced_features || {};
  const NAV_SECTIONS = ALL_NAV_SECTIONS.filter(s =>
    s.featureKey === "column_mapping" || features[s.featureKey]
  );
  const [activeSection, setActiveSection] = useState(() => NAV_SECTIONS[0]?.id || "column_mapping");

  useEffect(() => {
    if (!NAV_SECTIONS.some(s => s.id === activeSection)) {
      setActiveSection(NAV_SECTIONS[0]?.id || "column_mapping");
    }
  }, [formData.advanced_features]);

  const datasetKey = (d) => `${d.schema}.${d.table}`;
  const [activeDataset, setActiveDataset] = useState(
    formData.selected_datasets?.[0] ? datasetKey(formData.selected_datasets[0]) : ""
  );

  useEffect(() => {
    const datasets = formData.selected_datasets || [];
    if (datasets.length === 0) return;
    const stillValid = datasets.some(d => datasetKey(d) === activeDataset);
    if (!stillValid) {
      setActiveDataset(datasetKey(datasets[0]));
    }
  }, [formData.selected_datasets]);

  const currentDataset = formData.selected_datasets?.find(
    d => datasetKey(d) === activeDataset
  );

  const getDatasetIndex = () => {
    const idx = formData.selected_datasets?.findIndex(d => datasetKey(d) === activeDataset) ?? -1;
    return idx === -1 ? 0 : idx;
  };

  const updateDataset = (updates) => {
    const idx = getDatasetIndex();
    if (idx >= 0) {
      const updated = [...formData.selected_datasets];
      updated[idx] = { ...updated[idx], ...updates };
      setFormData({ ...formData, selected_datasets: updated });
    }
  };

  if (!formData.selected_datasets?.length) {
    return (
      <Card className="p-6 border-dashed border-2 text-center">
        <p className="text-slate-500 text-sm">Select datasets in the Data tab to configure rules</p>
      </Card>
    );
  }

  const ds = currentDataset || formData.selected_datasets[0];

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-1.5">
        {formData.selected_datasets.map((d) => {
          const key = datasetKey(d);
          const isActive = activeDataset === key;
          return (
            <button
              key={key}
              type="button"
              onClick={() => setActiveDataset(key)}
              className={cn(
                "px-3 py-1.5 rounded-md text-xs font-medium transition-all",
                isActive
                  ? "bg-[#0060AF] text-white shadow-sm shadow-[#0060AF]/20"
                  : "bg-[#0060AF]/10 text-[#0060AF] hover:bg-[#0060AF]/20 dark:bg-[#0060AF]/20 dark:text-blue-300 dark:hover:bg-[#0060AF]/30"
              )}
            >
              {d.table}
            </button>
          );
        })}
      </div>

      <div className="flex gap-3 min-h-[400px]">
        <nav className="w-[200px] shrink-0 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 p-2 space-y-1">
          <div className="px-2 py-1.5 mb-1">
            <p className="text-[10px] uppercase tracking-widest text-slate-400 dark:text-slate-500 font-semibold">Configuration</p>
          </div>
          {NAV_SECTIONS.map((section) => {
            const Icon = section.icon;
            const isActive = activeSection === section.id;
            return (
              <button
                key={section.id}
                type="button"
                onClick={() => setActiveSection(section.id)}
                className={cn(
                  "w-full flex items-center gap-2.5 px-3 py-2 rounded-md text-sm font-medium transition-all text-left",
                  isActive
                    ? "bg-[#0060AF] text-white shadow-sm shadow-[#0060AF]/20"
                    : "text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700 hover:text-slate-900 dark:hover:text-white"
                )}
              >
                <Icon className={cn("w-4 h-4 shrink-0", isActive ? "text-white" : section.color)} />
                <span className="truncate">{section.label}</span>
              </button>
            );
          })}

          <div className="border-t border-slate-200 dark:border-slate-700 mt-3 pt-3 px-2">
            <p className="text-[10px] text-slate-400 dark:text-slate-500">
              <span className="font-medium text-slate-500 dark:text-slate-400">{ds.schema}</span>.{ds.table}
            </p>
          </div>
        </nav>

        <div className="flex-1 min-w-0">
          {activeSection === "cleansing" && (
            <Card className="p-4 h-full">
              <div className="flex items-center gap-2 mb-3">
                <Zap className="w-4 h-4 text-amber-600" />
                <h4 className="font-semibold text-sm">Data Cleansing</h4>
              </div>
              <DataCleansing
                selectedObjects={[ds]}
                cleansing={formData.data_cleansing}
                onChange={(cleansing) => setFormData(prev => ({ ...prev, data_cleansing: cleansing }))}
              />
            </Card>
          )}

          {activeSection === "quality" && (
            <Card className="p-4 h-full">
              <div className="flex items-center gap-2 mb-3">
                <FileText className="w-4 h-4 text-emerald-600" />
                <h4 className="font-semibold text-sm">Data Quality Rules</h4>
              </div>
              <DataQualityRules
                selectedObjects={[ds]}
                rules={formData.dq_rules}
                onChange={(rules) => setFormData(prev => ({ ...prev, dq_rules: rules }))}
              />
            </Card>
          )}

          {activeSection === "column_mapping" && (
            <Card className="p-4 h-full">
              <div className="flex items-center gap-2 mb-3">
                <Columns className="w-4 h-4 text-[#0060AF]" />
                <h4 className="font-semibold text-sm">Column Mapping & Transformations</h4>
              </div>
              <ColumnMapper
                compact
                selectedObjects={[ds]}
                mappings={formData.column_mappings}
                connectionId={formData.source_connection_id}
                onChange={(mappingsOrUpdater) => {
                  setFormData(prev => ({
                    ...prev,
                    column_mappings: typeof mappingsOrUpdater === "function"
                      ? mappingsOrUpdater(prev.column_mappings)
                      : mappingsOrUpdater
                  }));
                }}
              />
            </Card>
          )}

          {activeSection === "security" && (
            <div className="space-y-4">
              <Card className="p-4">
                <div className="flex items-center gap-2 mb-3">
                  <Shield className="w-4 h-4 text-[#0060AF]" />
                  <h4 className="font-semibold text-sm">Encryption & Data Masking</h4>
                </div>
                <DataMaskingConfig
                  value={formData.data_masking_rules || []}
                  onChange={(rules) => setFormData({ ...formData, data_masking_rules: rules })}
                />
              </Card>
              <Card className="p-4 space-y-4">
                <div className="flex items-center gap-2">
                  <Lock className="w-4 h-4 text-blue-600" />
                  <h4 className="font-semibold text-sm">Classification & Access</h4>
                </div>
                <div>
                  <Label className="text-xs text-slate-600">Data Classification</Label>
                  <Select
                    value={ds.data_classification || ""}
                    onValueChange={(v) => updateDataset({ data_classification: v })}
                  >
                    <SelectTrigger className="h-8 text-xs mt-1">
                      <SelectValue placeholder="Select classification" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="public">Public</SelectItem>
                      <SelectItem value="confidential">Confidential</SelectItem>
                      <SelectItem value="personal_class_1">Personal Class 1</SelectItem>
                      <SelectItem value="personal_class_1_pci">Personal Class 1 - PCI</SelectItem>
                      <SelectItem value="personal_class_2">Personal Class 2</SelectItem>
                      <SelectItem value="personal_class_3">Personal Class 3</SelectItem>
                      <SelectItem value="personal_class_4">Personal Class 4</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label className="text-xs text-slate-600">Dataset Access Entitlements</Label>
                  <Input
                    value={ds.access_entitlements?.join(", ") || ""}
                    onChange={(e) => updateDataset({
                      access_entitlements: e.target.value.split(",").map(s => s.trim()).filter(s => s)
                    })}
                    placeholder="e.g. data_analyst, finance_user"
                    className="text-xs mt-1"
                  />
                  <p className="text-xs text-slate-400 mt-1">Leave empty to inherit pipeline-level entitlements</p>
                </div>
              </Card>
            </div>
          )}

          {activeSection === "sla" && (
            <Card className="p-4 h-full">
              <div className="flex items-center gap-2 mb-3">
                <ShieldCheck className="w-4 h-4 text-blue-600" />
                <h4 className="font-semibold text-sm">SLA Configuration</h4>
              </div>
              <SLAConfig
                value={ds.sla_config || {}}
                onChange={(sla) => updateDataset({ sla_config: sla })}
              />
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}
