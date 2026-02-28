import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Plus, Trash2, Shield, Eye, EyeOff } from "lucide-react";
import { cn } from "@/lib/utils";
import dataflowConfig from '@/dataflow-config';

const MASKING_ICON_MAP = { full: EyeOff, partial: Eye, hash: Shield, encrypt: Shield, tokenize: Shield, redact: EyeOff };
const maskingTypes = Object.fromEntries(
  (dataflowConfig.pipeline_wizard?.masking_types || []).map(m => [
    m.value,
    { label: m.label, description: m.description, icon: MASKING_ICON_MAP[m.value] || Shield }
  ])
);

const piiTypes = dataflowConfig.pipeline_wizard?.pii_types || [];

export default function DataMaskingConfig({ value = [], onChange }) {
  const [rules, setRules] = useState(value || []);

  const addRule = () => {
    const newRule = {
      column_name: "",
      masking_type: "full",
      pii_type: "custom",
      preserve_format: false,
      show_first: 0,
      show_last: 0,
    };
    const updated = [...rules, newRule];
    setRules(updated);
    onChange(updated);
  };

  const updateRule = (index, field, val) => {
    const updated = rules.map((rule, i) => 
      i === index ? { ...rule, [field]: val } : rule
    );
    setRules(updated);
    onChange(updated);
  };

  const removeRule = (index) => {
    const updated = rules.filter((_, i) => i !== index);
    setRules(updated);
    onChange(updated);
  };

  return (
    <Card className="dark:bg-slate-800 dark:border-slate-700">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Shield className="w-5 h-5 text-[#0060AF]" />
            <CardTitle>Data Masking & Encryption</CardTitle>
          </div>
          <Button onClick={addRule} size="sm" className="gap-2">
            <Plus className="w-4 h-4" />
            Add Rule
          </Button>
        </div>
        <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
          Configure PII handling and column-level encryption
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        {rules.length === 0 ? (
          <div className="text-center py-8 text-slate-500 dark:text-slate-400">
            <Shield className="w-12 h-12 mx-auto mb-3 text-slate-300 dark:text-slate-600" />
            <p>No masking rules configured</p>
            <p className="text-sm">Click "Add Rule" to protect sensitive data</p>
          </div>
        ) : (
          rules.map((rule, index) => {
            const MaskIcon = maskingTypes[rule.masking_type]?.icon || Shield;
            return (
              <Card key={index} className="border-slate-200 dark:border-slate-700">
                <CardContent className="pt-4 space-y-3">
                  <div className="flex items-start gap-3">
                    <div className={cn(
                      "w-10 h-10 rounded-lg flex items-center justify-center shrink-0",
                      "bg-blue-100 dark:bg-blue-900/40"
                    )}>
                      <MaskIcon className="w-5 h-5 text-[#0060AF] dark:text-blue-300" />
                    </div>
                    <div className="flex-1 space-y-3">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        <div>
                          <label className="text-xs text-slate-600 dark:text-slate-400 mb-1 block">
                            Column Name
                          </label>
                          <Input
                            placeholder="e.g., ssn, credit_card"
                            value={rule.column_name}
                            onChange={(e) => updateRule(index, "column_name", e.target.value)}
                            className="dark:bg-slate-700 dark:border-slate-600"
                          />
                        </div>
                        <div>
                          <label className="text-xs text-slate-600 dark:text-slate-400 mb-1 block">
                            PII Type
                          </label>
                          <Select
                            value={rule.pii_type}
                            onValueChange={(v) => updateRule(index, "pii_type", v)}
                          >
                            <SelectTrigger className="dark:bg-slate-700 dark:border-slate-600">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {piiTypes.map(type => (
                                <SelectItem key={type} value={type}>
                                  {type.replace(/_/g, " ").toUpperCase()}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                        <div>
                          <label className="text-xs text-slate-600 dark:text-slate-400 mb-1 block">
                            Masking Type
                          </label>
                          <Select
                            value={rule.masking_type}
                            onValueChange={(v) => updateRule(index, "masking_type", v)}
                          >
                            <SelectTrigger className="dark:bg-slate-700 dark:border-slate-600">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {Object.entries(maskingTypes).map(([key, config]) => (
                                <SelectItem key={key} value={key}>
                                  {config.label}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>

                        {rule.masking_type === "partial" && (
                          <>
                            <div>
                              <label className="text-xs text-slate-600 dark:text-slate-400 mb-1 block">
                                Show First N
                              </label>
                              <Input
                                type="number"
                                min="0"
                                value={rule.show_first}
                                onChange={(e) => updateRule(index, "show_first", parseInt(e.target.value))}
                                className="dark:bg-slate-700 dark:border-slate-600"
                              />
                            </div>
                            <div>
                              <label className="text-xs text-slate-600 dark:text-slate-400 mb-1 block">
                                Show Last N
                              </label>
                              <Input
                                type="number"
                                min="0"
                                value={rule.show_last}
                                onChange={(e) => updateRule(index, "show_last", parseInt(e.target.value))}
                                className="dark:bg-slate-700 dark:border-slate-600"
                              />
                            </div>
                          </>
                        )}
                      </div>

                      <div className="flex items-center justify-between">
                        <Badge variant="outline" className="text-xs dark:border-slate-600">
                          {maskingTypes[rule.masking_type]?.description}
                        </Badge>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => removeRule(index)}
                          className="text-red-600 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-900/20"
                        >
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            );
          })
        )}
      </CardContent>
    </Card>
  );
}