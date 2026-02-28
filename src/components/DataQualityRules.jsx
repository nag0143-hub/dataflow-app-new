import { useState, useMemo, useEffect } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Search, Plus, X, ShieldCheck, TableProperties, Columns, ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import dataflowConfig from '@/dataflow-config';

const dqConfig = dataflowConfig.pipeline_wizard?.dq_rules || {};
const COLUMN_RULES = dqConfig.column_rules_extended || [];
const DATASET_RULES = dqConfig.dataset_rules || [];
const ACTIONS = dqConfig.actions || [];

const PAGE_SIZE = 50;

export default function DataQualityRules({ selectedObjects = [], rules = {}, onChange }) {
  const [selectedTable, setSelectedTable] = useState(selectedObjects[0] ? `${selectedObjects[0].schema}.${selectedObjects[0].table}` : "");
  const [colSearch, setColSearch] = useState("");
  const [colPage, setColPage] = useState(0);

  useEffect(() => {
    if (selectedObjects.length > 0) {
      const newKey = `${selectedObjects[0].schema}.${selectedObjects[0].table}`;
      if (newKey !== selectedTable) {
        setSelectedTable(newKey);
        setColSearch("");
        setColPage(0);
      }
    }
  }, [selectedObjects]);
  const [addColRule, setAddColRule] = useState(null); // {col}
  const [addDatasetRule, setAddDatasetRule] = useState(false);
  const [newRule, setNewRule] = useState({ rule: "not_null", parameter: "", action: "fail_job" });

  const tableKey = selectedTable;
  const selectedObj = useMemo(() => selectedObjects.find(o => `${o.schema}.${o.table}` === selectedTable), [selectedObjects, selectedTable]);
  
  const tableColumns = useMemo(() => 
    selectedObj?.columns?.map(c => c.name) || [], 
    [selectedObj]
  );

  const columnRules = useMemo(() => rules[tableKey]?.column || [], [rules, tableKey]);
  const datasetRules = useMemo(() => rules[tableKey]?.dataset || [], [rules, tableKey]);

  const filteredCols = useMemo(() =>
    tableColumns.filter(c => c.toLowerCase().includes(colSearch.toLowerCase())),
    [tableColumns, colSearch]
  );
  const totalColPages = Math.ceil(filteredCols.length / PAGE_SIZE);
  const pageCols = filteredCols.slice(colPage * PAGE_SIZE, (colPage + 1) * PAGE_SIZE);

  const colsWithRules = useMemo(() => new Set(columnRules.map(r => r.column)), [columnRules]);

  const saveColumnRule = () => {
    const updated = [...columnRules, { column: addColRule, ...newRule }];
    onChange({ ...rules, [tableKey]: { ...rules[tableKey], column: updated } });
    setAddColRule(null);
    setNewRule({ rule: "not_null", parameter: "", action: "fail_job" });
  };

  const saveDatasetRule = () => {
    const updated = [...datasetRules, { ...newRule }];
    onChange({ ...rules, [tableKey]: { ...rules[tableKey], dataset: updated } });
    setAddDatasetRule(false);
    setNewRule({ rule: "row_count_min", parameter: "", action: "fail_job" });
  };

  const removeColRule = (idx) => {
    const updated = columnRules.filter((_, i) => i !== idx);
    onChange({ ...rules, [tableKey]: { ...rules[tableKey], column: updated } });
  };

  const removeDsRule = (idx) => {
    const updated = datasetRules.filter((_, i) => i !== idx);
    onChange({ ...rules, [tableKey]: { ...rules[tableKey], dataset: updated } });
  };

  if (selectedObjects.length === 0) {
    return (
      <div className="text-center py-10 text-slate-400 text-sm border border-dashed border-slate-200 rounded-xl">
        No tables selected. Add tables in the <strong>Objects</strong> tab first.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Table selector */}
      <div>
        <Label className="text-xs mb-1 block">Table</Label>
        <Select value={selectedTable} onValueChange={v => { setSelectedTable(v); setColSearch(""); setColPage(0); }}>
          <SelectTrigger>
            <SelectValue placeholder="Select a table" />
          </SelectTrigger>
          <SelectContent>
            {selectedObjects.map(obj => (
              <SelectItem key={`${obj.schema}.${obj.table}`} value={`${obj.schema}.${obj.table}`}>
                {obj.schema}.{obj.table}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {selectedTable && (
        <>
          {/* ── Dataset-level rules ── */}
          <div className="border border-slate-200 rounded-xl overflow-hidden">
            <div className="flex items-center justify-between px-3 py-2 bg-blue-50 dark:bg-blue-900/20 border-b border-blue-100 dark:border-blue-800">
              <div className="flex items-center gap-1.5">
                <TableProperties className="w-3.5 h-3.5 text-[#0060AF] dark:text-blue-400" />
                <span className="text-xs font-semibold text-[#0060AF] dark:text-blue-300">Dataset-Level Rules ({datasetRules.length})</span>
              </div>
              <Button type="button" variant="ghost" size="sm" className="h-6 text-xs px-2 text-[#0060AF] hover:text-[#004d8c] dark:text-blue-400 dark:hover:text-blue-300"
                onClick={() => { setAddDatasetRule(true); setNewRule({ rule: "row_count_min", parameter: "", action: "fail_job" }); }}>
                <Plus className="w-3 h-3 mr-1" /> Add Rule
              </Button>
            </div>

            {addDatasetRule && (
              <div className="px-3 py-2 bg-blue-50/50 dark:bg-blue-900/10 border-b border-blue-100 dark:border-blue-800 grid grid-cols-[1fr_1fr_1fr_auto] gap-2 items-end">
                <div>
                  <Label className="text-xs">Rule</Label>
                  <Select value={newRule.rule} onValueChange={v => setNewRule(r => ({ ...r, rule: v }))}>
                    <SelectTrigger className="h-7 text-xs"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {DATASET_RULES.map(r => <SelectItem key={r.value} value={r.value} className="text-xs">{r.label}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label className="text-xs">Parameter</Label>
                  <Input value={newRule.parameter} onChange={e => setNewRule(r => ({ ...r, parameter: e.target.value }))} className="h-7 text-xs" placeholder="e.g. 1000" />
                </div>
                <div>
                  <Label className="text-xs">On Failure</Label>
                  <Select value={newRule.action} onValueChange={v => setNewRule(r => ({ ...r, action: v }))}>
                    <SelectTrigger className="h-7 text-xs"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {ACTIONS.map(a => <SelectItem key={a.value} value={a.value} className="text-xs">{a.label}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex gap-1">
                  <Button type="button" size="sm" className="h-7 text-xs px-2" onClick={saveDatasetRule}>Save</Button>
                  <Button type="button" variant="ghost" size="sm" className="h-7 text-xs px-2" onClick={() => setAddDatasetRule(false)}>Cancel</Button>
                </div>
              </div>
            )}

            {datasetRules.length > 0 ? (
              <div className="divide-y divide-slate-50">
                <div className="grid grid-cols-[1fr_1fr_1fr_28px] gap-2 px-3 py-1.5 text-xs font-medium text-slate-500 bg-slate-50 sticky top-0">
                  <span>Rule</span><span>Parameter</span><span>On Failure</span><span></span>
                </div>
                {datasetRules.map((r, i) => (
                  <div key={i} className="grid grid-cols-[1fr_1fr_1fr_28px] gap-2 items-center px-3 py-1.5 text-xs">
                    <span className="text-slate-700">{DATASET_RULES.find(x => x.value === r.rule)?.label || r.rule}</span>
                    <span className="text-[#0060AF] dark:text-blue-400 font-mono">{r.parameter || "—"}</span>
                    <span className={cn("font-medium", r.action === "fail_job" ? "text-red-600" : r.action === "warn_continue" ? "text-amber-600" : "text-orange-600")}>
                      {ACTIONS.find(a => a.value === r.action)?.label}
                    </span>
                    <button onClick={() => removeDsRule(i)} className="text-slate-300 hover:text-red-400"><X className="w-3.5 h-3.5" /></button>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-xs text-slate-400 text-center py-4">No dataset rules configured</p>
            )}
          </div>

          {/* ── Column-level rules ── */}
          <div className="border border-slate-200 rounded-xl overflow-hidden">
            <div className="flex items-center justify-between px-3 py-2 bg-blue-50 border-b border-blue-100">
              <div className="flex items-center gap-1.5">
                <Columns className="w-3.5 h-3.5 text-blue-600" />
                <span className="text-xs font-semibold text-blue-700">Column-Level Rules ({columnRules.length})</span>
              </div>
            </div>

            {/* Existing column rules */}
            {columnRules.length > 0 && (
              <div className="border-b border-slate-100">
                <div className="grid grid-cols-[1fr_1fr_1fr_1fr_28px] gap-2 px-3 py-1.5 text-xs font-medium text-slate-500 bg-slate-50 sticky top-0">
                  <span>Column</span><span>Rule</span><span>Parameter</span><span>On Failure</span><span></span>
                </div>
                {columnRules.map((r, i) => (
                  <div key={i} className="grid grid-cols-[1fr_1fr_1fr_1fr_28px] gap-2 items-center px-3 py-1.5 text-xs border-t border-slate-50">
                    <span className="font-mono text-blue-700 truncate" title={r.column}>{r.column}</span>
                    <span className="text-slate-700">{COLUMN_RULES.find(x => x.value === r.rule)?.label || r.rule}</span>
                    <span className="text-slate-500 font-mono truncate">{r.parameter || "—"}</span>
                    <span className={cn("font-medium", r.action === "fail_job" ? "text-red-600" : r.action === "warn_continue" ? "text-amber-600" : "text-orange-600")}>
                      {ACTIONS.find(a => a.value === r.action)?.label}
                    </span>
                    <button onClick={() => removeColRule(i)} className="text-slate-300 hover:text-red-400"><X className="w-3.5 h-3.5" /></button>
                  </div>
                ))}
              </div>
            )}

            {/* Add rule inline form */}
            {addColRule && (
              <div className="px-3 py-2 bg-blue-50/50 border-b border-blue-100">
                <p className="text-xs font-semibold text-blue-700 mb-2">Adding rule for: <span className="font-mono">{addColRule}</span></p>
                <div className="grid grid-cols-[1fr_1fr_1fr_auto] gap-2 items-end">
                  <div>
                    <Label className="text-xs">Rule</Label>
                    <Select value={newRule.rule} onValueChange={v => setNewRule(r => ({ ...r, rule: v }))}>
                      <SelectTrigger className="h-7 text-xs"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        {COLUMN_RULES.map(r => <SelectItem key={r.value} value={r.value} className="text-xs">{r.label}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label className="text-xs">Parameter</Label>
                    <Input value={newRule.parameter} onChange={e => setNewRule(r => ({ ...r, parameter: e.target.value }))} className="h-7 text-xs" placeholder="e.g. ^[a-z]+$" />
                  </div>
                  <div>
                    <Label className="text-xs">On Failure</Label>
                    <Select value={newRule.action} onValueChange={v => setNewRule(r => ({ ...r, action: v }))}>
                      <SelectTrigger className="h-7 text-xs"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        {ACTIONS.map(a => <SelectItem key={a.value} value={a.value} className="text-xs">{a.label}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="flex gap-1">
                    <Button type="button" size="sm" className="h-7 text-xs px-2" onClick={saveColumnRule}>Save</Button>
                    <Button type="button" variant="ghost" size="sm" className="h-7 text-xs px-2" onClick={() => setAddColRule(null)}>Cancel</Button>
                  </div>
                </div>
              </div>
            )}

            {/* Column browser */}
            <div className="bg-slate-50 px-3 py-2 flex items-center gap-2 border-t border-slate-100">
              <div className="relative flex-1">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400" />
                <Input
                  placeholder={`Search ${tableColumns.length.toLocaleString()} columns...`}
                  value={colSearch}
                  onChange={e => { setColSearch(e.target.value); setColPage(0); }}
                  className="pl-7 h-7 text-xs"
                />
              </div>
              <span className="text-xs text-slate-400 shrink-0">{filteredCols.length.toLocaleString()} found</span>
            </div>
            <div className="max-h-52 overflow-y-auto">
              <div className="grid grid-cols-[1fr_80px] gap-2 px-3 py-1.5 text-xs font-medium text-slate-500 bg-white sticky top-0 border-b border-slate-100">
                <span>Column</span><span></span>
              </div>
              {pageCols.map(col => (
                <div key={col} className={cn("grid grid-cols-[1fr_80px] gap-2 items-center px-3 py-1.5 border-b border-slate-50 hover:bg-slate-50 text-xs", colsWithRules.has(col) && "bg-blue-50/40")}>
                  <span className={cn("font-mono truncate", colsWithRules.has(col) ? "text-blue-600" : "text-slate-700")} title={col}>{col}</span>
                  <Button
                    variant="ghost" size="sm"
                    className="h-6 text-xs px-2 text-blue-500 hover:text-blue-700"
                    type="button"
                    onClick={() => { setAddColRule(col); setNewRule({ rule: "not_null", parameter: "", action: "fail_job" }); }}
                  >
                    <Plus className="w-3 h-3 mr-1" /> Rule
                  </Button>
                </div>
              ))}
            </div>
            {totalColPages > 1 && (
              <div className="flex items-center justify-between px-3 py-2 border-t border-slate-100 bg-slate-50 text-xs text-slate-500">
                <span>Page {colPage + 1} of {totalColPages}</span>
                <div className="flex gap-1">
                  <Button variant="ghost" size="icon" className="h-6 w-6" disabled={colPage === 0} onClick={() => setColPage(p => p - 1)}><ChevronLeft className="w-3.5 h-3.5" /></Button>
                  <Button variant="ghost" size="icon" className="h-6 w-6" disabled={colPage >= totalColPages - 1} onClick={() => setColPage(p => p + 1)}><ChevronRight className="w-3.5 h-3.5" /></Button>
                </div>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}