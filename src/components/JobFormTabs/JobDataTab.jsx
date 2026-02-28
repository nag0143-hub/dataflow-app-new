import { useState } from "react";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import HelpTooltip from "@/components/HelpTooltip";
import ObjectSelector from "@/components/ObjectSelector";
import { ChevronDown, ChevronUp, Database, Filter, RefreshCw, FolderOutput, Settings2, Trash2, FileText, FolderSearch, Asterisk, Plus, X, FileSpreadsheet, FileJson, FileCode, AlertCircle, Table2 } from "lucide-react";
import dataflowConfig from '@/dataflow-config';
import { FLAT_FILE_PLATFORMS } from "@/components/JobSpecExport";

const LOAD_METHOD_OPTIONS = (dataflowConfig.pipeline_wizard?.load_methods || []).map(m => ({
  value: m.value,
  label: m.label,
  description: m.description,
}));

function LoadMethodSelect({ value, onChange, size = "default" }) {
  return (
    <Select value={value || "append"} onValueChange={onChange}>
      <SelectTrigger className={size === "sm" ? "h-8 text-xs" : ""}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {LOAD_METHOD_OPTIONS.map(opt => (
          <SelectItem key={opt.value} value={opt.value}>
            <div>
              <div className="font-medium">{opt.label}</div>
              <div className="text-xs text-slate-400">{opt.description}</div>
            </div>
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

function DatasetCard({ ds, index, formData, setFormData }) {
   const [expanded, setExpanded] = useState(false);
   const method = ds.load_method || formData.load_method || "append";

   const update = (field, value) => {
     const updated = [...formData.selected_datasets];
     updated[index] = { ...updated[index], [field]: value };
     setFormData({ ...formData, selected_datasets: updated });
   };

   const deleteDataset = () => {
     const updated = formData.selected_datasets.filter((_, i) => i !== index);
     setFormData({ ...formData, selected_datasets: updated });
   };

  const LOAD_COLORS = {
    append: "bg-green-100 text-green-700 border-green-200 dark:bg-green-900/40 dark:text-green-300 dark:border-green-700",
    replace: "bg-orange-100 text-orange-700 border-orange-200 dark:bg-orange-900/40 dark:text-orange-300 dark:border-orange-700",
    upsert: "bg-blue-100 text-blue-700 border-blue-200 dark:bg-blue-900/40 dark:text-blue-300 dark:border-blue-700",
    merge: "bg-cyan-100 text-cyan-700 border-cyan-200 dark:bg-cyan-900/40 dark:text-cyan-300 dark:border-cyan-700",
  };

  return (
    <div className="border border-slate-200 dark:border-slate-700 rounded-xl overflow-hidden bg-white dark:bg-slate-800">
      {/* Header row */}
      <div
        className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <Database className="w-4 h-4 text-slate-400 shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="font-mono text-sm font-medium text-slate-800 dark:text-slate-200 truncate block">
            {ds.schema}.{ds.table}
          </span>
          <div className="flex items-center gap-2 mt-0.5 flex-wrap">
            <span className={`text-xs px-2 py-0.5 rounded-full border font-medium ${LOAD_COLORS[method] || LOAD_COLORS.append}`}>
              {method}
            </span>
            {ds.incremental_column && (
              <span className="text-xs text-slate-400 flex items-center gap-1">
                <RefreshCw className="w-3 h-3" /> incremental: {ds.incremental_column}
              </span>
            )}
            {ds.filter_query && (
              <span className="text-xs text-slate-400 flex items-center gap-1">
                <Filter className="w-3 h-3" /> filtered
              </span>
            )}
            {ds.target_path && (
              <span className="text-xs text-slate-400 flex items-center gap-1">
                <FolderOutput className="w-3 h-3" /> {ds.target_path}
              </span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-1 shrink-0">
           <button
             type="button"
             onClick={(e) => { e.stopPropagation(); deleteDataset(); }}
             className="p-1 hover:bg-red-50 hover:text-red-600 rounded text-slate-400 transition-colors"
             title="Delete dataset"
           >
             <Trash2 className="w-4 h-4" />
           </button>
           <Settings2 className="w-3.5 h-3.5 text-slate-300" />
           {expanded ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
        </div>
      </div>

      {/* Expanded config */}
      {expanded && (
        <div className="border-t border-slate-100 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 px-4 py-4 space-y-4">
          {/* Load Method */}
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-xs font-semibold text-slate-600">Load Method</Label>
              <HelpTooltip text="How to handle data in the target table for this specific dataset. Overrides the pipeline-level default." />
            </div>
            <LoadMethodSelect
              value={ds.load_method || formData.load_method || "append"}
              onChange={(v) => update("load_method", v)}
              size="sm"
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            {/* Incremental Column */}
            <div>
              <div className="flex items-center gap-1.5 mb-1.5">
                <Label className="text-xs font-semibold text-slate-600">Incremental Column</Label>
                <HelpTooltip text="Column used to detect new/changed rows (e.g. updated_at, id). Leave blank for full loads." />
              </div>
              <Input
                value={ds.incremental_column || ""}
                onChange={(e) => update("incremental_column", e.target.value)}
                placeholder="updated_at"
                className="h-8 text-xs font-mono"
              />
              <p className="text-xs text-slate-400 mt-1">Optional — enables incremental loading</p>
            </div>

            {/* Last Value */}
            <div>
              <div className="flex items-center gap-1.5 mb-1.5">
                <Label className="text-xs font-semibold text-slate-600">Last Watermark Value</Label>
                <HelpTooltip text="The last known value of the incremental column from the previous run. Used as the starting point for the next load." />
              </div>
              <Input
                value={ds.last_value || ""}
                onChange={(e) => update("last_value", e.target.value)}
                placeholder="2024-01-01T00:00:00Z"
                className="h-8 text-xs font-mono"
              />
              <p className="text-xs text-slate-400 mt-1">Leave blank to load all data on first run</p>
            </div>
          </div>

          {/* Filter Query */}
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-xs font-semibold text-slate-600">Row Filter (WHERE clause)</Label>
              <HelpTooltip text="Optional SQL WHERE clause to limit which rows are extracted. E.g.: status = 'active' AND region = 'US'" />
            </div>
            <Input
              value={ds.filter_query || ""}
              onChange={(e) => update("filter_query", e.target.value)}
              placeholder="status = 'active' AND region = 'US'"
              className="h-8 text-xs font-mono"
            />
            <p className="text-xs text-slate-400 mt-1">Raw SQL — be careful with injection risks in dev environments</p>
          </div>

          {/* Target Path */}
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-xs font-semibold text-slate-600">Target Path / Table Override</Label>
              <HelpTooltip text="Override the default destination path or table name. For cloud storage: use a folder path like 'raw/sales/orders'. For databases: use schema.table." />
            </div>
            <Input
              value={ds.target_path || ""}
              onChange={(e) => update("target_path", e.target.value)}
              placeholder="raw/sales/orders  or  dw.fact_orders"
              className="h-8 text-xs font-mono"
            />
          </div>
        </div>
      )}
    </div>
  );
}

const SAMPLE_DATASETS = [
  { schema: "sales", table: "orders", filter_query: "", incremental_column: "updated_at", target_path: "raw/sales/orders" },
  { schema: "sales", table: "customers", filter_query: "status = 'active'", incremental_column: "updated_at", target_path: "raw/sales/customers" },
  { schema: "inventory", table: "products", filter_query: "", incremental_column: "last_modified", target_path: "raw/inventory/products" },
];

const FILE_INPUT_MODE_ICONS = { file_list: FileText, folder: FolderSearch, wildcard: Asterisk };
const FILE_INPUT_MODES = (dataflowConfig.pipeline_wizard?.file_input_modes || []).map(m => ({
  value: m.value,
  label: m.label,
  icon: FILE_INPUT_MODE_ICONS[m.value] || FileText,
  desc: m.description,
}));

function FileCard({ file, index, onUpdate, onRemove }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="border border-slate-200 rounded-xl overflow-hidden bg-white">
      <div
        className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-slate-50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <FileText className="w-4 h-4 text-[#0060AF] shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="font-mono text-sm font-medium text-slate-800 truncate block">
            {file.file_name || "Untitled file"}
          </span>
          {file.target_path && (
            <span className="text-xs text-slate-400 flex items-center gap-1 mt-0.5">
              <FolderOutput className="w-3 h-3" /> {file.target_path}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1 shrink-0">
          <button
            type="button"
            onClick={(e) => { e.stopPropagation(); onRemove(); }}
            className="p-1 hover:bg-red-50 hover:text-red-600 rounded text-slate-400 transition-colors"
            title="Remove file"
          >
            <Trash2 className="w-4 h-4" />
          </button>
          {expanded ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
        </div>
      </div>
      {expanded && (
        <div className="border-t border-slate-100 bg-slate-50 px-4 py-4 space-y-3">
          <div>
            <Label className="text-xs font-semibold text-slate-600 mb-1.5 block">File Name</Label>
            <Input
              value={file.file_name || ""}
              onChange={e => onUpdate({ ...file, file_name: e.target.value })}
              placeholder="orders_20240101.csv"
              className="h-8 text-xs font-mono"
            />
          </div>
          <div>
            <Label className="text-xs font-semibold text-slate-600 mb-1.5 block">Target Path / Table Override</Label>
            <Input
              value={file.target_path || ""}
              onChange={e => onUpdate({ ...file, target_path: e.target.value })}
              placeholder="raw/sales/orders"
              className="h-8 text-xs font-mono"
            />
          </div>
        </div>
      )}
    </div>
  );
}

function parseCSVSchema(text) {
  const lines = text.trim().split(/\r?\n/).filter(l => l.trim());
  if (lines.length === 0) return [];
  const firstLine = lines[0];
  const hasTypes = firstLine.toLowerCase().includes("data_type") || firstLine.toLowerCase().includes("type") || lines.length > 1 && lines[1].split(",").length >= 2 && /^[A-Z]/i.test(lines[1].split(",")[1]?.trim());
  if (lines.length === 1 || !hasTypes) {
    const headers = firstLine.split(",").map(h => h.trim().replace(/^["']|["']$/g, ""));
    return headers.filter(Boolean).map(name => ({ name, type: "VARCHAR", length: null, nullable: true }));
  }
  const headerRow = firstLine.split(",").map(h => h.trim().toLowerCase().replace(/^["']|["']$/g, ""));
  const nameIdx = headerRow.indexOf("column_name") !== -1 ? headerRow.indexOf("column_name") : headerRow.indexOf("name") !== -1 ? headerRow.indexOf("name") : 0;
  const typeIdx = headerRow.indexOf("data_type") !== -1 ? headerRow.indexOf("data_type") : headerRow.indexOf("type") !== -1 ? headerRow.indexOf("type") : 1;
  const lengthIdx = headerRow.indexOf("length") !== -1 ? headerRow.indexOf("length") : headerRow.indexOf("size") !== -1 ? headerRow.indexOf("size") : -1;
  const nullableIdx = headerRow.indexOf("nullable") !== -1 ? headerRow.indexOf("nullable") : -1;
  return lines.slice(1).filter(l => l.trim()).map(line => {
    const parts = line.split(",").map(p => p.trim().replace(/^["']|["']$/g, ""));
    return {
      name: parts[nameIdx] || "",
      type: (parts[typeIdx] || "VARCHAR").toUpperCase(),
      length: lengthIdx >= 0 && parts[lengthIdx] ? parseInt(parts[lengthIdx], 10) || null : null,
      nullable: nullableIdx >= 0 ? !["false", "no", "0", "n"].includes(parts[nullableIdx]?.toLowerCase()) : true,
    };
  }).filter(c => c.name);
}

function parseJSONSchemaColumns(text) {
  const data = JSON.parse(text);
  let cols;
  if (Array.isArray(data)) {
    cols = data;
  } else if (data.fields && Array.isArray(data.fields)) {
    cols = data.fields;
  } else if (data.columns && Array.isArray(data.columns)) {
    cols = data.columns;
  } else {
    throw new Error("Expected an array or an object with a 'fields' or 'columns' array");
  }
  return cols.map(c => {
    if (typeof c === "string") return { name: c, type: "VARCHAR", length: null, nullable: true };
    return {
      name: c.name || c.field || c.column || "",
      type: (c.type || c.data_type || c.dataType || "VARCHAR").toUpperCase(),
      length: c.length || c.size || c.precision || null,
      nullable: c.nullable !== undefined ? Boolean(c.nullable) : true,
    };
  }).filter(c => c.name);
}

function parseCopybookSchema(text) {
  const columns = [];
  const lines = text.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.replace(/^\s*\*.*$/, "").trim();
    if (!line) continue;
    const match = line.match(/^\d{2}\s+([\w-]+)\s+PIC\s+(.+?)\.?\s*$/i);
    if (!match) continue;
    const fieldName = match[1].replace(/-/g, "_");
    const picClause = match[2].trim().replace(/\.$/, "");
    let type = "VARCHAR";
    let length = 0;
    let nullable = true;
    if (/COMP-3/i.test(picClause)) {
      type = "PACKED_DECIMAL";
      const digits = (picClause.match(/[9VS]/gi) || []).length;
      length = Math.ceil((digits + 1) / 2);
    } else if (/COMP/i.test(picClause)) {
      type = "BINARY";
      const digits = (picClause.match(/9/g) || []).length;
      length = digits <= 4 ? 2 : digits <= 9 ? 4 : 8;
    } else if (/^S?9/i.test(picClause.replace(/\s+COMP.*$/i, ""))) {
      const hasDecimal = picClause.includes("V");
      const nines = picClause.replace(/[^9()]/gi, "");
      let digitCount = 0;
      const expandMatch = nines.match(/9\((\d+)\)/g);
      if (expandMatch) {
        expandMatch.forEach(m => { digitCount += parseInt(m.match(/\((\d+)\)/)[1], 10); });
      }
      digitCount += (nines.match(/(?<!\()9(?!\()/g) || []).length;
      type = hasDecimal ? "DECIMAL" : "INTEGER";
      length = digitCount || 1;
    } else if (/^X/i.test(picClause)) {
      type = "VARCHAR";
      const expandMatch = picClause.match(/X\((\d+)\)/i);
      if (expandMatch) {
        length = parseInt(expandMatch[1], 10);
      } else {
        length = (picClause.match(/X/gi) || []).length;
      }
    }
    columns.push({ name: fieldName, type, length: length || null, nullable });
  }
  return columns;
}

const SCHEMA_IMPORT_MODE_ICONS = { csv: FileSpreadsheet, json: FileJson, cfd: FileCode };
const SCHEMA_IMPORT_MODES = (dataflowConfig.pipeline_wizard?.schema_import_modes || []).map(m => ({
  value: m.value,
  label: m.label,
  icon: SCHEMA_IMPORT_MODE_ICONS[m.value] || FileText,
  desc: m.description,
}));

function getDefaultSchemaMode(platform) {
  if (platform === "cobol_ebcdic") return "cfd";
  if (platform === "flat_file_delimited") return "csv";
  return "json";
}

function SchemaDefinitionPanel({ formData, setFormData, platform }) {
  const [expanded, setExpanded] = useState(false);
  const [mode, setMode] = useState(getDefaultSchemaMode(platform));
  const [text, setText] = useState("");
  const [error, setError] = useState("");
  const fileSchema = formData.file_schema || [];

  const handleParse = () => {
    setError("");
    try {
      let columns;
      if (mode === "csv") columns = parseCSVSchema(text);
      else if (mode === "json") columns = parseJSONSchemaColumns(text);
      else if (mode === "cfd") columns = parseCopybookSchema(text);
      else return;
      if (!columns || columns.length === 0) {
        setError("No columns found. Check your input format.");
        return;
      }
      setFormData(prev => ({ ...prev, file_schema: columns }));
    } catch (e) {
      setError(`Parse error: ${e.message}`);
    }
  };

  const handleClear = () => {
    setFormData(prev => ({ ...prev, file_schema: [] }));
    setText("");
    setError("");
  };

  return (
    <div className="border border-slate-200 dark:border-slate-700 rounded-xl overflow-hidden">
      <button
        type="button"
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-2 px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors text-left"
      >
        <Table2 className="w-4 h-4 text-[#0060AF] shrink-0" />
        <span className="text-sm font-semibold text-slate-700 dark:text-slate-300 flex-1">Schema Definition</span>
        {fileSchema.length > 0 && (
          <span className="text-xs bg-blue-100 text-[#0060AF] dark:bg-blue-900/40 dark:text-blue-300 rounded-full px-2 py-0.5 font-medium">
            {fileSchema.length} column{fileSchema.length !== 1 ? "s" : ""}
          </span>
        )}
        <HelpTooltip text="Define the schema (columns/fields) of your flat file so the platform knows how to parse it." />
        {expanded ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
      </button>

      {expanded && (
        <div className="border-t border-slate-100 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 px-4 py-4 space-y-4">
          <div className="flex gap-2 flex-wrap">
            {SCHEMA_IMPORT_MODES.map(opt => {
              const Icon = opt.icon;
              const active = mode === opt.value;
              return (
                <button
                  key={opt.value}
                  type="button"
                  onClick={() => { setMode(opt.value); setError(""); }}
                  className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                    active
                      ? "bg-[#0060AF] text-white border-[#0060AF]"
                      : "bg-white dark:bg-slate-800 text-slate-600 dark:text-slate-400 border-slate-200 dark:border-slate-700 hover:border-[#0060AF]/40"
                  }`}
                >
                  <Icon className="w-3.5 h-3.5" />
                  {opt.label}
                </button>
              );
            })}
          </div>

          <div className="space-y-2">
            <Label className="text-xs text-slate-600 dark:text-slate-400">
              {mode === "csv" && "Paste a CSV header row, or rows with column_name,data_type,length,nullable"}
              {mode === "json" && 'Paste JSON like [{"name":"col1","type":"VARCHAR","length":50}] or {"fields":[...]}'}
              {mode === "cfd" && "Paste COBOL copybook text with level numbers and PIC clauses"}
            </Label>
            <Textarea
              value={text}
              onChange={e => setText(e.target.value)}
              placeholder={
                mode === "csv"
                  ? "column_name,data_type,length,nullable\ncustomer_id,INTEGER,10,false\nfirst_name,VARCHAR,50,true\nemail,VARCHAR,255,true"
                  : mode === "json"
                  ? '[{"name":"customer_id","type":"INTEGER","length":10},{"name":"first_name","type":"VARCHAR","length":50}]'
                  : "01  CUSTOMER-RECORD.\n    05  CUSTOMER-ID        PIC 9(10).\n    05  CUSTOMER-NAME      PIC X(50).\n    05  BALANCE            PIC S9(7)V99 COMP-3."
              }
              className="font-mono text-xs min-h-[100px] bg-white dark:bg-slate-900"
            />
            <div className="flex gap-2">
              <Button type="button" size="sm" onClick={handleParse} disabled={!text.trim()} className="gap-1.5">
                <Table2 className="w-3.5 h-3.5" />
                Parse Schema
              </Button>
              {fileSchema.length > 0 && (
                <Button type="button" variant="outline" size="sm" onClick={handleClear} className="gap-1.5 text-red-600 hover:text-red-700 hover:bg-red-50">
                  <X className="w-3.5 h-3.5" />
                  Clear Schema
                </Button>
              )}
            </div>
          </div>

          {error && (
            <div className="flex items-start gap-2 text-red-600 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-3 text-xs">
              <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
              {error}
            </div>
          )}

          {fileSchema.length > 0 && (
            <div className="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden bg-white dark:bg-slate-900">
              <div className="px-3 py-2 bg-slate-100 dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700">
                <span className="text-xs font-semibold text-slate-600 dark:text-slate-400">
                  Parsed Columns ({fileSchema.length})
                </span>
              </div>
              <div className="max-h-[200px] overflow-y-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-slate-100 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50">
                      <th className="text-left px-3 py-1.5 font-semibold text-slate-500">#</th>
                      <th className="text-left px-3 py-1.5 font-semibold text-slate-500">Name</th>
                      <th className="text-left px-3 py-1.5 font-semibold text-slate-500">Type</th>
                      <th className="text-left px-3 py-1.5 font-semibold text-slate-500">Length</th>
                      <th className="text-left px-3 py-1.5 font-semibold text-slate-500">Nullable</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fileSchema.map((col, idx) => (
                      <tr key={idx} className="border-b border-slate-50 dark:border-slate-800 hover:bg-slate-50 dark:hover:bg-slate-800/50">
                        <td className="px-3 py-1.5 text-slate-400">{idx + 1}</td>
                        <td className="px-3 py-1.5 font-mono font-medium text-slate-700 dark:text-slate-300">{col.name}</td>
                        <td className="px-3 py-1.5 font-mono text-blue-600 dark:text-blue-400">{col.type}</td>
                        <td className="px-3 py-1.5 text-slate-500">{col.length ?? "—"}</td>
                        <td className="px-3 py-1.5">
                          <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${col.nullable ? "bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300" : "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300"}`}>
                            {col.nullable ? "Yes" : "No"}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function FlatFileSourcePanel({ formData, setFormData, platform }) {
  const mode = formData.file_source_mode || "file_list";
  const files = formData.file_source_list || [];

  const updateField = (patch) => setFormData(prev => ({ ...prev, ...patch }));

  const addFile = () => {
    updateField({ file_source_list: [...files, { file_name: "", target_path: "" }] });
  };

  const updateFile = (index, updated) => {
    const next = [...files];
    next[index] = updated;
    updateField({ file_source_list: next });
  };

  const removeFile = (index) => {
    updateField({ file_source_list: files.filter((_, i) => i !== index) });
  };

  return (
    <div className="space-y-4">
      <div>
        <div className="flex items-center gap-1.5 mb-2">
          <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">File Input Mode</Label>
          <HelpTooltip text="Choose how to specify which files to include in this pipeline." />
        </div>
        <div className="flex gap-2">
          {FILE_INPUT_MODES.map(opt => {
            const Icon = opt.icon;
            const active = mode === opt.value;
            return (
              <button
                key={opt.value}
                type="button"
                onClick={() => updateField({ file_source_mode: opt.value })}
                className={`flex-1 flex flex-col items-center gap-1.5 px-3 py-3 rounded-lg border text-center transition-all ${
                  active
                    ? "bg-blue-50 dark:bg-blue-900/30 border-blue-400 dark:border-blue-600 text-blue-800 dark:text-blue-300"
                    : "bg-white dark:bg-slate-800 border-slate-200 dark:border-slate-700 hover:border-blue-300 text-slate-600 dark:text-slate-400"
                }`}
              >
                <Icon className={`w-5 h-5 ${active ? "text-blue-600" : "text-slate-400"}`} />
                <span className="text-sm font-medium">{opt.label}</span>
                <span className="text-xs text-slate-400 leading-tight">{opt.desc}</span>
              </button>
            );
          })}
        </div>
      </div>

      {mode === "file_list" && (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1.5">
              <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Files</Label>
              {files.length > 0 && (
                <span className="text-xs bg-slate-100 text-slate-500 rounded-full px-2 py-0.5 font-medium">
                  {files.length} file{files.length !== 1 ? "s" : ""}
                </span>
              )}
            </div>
            <button
              type="button"
              onClick={addFile}
              className="flex items-center gap-1 text-xs px-3 py-1.5 rounded-lg border border-blue-200 bg-blue-50 text-blue-700 hover:bg-blue-100 font-medium transition-colors"
            >
              <Plus className="w-3.5 h-3.5" /> Add File
            </button>
          </div>
          {files.length === 0 && (
            <div className="border-2 border-dashed border-slate-200 dark:border-slate-700 rounded-xl p-6 text-center">
              <FileText className="w-8 h-8 text-slate-300 dark:text-slate-600 mx-auto mb-2" />
              <p className="text-sm text-slate-500 font-medium">No files added yet</p>
              <p className="text-xs text-slate-400 mt-1">Click "Add File" to specify individual file names</p>
            </div>
          )}
          <div className="space-y-2">
            {files.map((f, idx) => (
              <FileCard
                key={idx}
                file={f}
                index={idx}
                onUpdate={(updated) => updateFile(idx, updated)}
                onRemove={() => removeFile(idx)}
              />
            ))}
          </div>
        </div>
      )}

      {mode === "folder" && (
        <div className="space-y-3">
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Folder Path</Label>
              <HelpTooltip text="All files in this directory will be included. Use a full path or relative path from the connection root." />
            </div>
            <Input
              value={formData.file_source_folder || ""}
              onChange={e => updateField({ file_source_folder: e.target.value })}
              placeholder="/data/inbound/sales/"
              className="font-mono text-sm"
            />
            <p className="text-xs text-slate-400 mt-1">Every file in this folder will be processed</p>
          </div>
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">File Extension Filter</Label>
              <HelpTooltip text="Optional — limit to specific extensions like .csv, .dat, .json" />
            </div>
            <Input
              value={formData.file_source_extension || ""}
              onChange={e => updateField({ file_source_extension: e.target.value })}
              placeholder=".csv"
              className="font-mono text-sm"
            />
            <p className="text-xs text-slate-400 mt-1">Leave blank to include all files</p>
          </div>
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Target Path</Label>
              <HelpTooltip text="Where to write the output for files from this folder" />
            </div>
            <Input
              value={formData.file_source_target_path || ""}
              onChange={e => updateField({ file_source_target_path: e.target.value })}
              placeholder="raw/sales/"
              className="font-mono text-sm"
            />
          </div>
        </div>
      )}

      {mode === "wildcard" && (
        <div className="space-y-3">
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Wildcard Pattern</Label>
              <HelpTooltip text="Glob pattern to match files. Supports * (any characters), ? (single character), and ** (recursive directories)." />
            </div>
            <Input
              value={formData.file_source_wildcard || ""}
              onChange={e => updateField({ file_source_wildcard: e.target.value })}
              placeholder="/data/inbound/**/*.csv"
              className="font-mono text-sm"
            />
            <p className="text-xs text-slate-400 mt-1.5">
              Examples: <code className="bg-slate-100 px-1 rounded">*.csv</code>{" "}
              <code className="bg-slate-100 px-1 rounded">orders_*.dat</code>{" "}
              <code className="bg-slate-100 px-1 rounded">/data/**/*.parquet</code>
            </p>
          </div>
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Base Directory</Label>
              <HelpTooltip text="Root directory where the wildcard pattern is applied" />
            </div>
            <Input
              value={formData.file_source_base_dir || ""}
              onChange={e => updateField({ file_source_base_dir: e.target.value })}
              placeholder="/data/inbound/"
              className="font-mono text-sm"
            />
          </div>
          <div>
            <div className="flex items-center gap-1.5 mb-1.5">
              <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Target Path</Label>
              <HelpTooltip text="Where to write matched files" />
            </div>
            <Input
              value={formData.file_source_target_path || ""}
              onChange={e => updateField({ file_source_target_path: e.target.value })}
              placeholder="raw/sales/"
              className="font-mono text-sm"
            />
          </div>
          <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg px-3 py-2">
            <p className="text-xs text-amber-700 dark:text-amber-300 font-medium">Wildcard mode disables the Advanced tab</p>
            <p className="text-xs text-amber-600 dark:text-amber-400 mt-0.5">Column mapping, data quality rules, and masking are not available when using wildcard patterns since the file schema is determined at runtime.</p>
          </div>
        </div>
      )}

      <SchemaDefinitionPanel formData={formData} setFormData={setFormData} platform={platform} />
    </div>
  );
}

export default function JobDataTab({ formData, setFormData, connections = [] }) {
  const datasets = formData.selected_datasets || [];
  const [showSamples, setShowSamples] = useState(false);

  const sourceConn = connections.find(c => c.id === formData.source_connection_id);
  const isFileSource = sourceConn && FLAT_FILE_PLATFORMS.includes(sourceConn.platform);

  const addSampleDatasets = () => {
    setFormData({ ...formData, selected_datasets: [...datasets, ...SAMPLE_DATASETS.filter(s => !datasets.some(d => d.schema === s.schema && d.table === s.table))] });
    setShowSamples(false);
  };

  if (isFileSource) {
    return (
      <div className="space-y-5">
        <FlatFileSourcePanel formData={formData} setFormData={setFormData} platform={sourceConn?.platform} />

        <div className="border border-slate-200 dark:border-slate-700 rounded-xl p-4">
          <div className="flex items-center gap-1.5 mb-1">
            <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Default Load Method</Label>
            <HelpTooltip text="Controls how data is written to the target." />
          </div>
          <p className="text-xs text-slate-400 mb-3">How to handle existing data in the target</p>
          <LoadMethodSelect
            value={formData.load_method}
            onChange={(v) => setFormData({ ...formData, load_method: v })}
          />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-5">
      <div>
        <div className="flex items-center gap-1.5 mb-2">
          <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Select Tables / Datasets</Label>
          <HelpTooltip text="Choose which tables or files from the source system to include in this pipeline. Each dataset can have its own load configuration." />
        </div>
        <p className="text-xs text-slate-400 mb-3">
          Pick one or more tables from the source connection. Each can be individually configured below.
        </p>
        {datasets.length === 0 && (
          <div className="mb-3">
            <button
              type="button"
              onClick={addSampleDatasets}
              className="text-xs px-3 py-1.5 rounded border border-amber-200 bg-amber-50 text-amber-700 hover:bg-amber-100 font-medium"
            >
              + Load Sample Datasets for Testing
            </button>
          </div>
        )}
        <ObjectSelector
          selectedObjects={formData.selected_datasets}
          onChange={(objects) => setFormData({ ...formData, selected_datasets: objects })}
          connectionId={formData.source_connection_id}
          platform={sourceConn?.platform}
        />
      </div>

      <div className="border border-slate-200 rounded-xl p-4">
        <div className="flex items-center gap-1.5 mb-1">
          <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">Default Load Method</Label>
          <HelpTooltip text="This applies to all datasets unless overridden individually. Controls how data is written to the target." />
        </div>
        <p className="text-xs text-slate-400 mb-3">Applied to all datasets — individual datasets can override this.</p>
        <LoadMethodSelect
          value={formData.load_method}
          onChange={(v) => setFormData({ ...formData, load_method: v })}
        />
      </div>

      {datasets.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-1.5">
              <Label className="text-sm font-semibold text-slate-700 dark:text-slate-300">
                Dataset Configuration
              </Label>
              <span className="text-xs bg-slate-100 text-slate-500 rounded-full px-2 py-0.5 font-medium">
                {datasets.length} dataset{datasets.length !== 1 ? "s" : ""}
              </span>
            </div>
            <p className="text-xs text-slate-400">Click a dataset to configure it</p>
          </div>
          <div className="space-y-2">
            {datasets.map((ds, idx) => (
              <DatasetCard
                key={`${ds.schema}.${ds.table}`}
                ds={ds}
                index={idx}
                formData={formData}
                setFormData={setFormData}
              />
            ))}
          </div>
        </div>
      )}

      {datasets.length === 0 && (
        <div className="border-2 border-dashed border-slate-200 rounded-xl p-6 text-center">
          <Database className="w-8 h-8 text-slate-300 mx-auto mb-2" />
          <p className="text-sm text-slate-500 font-medium">No datasets selected yet</p>
          <p className="text-xs text-slate-400 mt-1">Use the selector above to choose tables from your source connection</p>
        </div>
      )}
    </div>
  );
}