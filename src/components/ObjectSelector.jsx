import { useState, useEffect, useRef } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { 
  ChevronDown, 
  ChevronRight, 
  Search, 
  Table2, 
  Database,
  Settings2,
  X,
  Check,
  Upload,
  RefreshCw,
  Loader2,
  Wifi,
  WifiOff
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import SchemaImporter from "@/components/SchemaImporter";

const SAMPLE_SCHEMAS = [
  {
    name: "sales",
    tables: [
      {
        name: "orders",
        columns: [
          { name: "order_id", type: "int" },
          { name: "customer_id", type: "int" },
          { name: "order_date", type: "datetime" },
          { name: "total_amount", type: "decimal" },
          { name: "status", type: "varchar" },
          { name: "region", type: "varchar" },
          { name: "created_at", type: "datetime" },
          { name: "updated_at", type: "datetime" },
          { name: "payment_method", type: "varchar" },
          { name: "notes", type: "text" }
        ]
      },
      {
        name: "customers",
        columns: [
          { name: "customer_id", type: "int" },
          { name: "first_name", type: "varchar" },
          { name: "last_name", type: "varchar" },
          { name: "email", type: "varchar" },
          { name: "phone", type: "varchar" },
          { name: "country", type: "varchar" },
          { name: "signup_date", type: "datetime" },
          { name: "customer_tier", type: "varchar" },
          { name: "created_at", type: "datetime" },
          { name: "is_active", type: "bit" }
        ]
      }
    ]
  },
  {
    name: "inventory",
    tables: [
      {
        name: "products",
        columns: [
          { name: "product_id", type: "int" },
          { name: "product_name", type: "varchar" },
          { name: "sku", type: "varchar" },
          { name: "category", type: "varchar" },
          { name: "price", type: "decimal" },
          { name: "cost", type: "decimal" },
          { name: "quantity_on_hand", type: "int" },
          { name: "reorder_level", type: "int" },
          { name: "last_restocked", type: "datetime" },
          { name: "is_discontinued", type: "bit" }
        ]
      }
    ]
  }
];

export default function ObjectSelector({ selectedObjects = [], onChange, connectionId, platform }) {
  const [searchTerm, setSearchTerm] = useState("");
  const [schemas, setSchemas] = useState(SAMPLE_SCHEMAS);
  const [expandedSchemas, setExpandedSchemas] = useState([]);
  const [configPanelObject, setConfigPanelObject] = useState(null);
  const [objectConfig, setObjectConfig] = useState({});
  const [showImporter, setShowImporter] = useState(false);
  const [liveLoading, setLiveLoading] = useState(false);
  const [liveError, setLiveError] = useState(null);
  const [liveConnected, setLiveConnected] = useState(false);
  const lastFetchedId = useRef(null);

  const fetchLiveSchemas = async (connId) => {
    if (!connId) return;
    setLiveLoading(true);
    setLiveError(null);
    try {
      const resp = await fetch("/api/introspect-schema", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ connectionId: connId }),
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.error || "Server returned " + resp.status);
      }
      const data = await resp.json();
      if (data.success && data.schemas?.length > 0) {
        setSchemas(data.schemas);
        setExpandedSchemas(data.schemas.map(s => s.name));
        setLiveConnected(true);
        lastFetchedId.current = connId;
      } else {
        setLiveError(data.error || "No schemas found in this database");
        setLiveConnected(false);
      }
    } catch (err) {
      setLiveError("Failed to connect: " + err.message);
      setLiveConnected(false);
    } finally {
      setLiveLoading(false);
    }
  };

  useEffect(() => {
    if (connectionId && connectionId !== lastFetchedId.current) {
      fetchLiveSchemas(connectionId);
    }
  }, [connectionId]);

  const toggleSchema = (schema) => {
    setExpandedSchemas(prev =>
      prev.includes(schema) ? prev.filter(s => s !== schema) : [...prev, schema]
    );
  };

  const isSelected = (schema, table) =>
    selectedObjects.some(obj => obj.schema === schema && obj.table === table);

  const getObjectConfig = (schema, table) =>
    selectedObjects.find(obj => obj.schema === schema && obj.table === table);

  const toggleTable = (schema, table) => {
    if (isSelected(schema, table)) {
      onChange(selectedObjects.filter(obj => !(obj.schema === schema && obj.table === table)));
      if (configPanelObject?.schema === schema && configPanelObject?.table === table) {
        setConfigPanelObject(null);
      }
    } else {
      const tableObj = schemas.find(s => s.name === schema)?.tables.find(t => t.name === table);
      onChange([...selectedObjects, { 
        schema, 
        table, 
        target_path: `/${schema}/${table}`, 
        target_format: "original",
        columns: tableObj?.columns || []
      }]);
    }
  };

  const selectAllInSchema = (schema, tables) => {
    const toAdd = tables
      .filter(t => !isSelected(schema, t.name))
      .map(t => ({ 
        schema, 
        table: t.name, 
        target_path: `/${schema}/${t.name}`, 
        target_format: "original",
        columns: t.columns || []
      }));
    onChange([...selectedObjects, ...toAdd]);
  };

  const deselectAllInSchema = (schema, tables) => {
    onChange(selectedObjects.filter(obj => obj.schema !== schema));
    if (configPanelObject && tables.find(t => t.name === configPanelObject.table) && configPanelObject.schema === schema) {
      setConfigPanelObject(null);
    }
  };

  const openConfig = (schema, table) => {
    const existing = getObjectConfig(schema, table);
    setObjectConfig({
      filter_query: existing?.filter_query || "",
      target_path: existing?.target_path || `/${schema}/${table}`,
      target_dataset: existing?.target_dataset || "",
      target_format: existing?.target_format || "original",
      incremental_column: existing?.incremental_column || ""
    });
    setConfigPanelObject({ schema, table });
  };

  const saveConfig = () => {
    onChange(selectedObjects.map(obj =>
      obj.schema === configPanelObject.schema && obj.table === configPanelObject.table
        ? { ...obj, ...objectConfig }
        : obj
    ));
    setConfigPanelObject(null);
  };

  const handleImportedSchemas = (importedSchemas) => {
    // Merge imported schemas into existing (avoid duplicates)
    setSchemas(prev => {
      const merged = [...prev];
      importedSchemas.forEach(incoming => {
        const existing = merged.find(s => s.name === incoming.name);
        if (existing) {
          incoming.tables.forEach(t => {
            if (!existing.tables.find(et => et.name === t.name)) {
              existing.tables.push(t);
            }
          });
        } else {
          merged.push(incoming);
        }
      });
      return merged;
    });
    setExpandedSchemas(importedSchemas.map(s => s.name));
  };

  const filteredSchemas = schemas.map(schema => ({
    ...schema,
    tables: schema.tables.filter(t =>
      t.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      schema.name.toLowerCase().includes(searchTerm.toLowerCase())
    )
  })).filter(schema => schema.tables.length > 0);

  return (
    <div className="border border-slate-200 rounded-lg overflow-hidden">
      <div className="p-3 border-b border-slate-200 bg-slate-50 space-y-2">
        {liveConnected && (
          <div className="flex items-center gap-2 px-2 py-1.5 bg-emerald-50 border border-emerald-200 rounded-lg">
            <Wifi className="w-3.5 h-3.5 text-emerald-600" />
            <span className="text-xs text-emerald-700 font-medium flex-1">
              Live — {schemas.reduce((sum, s) => sum + s.tables.length, 0)} tables across {schemas.length} schema{schemas.length !== 1 ? "s" : ""}
            </span>
            <Button
              type="button" variant="ghost" size="sm"
              className="h-6 px-2 text-xs text-emerald-700 hover:text-emerald-800"
              onClick={() => fetchLiveSchemas(connectionId)}
              disabled={liveLoading}
            >
              {liveLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
            </Button>
          </div>
        )}
        {liveLoading && !liveConnected && (
          <div className="flex items-center gap-2 px-2 py-1.5 bg-blue-50 border border-blue-200 rounded-lg">
            <Loader2 className="w-3.5 h-3.5 text-[#0060AF] animate-spin" />
            <span className="text-xs text-[#0060AF] font-medium">Fetching tables from database...</span>
          </div>
        )}
        {liveError && (
          <div className="flex items-center gap-2 px-2 py-1.5 bg-amber-50 border border-amber-200 rounded-lg">
            <WifiOff className="w-3.5 h-3.5 text-amber-600 shrink-0" />
            <span className="text-xs text-amber-700 flex-1">{liveError}</span>
            <Button
              type="button" variant="ghost" size="sm"
              className="h-6 px-2 text-xs text-amber-700"
              onClick={() => fetchLiveSchemas(connectionId)}
              disabled={liveLoading}
            >
              Retry
            </Button>
          </div>
        )}
        {!liveConnected && !liveLoading && (liveError || !connectionId) && (
          <div className="flex items-center gap-2 px-2 py-1.5 bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-800 rounded-lg">
            <Upload className="w-3.5 h-3.5 text-[#0060AF] dark:text-blue-400 shrink-0" />
            <span className="text-xs text-[#0060AF] dark:text-blue-300 flex-1">
              No live connection — import your schema using DDL, JSON, or file upload
            </span>
          </div>
        )}
        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <Input
              placeholder="Search tables..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-9 bg-white"
            />
          </div>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="gap-1.5 shrink-0 text-xs"
            onClick={() => setShowImporter(v => !v)}
          >
            <Upload className="w-3.5 h-3.5" />
            Import Schema
          </Button>
        </div>
        {showImporter && (
          <div className="bg-white border border-slate-200 rounded-lg p-3">
            <SchemaImporter
              onImport={handleImportedSchemas}
              onClose={() => setShowImporter(false)}
              platform={platform}
            />
          </div>
        )}
      </div>

      {/* Object Tree */}
      <div className="max-h-[300px] overflow-y-auto">
        {filteredSchemas.length === 0 && schemas.length === 0 && (
          <div className="p-8 text-center">
            <Database className="w-12 h-12 text-slate-300 mx-auto mb-3" />
            <p className="text-sm font-medium text-slate-600 mb-1">No schemas imported</p>
            <p className="text-xs text-slate-500">Click "Import Schema" to upload metadata or discover from connection</p>
          </div>
        )}
        {filteredSchemas.length === 0 && schemas.length > 0 && (
          <div className="p-8 text-center">
            <Search className="w-12 h-12 text-slate-300 mx-auto mb-3" />
            <p className="text-sm font-medium text-slate-600 mb-1">No results</p>
            <p className="text-xs text-slate-500">Try a different search term</p>
          </div>
        )}
        {filteredSchemas.map(schema => {
          const selectedCount = schema.tables.filter(t => isSelected(schema.name, t.name)).length;
          const allSelected = selectedCount === schema.tables.length;

          return (
            <Collapsible
              key={schema.name}
              open={expandedSchemas.includes(schema.name)}
              onOpenChange={() => toggleSchema(schema.name)}
            >
              <CollapsibleTrigger asChild>
                <div className="flex items-center gap-2 px-3 py-2 hover:bg-slate-50 cursor-pointer border-b border-slate-100">
                  <Checkbox
                    checked={allSelected}
                    onCheckedChange={(checked) => {
                      if (checked) selectAllInSchema(schema.name, schema.tables);
                      else deselectAllInSchema(schema.name, schema.tables);
                    }}
                    onClick={(e) => e.stopPropagation()}
                  />
                  {expandedSchemas.includes(schema.name) ? (
                    <ChevronDown className="w-4 h-4 text-slate-400" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-slate-400" />
                  )}
                  <Database className="w-4 h-4 text-blue-500" />
                  <span className="font-medium text-slate-700">{schema.name}</span>
                  <span className="text-xs text-slate-400 ml-auto">
                    {selectedCount}/{schema.tables.length} selected
                  </span>
                </div>
              </CollapsibleTrigger>
              <CollapsibleContent>
                {schema.tables.map(tableObj => {
                  const tableName = tableObj.name;
                  return (
                    <div
                      key={tableName}
                      className={cn(
                        "flex items-center gap-2 pl-10 pr-3 py-2 hover:bg-slate-50 border-b border-slate-50",
                        isSelected(schema.name, tableName) && "bg-blue-50/50",
                        configPanelObject?.schema === schema.name && configPanelObject?.table === tableName && "bg-blue-100/60"
                      )}
                    >
                      <Checkbox
                        checked={isSelected(schema.name, tableName)}
                        onCheckedChange={() => toggleTable(schema.name, tableName)}
                      />
                      <Table2 className="w-4 h-4 text-slate-400" />
                      <span className="text-sm text-slate-600 flex-1">{tableName}</span>
                      {tableObj.columns?.length > 0 && (
                        <span className="text-xs text-slate-400">{tableObj.columns.length} cols</span>
                      )}
                      {isSelected(schema.name, tableName) && (
                        <button
                          type="button"
                          className="h-6 w-6 flex items-center justify-center rounded text-slate-400 hover:text-slate-700 hover:bg-slate-200 transition-colors"
                          onClick={(e) => { e.stopPropagation(); openConfig(schema.name, tableName); }}
                        >
                          <Settings2 className="w-3.5 h-3.5" />
                        </button>
                      )}
                    </div>
                  );
                })}
              </CollapsibleContent>
            </Collapsible>
          );
        })}
      </div>

      {/* Selected Summary */}
      <div className="p-3 border-t border-slate-200 bg-slate-50">
        <p className="text-sm text-slate-600">
          <span className="font-medium">{selectedObjects.length}</span> tables selected
          {configPanelObject && <span className="text-slate-400 ml-2">— configuring <span className="font-medium text-slate-600">{configPanelObject.schema}.{configPanelObject.table}</span></span>}
        </p>
      </div>

      {/* Inline Config Panel — no nested dialog */}
      {configPanelObject && (
        <div className="border-t border-blue-200 bg-blue-50/30 p-4 space-y-4">
          <div className="flex items-center justify-between mb-1">
            <p className="text-sm font-semibold text-slate-800">
              Configure: <span className="text-blue-700">{configPanelObject.schema}.{configPanelObject.table}</span>
            </p>
            <button type="button" onClick={() => setConfigPanelObject(null)} className="text-slate-400 hover:text-slate-600">
              <X className="w-4 h-4" />
            </button>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <Label className="text-xs">Target Path</Label>
              <Input
                value={objectConfig.target_path}
                onChange={(e) => setObjectConfig({ ...objectConfig, target_path: e.target.value })}
                placeholder="/schema/table"
                className="mt-1 h-8 text-sm"
              />
            </div>
            <div>
              <Label className="text-xs">Target Dataset Name</Label>
              <Input
                value={objectConfig.target_dataset}
                onChange={(e) => setObjectConfig({ ...objectConfig, target_dataset: e.target.value })}
                placeholder="e.g. dim_customers"
                className="mt-1 h-8 text-sm"
              />
            </div>
          </div>

          <div>
            <Label className="text-xs">Target Format</Label>
            <Select value={objectConfig.target_format} onValueChange={v => setObjectConfig({ ...objectConfig, target_format: v })}>
              <SelectTrigger className="mt-1 h-8 text-sm">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="original">Original (keep source format)</SelectItem>
                <SelectItem value="parquet">Parquet</SelectItem>
                <SelectItem value="delta">Delta Lake</SelectItem>
                <SelectItem value="iceberg">Apache Iceberg</SelectItem>
                <SelectItem value="csv">CSV</SelectItem>
                <SelectItem value="json">JSON (line-delimited)</SelectItem>
                <SelectItem value="avro">Avro</SelectItem>
                <SelectItem value="orc">ORC</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <Label className="text-xs">Filter Query (WHERE clause)</Label>
              <Input
                value={objectConfig.filter_query}
                onChange={(e) => setObjectConfig({ ...objectConfig, filter_query: e.target.value })}
                placeholder="status = 'active'"
                className="mt-1 h-8 text-sm"
              />
            </div>
            <div>
              <Label className="text-xs">Incremental Column</Label>
              <Input
                value={objectConfig.incremental_column}
                onChange={(e) => setObjectConfig({ ...objectConfig, incremental_column: e.target.value })}
                placeholder="updated_at"
                className="mt-1 h-8 text-sm"
              />
            </div>
          </div>

          <div className="flex justify-end gap-2 pt-1">
            <Button type="button" variant="outline" size="sm" onClick={() => setConfigPanelObject(null)}>Cancel</Button>
            <Button type="button" size="sm" onClick={saveConfig} className="gap-1.5">
              <Check className="w-3.5 h-3.5" />
              Save
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}