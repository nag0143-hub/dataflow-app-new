import { useState, useMemo, useEffect, useRef, useCallback } from "react";
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
import { Search, X, Plus, ChevronLeft, ChevronRight, Lock, Eye, EyeOff } from "lucide-react";
import { cn } from "@/lib/utils";
import { DragDropContext, Droppable, Draggable } from "@hello-pangea/dnd";
import { Checkbox } from "@/components/ui/checkbox";
import ColumnMapperRow from "./ColumnMapperRow";
import ColumnMapperBulkActions from "./ColumnMapperBulkActions";
import ColumnMapperImportExport from "./ColumnMapperImportExport";
import CustomFunctionQuickAdd from "./CustomFunctionQuickAdd";
import { GLOBAL_RULES, DQ_RULES, ENCRYPTION_TYPES, TRANSFORMATIONS, PAGE_SIZE, MAPPING_PAGE_SIZE, DATA_TYPES } from "./constants";
import { columnCacheManager, invalidateCacheForObjects } from "./cacheManager";
import { dataflow } from '@/api/client';

// Seeded pseudo-random so data types are stable across renders
function seededRand(seed) {
  let s = seed;
  return () => { s = (s * 1664525 + 1013904223) & 0xffffffff; return Math.abs(s) / 0x7fffffff; };
}

function getMockColumns(schema, table, selectedObjects) {
   const key = `${schema}.${table}`;
   const cached = columnCacheManager.get(key);
   if (cached) return cached;

   const selectedObj = selectedObjects?.find(obj => obj.schema === schema && obj.table === table);
   if (selectedObj?.columns && Array.isArray(selectedObj.columns) && selectedObj.columns.length > 0) {
     const cols = selectedObj.columns.map((col, idx) => ({
       name: col.name,
       dataType: col.type || "varchar",
       length: extractLength(col.type),
       order: idx + 1
     }));
     columnCacheManager.set(key, cols);
     return cols;
   }

   return [];
}

async function fetchColumnsFromConnection(connectionId, schema, table) {
  try {
    const resp = await fetch("/api/introspect-schema", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ connectionId }),
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    if (!data.success || !data.schemas) return null;
    const schemaObj = data.schemas.find(s => s.name === schema);
    if (!schemaObj) return null;
    const tableObj = schemaObj.tables?.find(t => t.name === table);
    return tableObj?.columns || null;
  } catch {
    return null;
  }
}

function extractLength(dataType) {
  if (!dataType) return "";
  const match = dataType.match(/\(([^)]+)\)/);
  return match ? match[1] : "";
}

export default function ColumnMapper({ selectedObjects = [], mappings = [], onChange, compact = false, connectionId = null }) {
    const [selectedTable, setSelectedTable] = useState(selectedObjects[0] ? `${selectedObjects[0].schema}.${selectedObjects[0].table}` : "");
    const [selectedDatasets, setSelectedDatasets] = useState(new Set(selectedObjects.map(o => `${o.schema}.${o.table}`)));
    const [search, setSearch] = useState("");
    const [mappingSearch, setMappingSearch] = useState("");
    const [page, setPage] = useState(0);
    const [mappingPage, setMappingPage] = useState(0);
    const [globalRules, setGlobalRules] = useState([]);
    const [isCondensed, setIsCondensed] = useState(false);
    const [selectedMappings, setSelectedMappings] = useState(new Set());
    const autoMappedRef = useRef(new Set());
    const [customFunctions, setCustomFunctions] = useState([]);
    const [fetchedColumns, setFetchedColumns] = useState({});
    const fetchingRef = useRef(new Set());

    const loadCustomFunctions = () => {
      dataflow.entities.CustomFunction.list().then(funcs => {
        setCustomFunctions(funcs.map(f => ({
          value: `custom_${f.name}`,
          label: f.label || f.name,
          category: f.category === "spark_udf" ? "spark_udf" : "custom",
          expressionTemplate: f.expression_template,
        })));
      }).catch(() => {});
    };

    // Load custom functions from the database
    useEffect(() => {
      loadCustomFunctions();
    }, []);

    // Merge built-in + custom function options
    const allTransformations = useMemo(() => [
      ...TRANSFORMATIONS,
      ...customFunctions
    ], [customFunctions]);

  useEffect(() => {
    if (selectedObjects.length > 0) {
      const newKey = `${selectedObjects[0].schema}.${selectedObjects[0].table}`;
      if (newKey !== selectedTable) {
        setSelectedTable(newKey);
        setSelectedDatasets(new Set(selectedObjects.map(o => `${o.schema}.${o.table}`)));
        setSearch("");
        setMappingSearch("");
        setPage(0);
        setMappingPage(0);
        setSelectedMappings(new Set());
      }
    }
  }, [selectedObjects]);

  const tableKey = selectedTable;

  const prevConnectionIdRef = useRef(connectionId);
  useEffect(() => {
    invalidateCacheForObjects(selectedObjects);
    if (prevConnectionIdRef.current !== connectionId) {
      setFetchedColumns({});
      fetchingRef.current.clear();
      prevConnectionIdRef.current = connectionId;
    }
  }, [selectedObjects, connectionId]);

  useEffect(() => {
    if (!selectedTable || !connectionId) return;
    const selectedObj = selectedObjects?.find(obj => `${obj.schema}.${obj.table}` === selectedTable);
    const hasRealColumns = selectedObj?.columns && Array.isArray(selectedObj.columns) && selectedObj.columns.length > 0;
    if (hasRealColumns) return;
    if (fetchedColumns[selectedTable]) return;
    if (fetchingRef.current.has(selectedTable)) return;
    fetchingRef.current.add(selectedTable);
    const [schema, table] = selectedTable.split(".");
    fetchColumnsFromConnection(connectionId, schema, table).then(remoteCols => {
      fetchingRef.current.delete(selectedTable);
      if (remoteCols && remoteCols.length > 0) {
        setFetchedColumns(prev => ({ ...prev, [selectedTable]: remoteCols }));
        const parsed = remoteCols.map((col, idx) => ({
          name: col.name,
          dataType: col.type || "varchar",
          length: extractLength(col.type),
          order: idx + 1
        }));
        columnCacheManager.set(selectedTable, parsed);
      }
    });
  }, [selectedTable, connectionId, selectedObjects]);

  const tableColumns = useMemo(() => {
    if (!selectedTable) return [];
    const fetched = fetchedColumns[selectedTable];
    if (fetched && fetched.length > 0) {
      return fetched.map((col, idx) => ({
        name: col.name,
        dataType: col.type || "varchar",
        length: extractLength(col.type),
        order: idx + 1
      }));
    }
    const [schema, table] = selectedTable.split(".");
    return getMockColumns(schema, table, selectedObjects);
  }, [selectedTable, selectedObjects, fetchedColumns]);

  useEffect(() => {
    if (!tableKey || !tableColumns.length || mappings[tableKey] || autoMappedRef.current.has(tableKey)) return;
    autoMappedRef.current.add(tableKey);
    const auto = tableColumns.map(c => ({ 
      source: c.name, 
      target: c.name, 
      transformation: "direct",
      sourceDataType: c.dataType,
      sourceLength: c.length
    }));
    onChange({ ...mappings, [tableKey]: auto });
  }, [tableKey, tableColumns]);

  const tableMappings = mappings[tableKey] || [];

  const mappedSourceCols = useMemo(() => new Set(tableMappings.map(m => m.source)), [tableMappings]);

  const filteredColumns = useMemo(() =>
    tableColumns.filter(c =>
      c.name.toLowerCase().includes(search.toLowerCase()) ||
      c.dataType.toLowerCase().includes(search.toLowerCase())
    ), [tableColumns, search]);

  const totalPages = Math.ceil(filteredColumns.length / PAGE_SIZE);
  const pageColumns = filteredColumns.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  const updateMapping = useCallback((source, field, value) => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      const existing = tbl.find(m => m.source === source);
      let updated;
      if (existing) {
        updated = tbl.map(m => m.source === source ? { ...m, [field]: value } : m);
      } else {
        updated = [...tbl, { source, target: source, transformation: "direct", [field]: value }];
      }
      return { ...prev, [key]: updated };
    });
  }, [selectedTable, onChange]);

  const addMapping = useCallback((col) => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      if (tbl.some(m => m.source === col.name)) return prev;
      return { 
        ...prev, 
        [key]: [...tbl, { 
          source: col.name, 
          target: col.name, 
          transformation: "direct",
          sourceDataType: col.dataType,
          sourceLength: col.length
        }] 
      };
    });
  }, [selectedTable, onChange]);

  const removeMapping = useCallback((source) => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      return { ...prev, [key]: tbl.filter(m => m.source !== source) };
    });
  }, [selectedTable, onChange]);

  const duplicateMapping = useCallback((m) => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      const newMapping = { source: m.source, target: `${m.target}_derived`, transformation: m.transformation, derived: true };
      return { ...prev, [key]: [...tbl, newMapping] };
    });
  }, [selectedTable, onChange]);

  // Search across all mappings, not just current page
  const filteredMappings = useMemo(() => {
    if (!mappingSearch) return tableMappings;
    return tableMappings.filter(m =>
      (m.source && m.source.toLowerCase().includes(mappingSearch.toLowerCase())) ||
      (m.target && m.target.toLowerCase().includes(mappingSearch.toLowerCase())) ||
      (m.transformation && m.transformation.toLowerCase().includes(mappingSearch.toLowerCase()))
    );
  }, [tableMappings, mappingSearch]);

  const totalMappingPages = Math.ceil(filteredMappings.length / MAPPING_PAGE_SIZE);
  const pageMappings = filteredMappings.slice(mappingPage * MAPPING_PAGE_SIZE, (mappingPage + 1) * MAPPING_PAGE_SIZE);

  const addAllVisible = useCallback(() => {
     onChange(prev => {
       const key = selectedTable;
       const tbl = prev[key] || [];
       const mapped = new Set(tbl.map(m => m.source));
       const newCols = pageColumns
         .filter(c => !mapped.has(c.name))
         .map(c => ({ 
           source: c.name, 
           target: c.name, 
           transformation: "direct",
           sourceDataType: c.dataType,
           sourceLength: c.length
         }));
       return { ...prev, [key]: [...tbl, ...newCols] };
     });
   }, [selectedTable, pageColumns, onChange]);

  // Apply global transformation rules to all mappings
  const applyGlobalRules = useCallback(() => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      const updated = tbl.map(m => {
        const sourceCol = tableColumns.find(c => c.name === m.source);
        if (!sourceCol || m.is_audit) return m;

        for (const rule of globalRules) {
          const ruleConfig = GLOBAL_RULES.find(r => r.value === rule);
          if (ruleConfig && ruleConfig.pattern.test(sourceCol.name)) {
            let newTransform = m.transformation;
            if (rule === "date_standardize") newTransform = "date_iso";
            else if (rule === "text_trim") newTransform = "trim";
            else if (rule === "number_remove_leading") newTransform = "round_0dp";
            else if (rule === "email_lower") newTransform = "lowercase";
            return { ...m, transformation: newTransform };
          }
        }
        return m;
      });
      return { ...prev, [key]: updated };
    });
  }, [selectedTable, tableColumns, globalRules, onChange]);

  // Bulk actions
  const handleSelectAll = useCallback(() => {
    const allNonAudit = new Set(
      tableMappings
        .filter(m => !m.is_audit)
        .map((m, i) => i)
    );
    setSelectedMappings(allNonAudit);
  }, [tableMappings]);

  const handleDeselectAll = useCallback(() => {
    setSelectedMappings(new Set());
  }, []);

  const handleApplyTransformation = useCallback((transformation) => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      const updated = tbl.map((m, i) => 
        selectedMappings.has(i) ? { ...m, transformation } : m
      );
      return { ...prev, [key]: updated };
    });
    setSelectedMappings(new Set());
  }, [selectedTable, selectedMappings, onChange]);

  const handleDeleteSelected = useCallback(() => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      return { 
        ...prev, 
        [key]: tbl.filter((m, i) => !selectedMappings.has(i) || m.is_audit)
      };
    });
    setSelectedMappings(new Set());
  }, [selectedTable, selectedMappings, onChange]);

  const handleDuplicateSelected = useCallback(() => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      const toDuplicate = tbl.filter((m, i) => selectedMappings.has(i));
      const duplicated = toDuplicate.map(m => ({
        ...m,
        target: `${m.target}_copy`,
        derived: true
      }));
      return { ...prev, [key]: [...tbl, ...duplicated] };
    });
    setSelectedMappings(new Set());
  }, [selectedTable, selectedMappings, onChange]);

  const handleImportMappings = useCallback((importedMappings) => {
    onChange(prev => {
      const key = selectedTable;
      const tbl = prev[key] || [];
      
      // Merge imported mappings with existing, replacing if source already exists
      const sourceSet = new Set(importedMappings.map(m => m.source));
      const existing = tbl.filter(m => !sourceSet.has(m.source) && !m.is_audit);
      
      return { ...prev, [key]: [...existing, ...importedMappings.map(m => ({ ...m, is_audit: false }))] };
    });
    setSelectedMappings(new Set());
  }, [selectedTable, onChange]);

  if (selectedObjects.length === 0) {
    return (
      <div className="text-center py-10 text-slate-400 text-sm border border-dashed border-slate-200 rounded-xl">
        No tables selected. Add tables in the <strong>Objects</strong> tab first.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {!compact && (
        <>
          <CustomFunctionQuickAdd
            customFunctions={customFunctions}
            onFunctionAdded={loadCustomFunctions}
          />

          <div className="border border-slate-200 rounded-xl p-4 bg-slate-50">
            <h3 className="text-sm font-semibold text-slate-900 mb-4">Dataset Selection</h3>
            <div className="space-y-4">
              <div className="border border-slate-300 rounded-lg bg-white p-3">
                <div className="text-xs font-medium text-slate-700 mb-2">Select Datasets ({selectedDatasets.size})</div>
                <div className="space-y-2 max-h-40 overflow-y-auto">
                  {selectedObjects.map(obj => {
                    const tableKey = `${obj.schema}.${obj.table}`;
                    const isSelected = selectedDatasets.has(tableKey);
                    return (
                      <label key={tableKey} className="flex items-center gap-2 text-xs text-slate-700 cursor-pointer hover:bg-slate-50 p-2 rounded">
                        <Checkbox
                          checked={isSelected}
                          onCheckedChange={checked => {
                            const newSet = new Set(selectedDatasets);
                            if (checked) {
                              newSet.add(tableKey);
                            } else {
                              newSet.delete(tableKey);
                            }
                            setSelectedDatasets(newSet);
                            if (!newSet.has(selectedTable)) {
                              setSelectedTable(Array.from(newSet)[0] || "");
                            }
                          }}
                        />
                        <span className="font-mono">{obj.schema}.{obj.table}</span>
                      </label>
                    );
                  })}
                </div>
              </div>

              {selectedTable && (
                <div>
                  <Label className="text-xs mb-2 block">Edit Table</Label>
                  <Select value={selectedTable} onValueChange={v => { setSelectedTable(v); setSearch(""); setPage(0); }}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select a table to edit" />
                    </SelectTrigger>
                    <SelectContent>
                      {Array.from(selectedDatasets).map(tableKey => (
                        <SelectItem key={tableKey} value={tableKey}>
                          {tableKey}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              )}
            </div>
          </div>
        </>
      )}

      {selectedTable && (
        <>
          {/* Column mappings */}
          {tableMappings.filter(m => !m.is_audit).length > 0 && (
            <div className="border border-slate-200 rounded-xl overflow-hidden">
              <div className="bg-slate-100 px-4 py-3 space-y-2">
                <div className="flex items-center justify-between gap-2">
                   <span className="text-sm font-semibold text-slate-900">Column Mappings ({tableMappings.filter(m => !m.is_audit).length})</span>
                  <div className="flex items-center gap-2">
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-6 text-xs gap-1 border-amber-300 text-amber-700 hover:bg-amber-50"
                      onClick={() => {
                        const newAudit = { source: null, target: `audit_col_${Date.now()}`, transformation: "direct", derived: true, is_audit: true };
                        onChange(prev => ({ ...prev, [tableKey]: [...(prev[tableKey] || []), newAudit] }));
                      }}
                    >
                      <Lock className="w-3 h-3" /> Add Audit Column
                    </Button>
                    <ColumnMapperImportExport
                      mappings={tableMappings.filter(m => !m.is_audit)}
                      tableName={selectedTable}
                      onImport={handleImportMappings}
                      disabled={false}
                    />
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => setIsCondensed(!isCondensed)}
                      className="h-6 text-xs gap-1"
                      title={isCondensed ? "Expand view" : "Condense view"}
                    >
                      {isCondensed ? <Eye className="w-3 h-3" /> : <EyeOff className="w-3 h-3" />}
                      {isCondensed ? "Expand" : "Condense"}
                    </Button>
                    <button type="button" onClick={() => onChange(prev => ({ ...prev, [tableKey]: prev[tableKey].filter(m => m.is_audit) }))} className="text-red-500 hover:text-red-700 text-xs">
                      Clear All
                    </button>
                  </div>
                </div>
                <div className="relative flex-1 max-w-xs">
                  <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-slate-400" />
                  <input
                    type="text"
                    placeholder="Search all columns (name, target, transformation)..."
                    value={mappingSearch}
                    onChange={e => { setMappingSearch(e.target.value); setMappingPage(0); }}
                    className="w-full pl-6 pr-2 h-6 text-xs rounded border border-slate-200 bg-white text-slate-700 placeholder:text-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-300"
                  />
                </div>
                {mappingSearch && (
                  <span className="text-slate-600 text-xs">{filteredMappings.filter(m => !m.is_audit).length} shown</span>
                )}
              </div>
              {/* Bulk actions bar */}
              <ColumnMapperBulkActions
                selectedCount={selectedMappings.size}
                totalCount={tableMappings.filter(m => !m.is_audit).length}
                onSelectAll={handleSelectAll}
                onDeselectAll={handleDeselectAll}
                onApplyTransformation={handleApplyTransformation}
                onDeleteSelected={handleDeleteSelected}
                onDuplicate={handleDuplicateSelected}
              />

              <div className="overflow-x-auto border-t border-slate-100">
                <DragDropContext onDragEnd={(result) => {
                  const { source, destination } = result;
                  if (!destination) return;
                  const newMappings = [...tableMappings];
                  const item = newMappings.splice(source.index, 1)[0];
                  newMappings.splice(destination.index, 0, item);
                  onChange({ ...mappings, [tableKey]: newMappings });
                }}>
                  <Droppable droppableId="mappings-table" type="MAPPING">
                    {(provided) => (
                      <div ref={provided.innerRef} {...provided.droppableProps} className="overflow-y-auto max-h-96">
                        <table className="w-full border-collapse">
                          <thead>
                            <tr className="bg-slate-50 border-b border-slate-200 sticky top-0 z-10">
                              <th className="py-2 w-6"></th>
                              <th className="py-2 w-6"><Checkbox /></th>
                              {isCondensed ? (
                                <>
                                  <th className="py-2 w-5"></th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200">Source</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200">Target</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600">Transformation</th>
                                  <th className="py-2 w-6"></th>
                                </>
                              ) : (
                                <>
                                  <th className="py-2 w-5"></th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200 bg-blue-50/50">Source Col</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200 bg-blue-50/50">Src Type</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200 bg-blue-50/50">Src Len</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200 bg-green-50/50">Target Col</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200 bg-green-50/50">Tgt Type</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200 bg-green-50/50">Tgt Len</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600 border-r border-slate-200">Transform</th>
                                  <th className="px-2 py-2 text-left text-xs font-semibold text-slate-600">Expression</th>
                                  <th className="py-2 w-6"></th>
                                </>
                              )}
                            </tr>
                          </thead>
                          <tbody>
                            {filteredMappings.filter(m => !m.is_audit).length === 0 ? (
                              <tr>
                                <td colSpan={isCondensed ? 7 : 11} className="text-center py-6 text-xs text-slate-400">
                                  {mappingSearch ? "No matching column mappings" : "No column mappings yet"}
                                </td>
                              </tr>
                            ) : (
                              pageMappings.filter(m => !m.is_audit).map((m, i) => {
                                const mappingIndex = tableMappings.indexOf(m);
                                const sourceCol = tableColumns.find(c => c.name === m.source);
                                const isSelected = selectedMappings.has(mappingIndex);
                                return (
                                  <Draggable key={`${m.source}-${mappingIndex}`} draggableId={`${m.source}-${mappingIndex}`} index={mappingIndex} type="MAPPING">
                                    {(provided, snapshot) => (
                                      <ColumnMapperRow
                                            ref={provided.innerRef}
                                            mapping={m}
                                            sourceCol={sourceCol}
                                            isSelected={isSelected}
                                            onSelect={() => {
                                              const newSet = new Set(selectedMappings);
                                              if (isSelected) newSet.delete(mappingIndex);
                                              else newSet.add(mappingIndex);
                                              setSelectedMappings(newSet);
                                            }}
                                            onUpdate={(field, value) => updateMapping(m.source, field, value)}
                                            onRemove={() => removeMapping(m.source)}
                                            isCondensed={isCondensed}
                                            isDragging={snapshot.isDragging}
                                            dragProps={provided.draggableProps}
                                            dragHandleProps={provided.dragHandleProps}
                                            isAudit={false}
                                            transformations={allTransformations}
                                          />
                                    )}
                                  </Draggable>
                                );
                              })
                            )}

                            {/* Audit Columns section */}
                            {tableMappings.some(m => m.is_audit) && (
                              <>
                                <tr className="bg-amber-50 border-t-2 border-amber-200">
                                  <td colSpan={isCondensed ? 7 : 11} className="px-3 py-1">
                                    <div className="flex items-center gap-2">
                                      <Lock className="w-3 h-3 text-amber-600" />
                                      <span className="text-xs font-semibold text-amber-800">Audit & System Columns</span>
                                      <span className="text-xs text-amber-600">({tableMappings.filter(m => m.is_audit).length})</span>
                                    </div>
                                  </td>
                                </tr>
                                {tableMappings.filter(m => m.is_audit).map((m, i) => {
                                  const mappingIndex = tableMappings.indexOf(m);
                                  return (
                                    <Draggable key={`audit-${m.target}-${i}`} draggableId={`audit-${m.target}-${i}`} index={mappingIndex} type="MAPPING">
                                      {(provided, snapshot) => (
                                        <ColumnMapperRow
                                          ref={provided.innerRef}
                                          {...provided.draggableProps}
                                          mapping={m}
                                          sourceCol={null}
                                          isSelected={false}
                                          onSelect={() => {}}
                                          onUpdate={(field, value) => {
                                            onChange(prev => {
                                              const key = selectedTable;
                                              const tbl = [...(prev[key] || [])];
                                              const idx = tbl.indexOf(m);
                                              if (idx >= 0) tbl[idx] = { ...tbl[idx], [field]: value };
                                              return { ...prev, [key]: tbl };
                                            });
                                          }}
                                          onRemove={() => {
                                            onChange(prev => {
                                              const key = selectedTable;
                                              return { ...prev, [key]: prev[key].filter(r => r !== m) };
                                            });
                                          }}
                                          isCondensed={isCondensed}
                                          isDragging={snapshot.isDragging}
                                          dragProps={provided.draggableProps}
                                          dragHandleProps={provided.dragHandleProps}
                                          isAudit={true}
                                          transformations={allTransformations}
                                        />
                                      )}
                                    </Draggable>
                                  );
                                })}
                              </>
                            )}
                          </tbody>
                        </table>
                        {provided.placeholder}
                      </div>
                    )}
                  </Droppable>
                </DragDropContext>
              </div>
              {totalMappingPages > 1 && (
                <div className="flex items-center justify-between px-3 py-2 border-t border-slate-100 bg-slate-50 text-xs text-slate-600">
                  <span>Page {mappingPage + 1} of {totalMappingPages}</span>
                  <div className="flex gap-1">
                    <Button variant="ghost" size="icon" className="h-6 w-6" disabled={mappingPage === 0} onClick={() => setMappingPage(p => p - 1)}>
                      <ChevronLeft className="w-3.5 h-3.5" />
                    </Button>
                    <Button variant="ghost" size="icon" className="h-6 w-6" disabled={mappingPage >= totalMappingPages - 1} onClick={() => setMappingPage(p => p + 1)}>
                      <ChevronRight className="w-3.5 h-3.5" />
                    </Button>
                  </div>
                </div>
              )}
            </div>
          )}

        </>
        )}
    </div>
    );
    }