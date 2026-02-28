import { forwardRef, memo } from "react";
import { X, GripVertical, Lock } from "lucide-react";
import { TRANSFORMATIONS, TRANSFORMATION_PARAMS } from "./constants";
import { cn } from "@/lib/utils";
import { Checkbox } from "@/components/ui/checkbox";
import TransformSelect from "./TransformSelect";

const EditableCell = memo(function EditableCell({ value, onChange, placeholder = "", className = "", mono = false, disabled = false }) {
  return (
    <input
      value={value || ""}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      disabled={disabled}
      className={cn(
        "w-full h-7 px-2 text-xs border-0 bg-transparent focus:bg-white focus:border focus:border-blue-400 focus:outline-none rounded focus:shadow-sm transition-all",
        mono && "font-mono",
        disabled && "opacity-50 cursor-not-allowed",
        className
      )}
    />
  );
});

const SelectCell = memo(function SelectCell({ value, onChange, options, disabled = false }) {
  return (
    <TransformSelect value={value} onChange={onChange} options={options} disabled={disabled} />
  );
});

/** Inline param inputs that appear when a transformation needs extra config */
const TransformParams = memo(function TransformParams({ mapping, onUpdate, isCondensed }) {
  const params = TRANSFORMATION_PARAMS[mapping.transformation];
  if (!params || isCondensed) return null;
  return (
    <tr className="bg-blue-50/40">
      <td colSpan={3} />
      <td colSpan={8} className="pb-1 pt-0 pl-8 pr-2">
        <div className="flex flex-wrap gap-2 items-center py-1">
          <span className="text-xs text-blue-600 font-medium mr-1">Params:</span>
          {params.fields.map(f => (
            <div key={f.key} className="flex items-center gap-1">
              <span className="text-xs text-slate-500">{f.label}:</span>
              <input
                value={mapping[f.key] || ""}
                onChange={e => onUpdate(f.key, e.target.value)}
                placeholder={f.placeholder}
                className="h-6 px-2 text-xs border border-blue-200 bg-white rounded font-mono w-36 focus:outline-none focus:border-blue-400"
              />
            </div>
          ))}
        </div>
      </td>
    </tr>
  );
});


      const ColumnMapperRow = memo(forwardRef(function ColumnMapperRow({
  mapping,
  sourceCol,
  isSelected,
  onSelect,
  onUpdate,
  onRemove,
  isCondensed,
  isDragging,
  dragProps,
  dragHandleProps,
  isAudit,
  transformations = TRANSFORMATIONS,
}, ref) {

  const needsParams = !!TRANSFORMATION_PARAMS[mapping.transformation];

  // Show expression cell only for custom_sql in full mode (params row handles the rest)
  const showExpressionCell = !isCondensed && mapping.transformation === "custom_sql";

  if (isCondensed) {
    return (
      <>
        <tr
          ref={ref}
          className={cn(
            "border-b border-slate-100 transition-colors group",
            isDragging ? "bg-blue-50 shadow-lg" : isAudit ? "bg-amber-50 hover:bg-amber-100" : "hover:bg-blue-50/50"
          )}
          {...dragProps}
        >
          <td className="pl-2 py-1 w-6" {...dragHandleProps}>
            <GripVertical className="w-3 h-3 text-slate-300 cursor-move group-hover:text-slate-500" />
          </td>
          <td className="py-1 w-6">
            <Checkbox checked={isSelected} onCheckedChange={onSelect} disabled={isAudit} className="h-3 w-3" />
          </td>
          {isAudit && <td className="py-1 w-5"><Lock className="w-3 h-3 text-amber-500" /></td>}
          <td className="py-1 text-xs font-mono text-slate-700 pl-2">{mapping.source || <span className="text-slate-400 italic">computed</span>}</td>
          <td className="py-1"><EditableCell value={mapping.target} onChange={v => onUpdate("target", v)} mono /></td>
          <td className="py-1 min-w-[160px]">
            <SelectCell value={mapping.transformation || "direct"} onChange={v => onUpdate("transformation", v)} options={transformations} />
          </td>
          <td className="py-1 pr-2 w-6">
            <button type="button" onClick={onRemove} className="opacity-0 group-hover:opacity-100 text-slate-400 hover:text-red-500 transition-all">
              <X className="w-3 h-3" />
            </button>
          </td>
        </tr>
      </>
    );
  }

  // Full Excel-style row
  return (
    <>
      <tr
        ref={ref}
        className={cn(
          "border-b border-slate-100 transition-colors group",
          isDragging ? "bg-blue-50 shadow-lg" : isAudit ? "bg-amber-50/60 hover:bg-amber-50" : "hover:bg-blue-50/40"
        )}
        {...dragProps}
      >
        <td className="pl-2 py-1 w-6" {...dragHandleProps}>
          <GripVertical className="w-3 h-3 text-slate-300 cursor-move group-hover:text-slate-500" />
        </td>
        <td className="py-1 w-6">
          <Checkbox checked={isSelected} onCheckedChange={onSelect} disabled={isAudit} className="h-3 w-3" />
        </td>
        <td className="py-1 w-5 text-center">
          {isAudit && <Lock className="w-3 h-3 text-amber-500 inline" />}
        </td>

        {/* Source Name */}
        <td className="py-1 px-2 text-xs font-mono text-slate-800 border-r border-slate-100 min-w-[120px]">
          {mapping.source || <span className="text-slate-400 italic text-xs">computed</span>}
        </td>
        {/* Source Type */}
        <td className="py-1 text-xs font-mono text-slate-500 border-r border-slate-100 min-w-[80px] px-2">
          {mapping.sourceDataType || ""}
        </td>
        {/* Source Length */}
        <td className="py-1 text-xs font-mono text-slate-400 border-r border-slate-100 min-w-[60px] px-2">
          {mapping.sourceLength || ""}
        </td>

        {/* Target Name */}
        <td className="py-1 border-r border-slate-100 min-w-[120px]">
          <EditableCell value={mapping.target} onChange={v => onUpdate("target", v)} placeholder="target_col" mono />
        </td>
        {/* Target Type */}
        <td className="py-1 border-r border-slate-100 min-w-[80px]">
          <EditableCell value={mapping.targetDataType} onChange={v => onUpdate("targetDataType", v)} placeholder="varchar" mono />
        </td>
        {/* Target Length */}
        <td className="py-1 border-r border-slate-100 min-w-[60px]">
          <EditableCell value={mapping.targetLength} onChange={v => onUpdate("targetLength", v)} placeholder="255" mono />
        </td>

        {/* Transformation */}
        <td className="py-1 border-r border-slate-100 min-w-[160px]">
          <SelectCell value={mapping.transformation || "direct"} onChange={v => onUpdate("transformation", v)} options={transformations} />
        </td>

        {/* Expression cell - only for custom_sql, otherwise show hint about params row */}
        <td className="py-1 min-w-[140px]">
          {showExpressionCell ? (
            <EditableCell
              value={mapping.expression}
              onChange={v => onUpdate("expression", v)}
              placeholder="CAST({col} AS VARCHAR)"
            />
          ) : needsParams ? (
            <span className="text-xs text-blue-400 italic px-2">↓ see params</span>
          ) : null}
        </td>

        <td className="py-1 pr-2 w-6">
          <button type="button" onClick={onRemove} className="opacity-0 group-hover:opacity-100 text-slate-400 hover:text-red-500 transition-all">
            <X className="w-3 h-3" />
          </button>
        </td>
      </tr>

      {/* Transformation parameter sub-row */}
      {needsParams && mapping.transformation !== "custom_sql" && (
        <TransformParams mapping={mapping} onUpdate={onUpdate} isCondensed={isCondensed} />
      )}

    </>
    );
    }));

    export default ColumnMapperRow;