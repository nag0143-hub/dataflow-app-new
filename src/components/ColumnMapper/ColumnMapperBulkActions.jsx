import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Copy, Trash2 } from "lucide-react";
import { TRANSFORMATIONS } from "./constants";

export default function ColumnMapperBulkActions({
  selectedCount,
  onSelectAll,
  onDeselectAll,
  onApplyTransformation,
  onDeleteSelected,
  onDuplicate,
  totalCount,
}) {
  if (selectedCount === 0) return null;

  return (
    <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 flex flex-wrap items-center gap-3">
      <div className="text-sm font-medium text-blue-900">
        {selectedCount} column{selectedCount !== 1 ? "s" : ""} selected
      </div>

      <div className="flex flex-wrap gap-2">
        <Button
          size="sm"
          variant="outline"
          onClick={onSelectAll}
          className="text-xs h-7"
          disabled={selectedCount === totalCount}
        >
          Select All
        </Button>
        <Button
          size="sm"
          variant="outline"
          onClick={onDeselectAll}
          className="text-xs h-7"
        >
          Deselect
        </Button>

        <div className="border-l border-blue-200"></div>

        <Select onValueChange={onApplyTransformation}>
          <SelectTrigger className="w-48 h-7 text-xs border-blue-200">
            <SelectValue placeholder="Apply transformation..." />
          </SelectTrigger>
          <SelectContent>
            {TRANSFORMATIONS.map((t) => (
              <SelectItem key={t.value} value={t.value} className="text-xs">
                {t.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Button
          size="sm"
          variant="outline"
          onClick={onDuplicate}
          className="text-xs h-7 gap-1"
        >
          <Copy className="w-3 h-3" />
          Duplicate
        </Button>

        <Button
          size="sm"
          variant="outline"
          onClick={onDeleteSelected}
          className="text-xs h-7 text-red-600 hover:text-red-700 hover:bg-red-50"
        >
          <Trash2 className="w-3 h-3" />
          Delete
        </Button>
      </div>
    </div>
  );
}