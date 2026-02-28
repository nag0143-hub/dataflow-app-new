import { useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Download, Upload, AlertCircle } from "lucide-react";
import { exportMappingsToCSV, importMappingsFromCSV } from "./csvUtils";

export default function ColumnMapperImportExport({ 
  mappings = [], 
  tableName = "mappings",
  onImport,
  disabled = false 
}) {
  const [isImporting, setIsImporting] = useState(false);
  const [importError, setImportError] = useState(null);
  const fileInputRef = useRef(null);

  const handleExport = () => {
    try {
      exportMappingsToCSV(mappings, tableName);
    } catch (error) {
      alert(`Export failed: ${error.message}`);
    }
  };

  const handleImportClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileSelected = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setIsImporting(true);
    setImportError(null);

    try {
      const importedMappings = await importMappingsFromCSV(file);
      onImport(importedMappings);
    } catch (error) {
      setImportError(error.message);
    } finally {
      setIsImporting(false);
      // Reset file input
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  return (
    <div className="space-y-2">
      <div className="flex gap-2">
        <Button
          size="sm"
          variant="outline"
          onClick={handleExport}
          disabled={disabled || mappings.length === 0}
          className="h-7 text-xs gap-1"
          title="Export mappings as CSV"
        >
          <Download className="w-3 h-3" />
          Export CSV
        </Button>
        
        <Button
          size="sm"
          variant="outline"
          onClick={handleImportClick}
          disabled={disabled || isImporting}
          className="h-7 text-xs gap-1"
          title="Import mappings from CSV"
        >
          <Upload className="w-3 h-3" />
          {isImporting ? "Importing..." : "Import CSV"}
        </Button>

        <input
          ref={fileInputRef}
          type="file"
          accept=".csv"
          onChange={handleFileSelected}
          className="hidden"
          disabled={disabled || isImporting}
        />
      </div>

      {importError && (
        <div className="flex gap-2 p-2 bg-red-50 border border-red-200 rounded text-xs text-red-700">
          <AlertCircle className="w-3 h-3 flex-shrink-0 mt-0.5" />
          <span>{importError}</span>
        </div>
      )}
    </div>
  );
}