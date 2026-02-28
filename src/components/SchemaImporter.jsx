import { useState, useRef, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { dataflow } from '@/api/client';
import {
  Upload,
  Code2,
  FileSpreadsheet,
  FileJson,
  FileCode,
  X,
  Check,
  Loader2,
  AlertCircle,
  AlertTriangle,
  ChevronDown,
  ChevronUp,
  Table2,
  Database,
} from "lucide-react";
import { cn } from "@/lib/utils";

/**
 * Parses SQL DDL text and extracts table/column definitions.
 * Handles CREATE TABLE statements.
 */
function parseDDL(ddl) {
  const schemas = {};
  const createTableRe = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"?(\w+)"?\."?(\w+)"?|"?(\w+)"?)\s*\(([^;]+?)\)/gis;
  let match;
  while ((match = createTableRe.exec(ddl)) !== null) {
    const schemaName = match[1] || "dbo";
    const tableName = match[2] || match[3];
    const body = match[4];

    // Parse columns (skip constraints)
    const columns = [];
    body.split(/,\n?/).forEach(line => {
      line = line.trim();
      if (/^(PRIMARY|FOREIGN|UNIQUE|CHECK|CONSTRAINT|INDEX|KEY)\b/i.test(line)) return;
      const colMatch = line.match(/^"?(\w+)"?\s+([A-Z][\w(),.]*)/i);
      if (colMatch) {
        columns.push({ name: colMatch[1], type: colMatch[2].toUpperCase() });
      }
    });

    if (!schemas[schemaName]) schemas[schemaName] = {};
    schemas[schemaName][tableName] = columns;
  }
  return schemas;
}

/**
 * Parses a simple JSON schema (array of { schema, table, columns[] } or
 * object like { schema: { table: [col,...] } })
 */
function parseJSONSchema(text) {
  const data = JSON.parse(text);
  const schemas = {};

  if (Array.isArray(data)) {
    // [{ schema, table, columns: [{name, type}] }]
    data.forEach(entry => {
      const s = entry.schema || "dbo";
      const t = entry.table || entry.name;
      if (!schemas[s]) schemas[s] = {};
      schemas[s][t] = (entry.columns || []).map(c =>
        typeof c === "string" ? { name: c, type: "VARCHAR" } : c
      );
    });
  } else if (typeof data === "object") {
    // { schema: { table: ["col1", ...] } } or flat { table: [...] }
    Object.entries(data).forEach(([key, val]) => {
      if (typeof val === "object" && !Array.isArray(val)) {
        // key = schema
        schemas[key] = {};
        Object.entries(val).forEach(([tbl, cols]) => {
          schemas[key][tbl] = Array.isArray(cols)
            ? cols.map(c => typeof c === "string" ? { name: c, type: "VARCHAR" } : c)
            : [];
        });
      } else if (Array.isArray(val)) {
        if (!schemas["dbo"]) schemas["dbo"] = {};
        schemas["dbo"][key] = val.map(c => typeof c === "string" ? { name: c, type: "VARCHAR" } : c);
      }
    });
  }
  return schemas;
}

/**
 * Parses XML schema like:
 * <schema name="dbo"><table name="Customers"><column name="id" type="INT"/></table></schema>
 */
function parseXMLSchema(text) {
  const schemas = {};
  const parser = new DOMParser();
  const doc = parser.parseFromString(text, "text/xml");
  const schemaNodes = doc.querySelectorAll("schema");
  if (schemaNodes.length === 0) {
    // flat <tables><table name="...">...
    const tables = doc.querySelectorAll("table");
    tables.forEach(t => {
      const s = t.getAttribute("schema") || "dbo";
      const name = t.getAttribute("name");
      if (!schemas[s]) schemas[s] = {};
      schemas[s][name] = [...t.querySelectorAll("column")].map(c => ({
        name: c.getAttribute("name"),
        type: (c.getAttribute("type") || "VARCHAR").toUpperCase(),
      }));
    });
  } else {
    schemaNodes.forEach(sNode => {
      const s = sNode.getAttribute("name") || "dbo";
      schemas[s] = {};
      sNode.querySelectorAll("table").forEach(t => {
        const name = t.getAttribute("name");
        schemas[s][name] = [...t.querySelectorAll("column")].map(c => ({
          name: c.getAttribute("name"),
          type: (c.getAttribute("type") || "VARCHAR").toUpperCase(),
        }));
      });
    });
  }
  return schemas;
}

const PLATFORM_TO_FLAVOR = {
  postgresql: "postgresql",
  sql_server: "sqlserver",
  mysql: "mysql",
  oracle: "oracle",
};

const DDL_FLAVORS = [
  { id: "auto", label: "Auto-detect" },
  { id: "postgresql", label: "PostgreSQL" },
  { id: "sqlserver", label: "SQL Server" },
  { id: "mysql", label: "MySQL" },
  { id: "oracle", label: "Oracle" },
];

const FLAVOR_TYPES = {
  postgresql: {
    native: [
      "SERIAL", "BIGSERIAL", "SMALLSERIAL", "TEXT", "JSONB", "JSON", "TIMESTAMPTZ",
      "UUID", "BOOLEAN", "BOOL", "NUMERIC", "BYTEA", "ARRAY", "INT", "INTEGER",
      "BIGINT", "SMALLINT", "REAL", "DOUBLE PRECISION", "FLOAT", "VARCHAR", "CHAR",
      "CHARACTER VARYING", "TIMESTAMP", "DATE", "TIME", "INTERVAL", "MONEY", "DECIMAL",
      "INET", "CIDR", "MACADDR", "BIT", "VARBIT", "XML", "OID", "POINT", "LINE",
      "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE", "TSVECTOR", "TSQUERY",
    ],
    features: [/IF\s+NOT\s+EXISTS/i, /SERIAL/i, /"[^"]+"\."[^"]+"/],
    antiPatterns: [
      { pattern: /\bIDENTITY\s*\(/i, msg: "IDENTITY(...) is SQL Server syntax; use SERIAL/BIGSERIAL in PostgreSQL" },
      { pattern: /\bAUTO_INCREMENT\b/i, msg: "AUTO_INCREMENT is MySQL syntax; use SERIAL/BIGSERIAL in PostgreSQL" },
      { pattern: /\bNVARCHAR\b/i, msg: "NVARCHAR is SQL Server syntax; use VARCHAR or TEXT in PostgreSQL" },
      { pattern: /\bUNIQUEIDENTIFIER\b/i, msg: "UNIQUEIDENTIFIER is SQL Server syntax; use UUID in PostgreSQL" },
      { pattern: /\bDATETIME2\b/i, msg: "DATETIME2 is SQL Server syntax; use TIMESTAMPTZ in PostgreSQL" },
      { pattern: /\bVARCHAR2\b/i, msg: "VARCHAR2 is Oracle syntax; use VARCHAR in PostgreSQL" },
      { pattern: /\bNUMBER\b/i, msg: "NUMBER is Oracle syntax; use NUMERIC in PostgreSQL" },
      { pattern: /\bENGINE\s*=/i, msg: "ENGINE= is MySQL syntax; not valid in PostgreSQL" },
      { pattern: /\bTINYINT\b/i, msg: "TINYINT is MySQL/SQL Server syntax; use SMALLINT in PostgreSQL" },
      { pattern: /\bENUM\s*\(/i, msg: "ENUM(...) inline is MySQL syntax; create an ENUM type separately in PostgreSQL" },
      { pattern: /\[[\w]+\]/i, msg: "Bracket-quoted identifiers are SQL Server syntax; use double quotes in PostgreSQL" },
      { pattern: /`[\w]+`/i, msg: "Backtick-quoted identifiers are MySQL syntax; use double quotes in PostgreSQL" },
      { pattern: /\bGO\b/i, msg: "GO batch separator is SQL Server syntax; not valid in PostgreSQL" },
    ],
  },
  sqlserver: {
    native: [
      "NVARCHAR", "NCHAR", "NTEXT", "UNIQUEIDENTIFIER", "DATETIME2", "DATETIMEOFFSET",
      "SMALLDATETIME", "DATETIME", "BIT", "MONEY", "SMALLMONEY", "TINYINT", "INT",
      "INTEGER", "BIGINT", "SMALLINT", "REAL", "FLOAT", "DECIMAL", "NUMERIC",
      "VARCHAR", "CHAR", "TEXT", "VARBINARY", "BINARY", "IMAGE", "XML",
      "SQL_VARIANT", "HIERARCHYID", "GEOMETRY", "GEOGRAPHY", "ROWVERSION", "TIMESTAMP",
      "DATE", "TIME",
    ],
    features: [/IDENTITY\s*\(/i, /\[[\w]+\]\.\[[\w]+\]/i, /\bGO\b/i],
    antiPatterns: [
      { pattern: /\bSERIAL\b/i, msg: "SERIAL is PostgreSQL syntax; use IDENTITY(1,1) in SQL Server" },
      { pattern: /\bBIGSERIAL\b/i, msg: "BIGSERIAL is PostgreSQL syntax; use BIGINT IDENTITY(1,1) in SQL Server" },
      { pattern: /\bBOOLEAN\b/i, msg: "BOOLEAN is PostgreSQL syntax; use BIT in SQL Server" },
      { pattern: /\bBYTEA\b/i, msg: "BYTEA is PostgreSQL syntax; use VARBINARY(MAX) in SQL Server" },
      { pattern: /\bTEXT\b(?!\s)/i, msg: "TEXT is deprecated in SQL Server; consider VARCHAR(MAX)" },
      { pattern: /\bJSONB?\b/i, msg: "JSON/JSONB is PostgreSQL syntax; use NVARCHAR(MAX) with JSON functions in SQL Server" },
      { pattern: /\bTIMESTAMPTZ\b/i, msg: "TIMESTAMPTZ is PostgreSQL syntax; use DATETIMEOFFSET in SQL Server" },
      { pattern: /\bAUTO_INCREMENT\b/i, msg: "AUTO_INCREMENT is MySQL syntax; use IDENTITY(1,1) in SQL Server" },
      { pattern: /\bVARCHAR2\b/i, msg: "VARCHAR2 is Oracle syntax; use VARCHAR in SQL Server" },
      { pattern: /\bNUMBER\b/i, msg: "NUMBER is Oracle syntax; use NUMERIC or DECIMAL in SQL Server" },
      { pattern: /IF\s+NOT\s+EXISTS/i, msg: "IF NOT EXISTS is not standard SQL Server CREATE TABLE syntax" },
      { pattern: /`[\w]+`/i, msg: "Backtick-quoted identifiers are MySQL syntax; use brackets [] in SQL Server" },
      { pattern: /\bENGINE\s*=/i, msg: "ENGINE= is MySQL syntax; not valid in SQL Server" },
    ],
  },
  mysql: {
    native: [
      "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "FLOAT",
      "DOUBLE", "DECIMAL", "NUMERIC", "BIT", "BOOL", "BOOLEAN", "CHAR", "VARCHAR",
      "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "TINYBLOB", "BLOB", "MEDIUMBLOB",
      "LONGBLOB", "BINARY", "VARBINARY", "DATE", "DATETIME", "TIMESTAMP", "TIME",
      "YEAR", "JSON", "ENUM", "SET", "GEOMETRY", "POINT", "LINESTRING", "POLYGON",
    ],
    features: [/AUTO_INCREMENT/i, /`[\w]+`/i, /ENGINE\s*=/i],
    antiPatterns: [
      { pattern: /\bSERIAL\b/i, msg: "SERIAL is PostgreSQL syntax; use INT AUTO_INCREMENT in MySQL" },
      { pattern: /\bBIGSERIAL\b/i, msg: "BIGSERIAL is PostgreSQL syntax; use BIGINT AUTO_INCREMENT in MySQL" },
      { pattern: /\bIDENTITY\s*\(/i, msg: "IDENTITY(...) is SQL Server syntax; use AUTO_INCREMENT in MySQL" },
      { pattern: /\bNVARCHAR\b/i, msg: "NVARCHAR is SQL Server syntax; use VARCHAR with utf8mb4 charset in MySQL" },
      { pattern: /\bUNIQUEIDENTIFIER\b/i, msg: "UNIQUEIDENTIFIER is SQL Server syntax; use CHAR(36) or BINARY(16) in MySQL" },
      { pattern: /\bDATETIME2\b/i, msg: "DATETIME2 is SQL Server syntax; use DATETIME(6) in MySQL" },
      { pattern: /\bTIMESTAMPTZ\b/i, msg: "TIMESTAMPTZ is PostgreSQL syntax; use TIMESTAMP in MySQL" },
      { pattern: /\bBYTEA\b/i, msg: "BYTEA is PostgreSQL syntax; use BLOB in MySQL" },
      { pattern: /\bJSONB\b/i, msg: "JSONB is PostgreSQL syntax; use JSON in MySQL" },
      { pattern: /\bVARCHAR2\b/i, msg: "VARCHAR2 is Oracle syntax; use VARCHAR in MySQL" },
      { pattern: /\bNUMBER\b/i, msg: "NUMBER is Oracle syntax; use NUMERIC or DECIMAL in MySQL" },
      { pattern: /\bCLOB\b/i, msg: "CLOB is Oracle syntax; use LONGTEXT in MySQL" },
      { pattern: /\[[\w]+\]/i, msg: "Bracket-quoted identifiers are SQL Server syntax; use backticks in MySQL" },
      { pattern: /"[\w]+"\.?"?/i, msg: "Double-quoted identifiers may not work in MySQL; use backticks" },
      { pattern: /\bGO\b/i, msg: "GO batch separator is SQL Server syntax; not valid in MySQL" },
    ],
  },
  oracle: {
    native: [
      "NUMBER", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "BLOB",
      "BFILE", "DATE", "TIMESTAMP", "INTERVAL", "RAW", "LONG RAW", "LONG", "ROWID",
      "UROWID", "BINARY_FLOAT", "BINARY_DOUBLE", "FLOAT", "INTEGER", "INT",
      "SMALLINT", "REAL", "DOUBLE PRECISION", "NUMERIC", "DECIMAL", "DEC",
      "XMLType", "SDO_GEOMETRY",
    ],
    features: [/"[^"]+"\."[^"]+"/i, /\bVARCHAR2\b/i, /\bNUMBER\b/i],
    antiPatterns: [
      { pattern: /\bSERIAL\b/i, msg: "SERIAL is PostgreSQL syntax; use a SEQUENCE with a trigger in Oracle" },
      { pattern: /\bBIGSERIAL\b/i, msg: "BIGSERIAL is PostgreSQL syntax; use a SEQUENCE in Oracle" },
      { pattern: /\bAUTO_INCREMENT\b/i, msg: "AUTO_INCREMENT is MySQL syntax; use a SEQUENCE or IDENTITY column in Oracle" },
      { pattern: /\bIDENTITY\s*\(/i, msg: "IDENTITY(...) is SQL Server syntax; Oracle 12c+ supports GENERATED AS IDENTITY" },
      { pattern: /\bBOOLEAN\b/i, msg: "BOOLEAN is not supported in Oracle table columns; use NUMBER(1)" },
      { pattern: /\bBYTEA\b/i, msg: "BYTEA is PostgreSQL syntax; use BLOB or RAW in Oracle" },
      { pattern: /\bTEXT\b/i, msg: "TEXT is PostgreSQL/MySQL syntax; use CLOB in Oracle" },
      { pattern: /\bJSONB?\b/i, msg: "JSON/JSONB is PostgreSQL syntax; use CLOB with JSON functions in Oracle" },
      { pattern: /\bTIMESTAMPTZ\b/i, msg: "TIMESTAMPTZ is PostgreSQL shorthand; use TIMESTAMP WITH TIME ZONE in Oracle" },
      { pattern: /\bNVARCHAR\b(?!\d)/i, msg: "NVARCHAR is SQL Server syntax; use NVARCHAR2 in Oracle" },
      { pattern: /\bVARCHAR\b(?!2)/i, msg: "VARCHAR works but VARCHAR2 is recommended in Oracle" },
      { pattern: /IF\s+NOT\s+EXISTS/i, msg: "IF NOT EXISTS is not supported in Oracle CREATE TABLE" },
      { pattern: /`[\w]+`/i, msg: "Backtick-quoted identifiers are MySQL syntax; use double quotes in Oracle" },
      { pattern: /\[[\w]+\]/i, msg: "Bracket-quoted identifiers are SQL Server syntax; use double quotes in Oracle" },
      { pattern: /\bENGINE\s*=/i, msg: "ENGINE= is MySQL syntax; not valid in Oracle" },
      { pattern: /\bGO\b/i, msg: "GO batch separator is SQL Server syntax; not valid in Oracle" },
      { pattern: /\bTINYINT\b/i, msg: "TINYINT is MySQL/SQL Server syntax; use NUMBER(3) in Oracle" },
      { pattern: /\bBIT\b/i, msg: "BIT is SQL Server syntax; use NUMBER(1) in Oracle" },
    ],
  },
};

function detectFlavor(ddl) {
  const scores = {};
  for (const [flavor, config] of Object.entries(FLAVOR_TYPES)) {
    let score = 0;
    for (const feat of config.features) {
      if (feat.test(ddl)) score += 2;
    }
    for (const nativeType of config.native) {
      const re = new RegExp(`\\b${nativeType.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
      if (re.test(ddl)) score += 1;
    }
    for (const ap of config.antiPatterns) {
      if (ap.pattern.test(ddl)) score -= 1;
    }
    scores[flavor] = score;
  }
  const sorted = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  return sorted[0][1] > 0 ? sorted[0][0] : "postgresql";
}

function validateDDL(ddl, flavor) {
  const warnings = [];
  const errors = [];

  if (!ddl.trim()) return { warnings, errors, detectedFlavor: flavor };

  let effectiveFlavor = flavor;
  if (flavor === "auto") {
    effectiveFlavor = detectFlavor(ddl);
  }

  const config = FLAVOR_TYPES[effectiveFlavor];
  if (!config) return { warnings, errors, detectedFlavor: effectiveFlavor };

  for (const ap of config.antiPatterns) {
    if (ap.pattern.test(ddl)) {
      warnings.push(ap.msg);
    }
  }

  const statements = ddl.split(/;/).filter(s => s.trim());
  for (const stmt of statements) {
    const trimmed = stmt.trim();
    if (!trimmed) continue;
    if (/^\s*--/m.test(trimmed) && trimmed.replace(/--.*$/gm, '').trim() === '') continue;
    if (/^\s*\/\*[\s\S]*?\*\/\s*$/m.test(trimmed)) continue;
    if (/^\s*GO\s*$/i.test(trimmed)) continue;

    if (/^\s*CREATE\s+/i.test(trimmed)) {
      if (!/CREATE\s+(TABLE|INDEX|SEQUENCE|TYPE|VIEW|SCHEMA|DATABASE|TRIGGER|FUNCTION|PROCEDURE)\s+/i.test(trimmed)) {
        errors.push(`Unrecognized CREATE statement: "${trimmed.substring(0, 60)}..."`);
      }
    } else if (/^\s*(ALTER|DROP|INSERT|UPDATE|DELETE|SET|USE|GRANT|COMMENT)\s+/i.test(trimmed)) {
      // valid non-CREATE statements, skip
    } else if (trimmed.length > 3 && !/^\s*$/m.test(trimmed)) {
      const nonComment = trimmed.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '').trim();
      if (nonComment.length > 3) {
        errors.push(`Unparseable statement: "${nonComment.substring(0, 60)}${nonComment.length > 60 ? '...' : ''}"`);
      }
    }
  }

  return { warnings, errors, detectedFlavor: effectiveFlavor };
}

const MODES = [
  { id: "ddl", label: "DDL", icon: Code2, desc: "Paste CREATE TABLE SQL" },
  { id: "json", label: "JSON", icon: FileJson, desc: "JSON schema definition" },
  { id: "xml", label: "XML", icon: FileCode, desc: "XML schema definition" },
  { id: "file", label: "Upload File", icon: Upload, desc: "Excel / CSV / JSON / XML" },
];

export default function SchemaImporter({ onImport, onClose, platform }) {
  const [mode, setMode] = useState("ddl");
  const [text, setText] = useState("");
  const [parsedSchemas, setParsedSchemas] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [expandedSchemas, setExpandedSchemas] = useState([]);
  const [selectedTables, setSelectedTables] = useState({});
  const [ddlFlavor, setDdlFlavor] = useState(PLATFORM_TO_FLAVOR[platform] || "auto");
  const [validationResult, setValidationResult] = useState(null);
  const fileRef = useRef();

  useEffect(() => {
    if (platform && PLATFORM_TO_FLAVOR[platform]) {
      setDdlFlavor(PLATFORM_TO_FLAVOR[platform]);
    }
  }, [platform]);

  const parse = () => {
    setError("");
    setValidationResult(null);
    try {
      let schemas;
      if (mode === "ddl") {
        const vResult = validateDDL(text, ddlFlavor);
        setValidationResult(vResult);
        schemas = parseDDL(text);
      }
      else if (mode === "json") schemas = parseJSONSchema(text);
      else if (mode === "xml") schemas = parseXMLSchema(text);
      else return;

      if (Object.keys(schemas).length === 0) {
        setError("No tables found. Check your input format.");
        return;
      }
      setParsedSchemas(schemas);
      const firstSchema = Object.keys(schemas)[0];
      setExpandedSchemas([firstSchema]);
      const sel = {};
      Object.entries(schemas).forEach(([s, tables]) => {
        Object.keys(tables).forEach(t => { sel[`${s}.${t}`] = true; });
      });
      setSelectedTables(sel);
    } catch (e) {
      setError(`Parse error: ${e.message}`);
    }
  };

  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setLoading(true);
    setError("");
    try {
      const { file_url } = await dataflow.integrations.Core.UploadFile({ file });
      const result = await dataflow.integrations.Core.ExtractDataFromUploadedFile({
        file_url,
        json_schema: {
          type: "object",
          properties: {
            tables: {
              type: "array",
              items: {
                type: "object",
                properties: {
                  schema: { type: "string" },
                  table: { type: "string" },
                  columns: {
                    type: "array",
                    items: {
                      type: "object",
                      properties: {
                        name: { type: "string" },
                        type: { type: "string" },
                      }
                    }
                  }
                }
              }
            }
          }
        }
      });

      if (result.status !== "success") throw new Error(result.details || "Extraction failed");

      const rawTables = result.output?.tables || (Array.isArray(result.output) ? result.output : []);
      const schemas = {};
      rawTables.forEach(entry => {
        const s = entry.schema || "dbo";
        const t = entry.table || entry.name;
        if (!t) return;
        if (!schemas[s]) schemas[s] = {};
        schemas[s][t] = (entry.columns || []).map(c =>
          typeof c === "string" ? { name: c, type: "VARCHAR" } : c
        );
      });

      if (Object.keys(schemas).length === 0) throw new Error("No tables could be extracted from the file.");
      setParsedSchemas(schemas);
      const firstSchema = Object.keys(schemas)[0];
      setExpandedSchemas([firstSchema]);
      const sel = {};
      Object.entries(schemas).forEach(([s, tables]) => {
        Object.keys(tables).forEach(t => { sel[`${s}.${t}`] = true; });
      });
      setSelectedTables(sel);
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  };

  const toggleSchema = (s) => setExpandedSchemas(prev =>
    prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s]
  );

  const toggleTable = (s, t) => {
    const key = `${s}.${t}`;
    setSelectedTables(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const toggleAllInSchema = (s, tables) => {
    const allSelected = Object.keys(tables).every(t => selectedTables[`${s}.${t}`]);
    const updates = {};
    Object.keys(tables).forEach(t => { updates[`${s}.${t}`] = !allSelected; });
    setSelectedTables(prev => ({ ...prev, ...updates }));
  };

  const handleImport = () => {
    const objects = [];
    Object.entries(parsedSchemas).forEach(([s, tables]) => {
      Object.keys(tables).forEach(t => {
        if (selectedTables[`${s}.${t}`]) {
          objects.push({ schema: s, table: t, target_path: `/${s}/${t}`, target_format: "original" });
        }
      });
    });
    onImport(objects);
    onClose();
  };

  const selectedCount = Object.values(selectedTables).filter(Boolean).length;

  return (
    <div className="border border-blue-200 rounded-xl bg-blue-50/20 p-4 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Upload className="w-4 h-4 text-blue-600" />
          <span className="font-semibold text-slate-800 text-sm">Import Schema</span>
        </div>
        <button type="button" onClick={onClose} className="text-slate-400 hover:text-slate-600">
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Mode selector */}
      <div className="flex gap-2 flex-wrap">
        {MODES.map(m => (
          <button
            key={m.id}
            type="button"
            onClick={() => { setMode(m.id); setParsedSchemas(null); setError(""); setText(""); setValidationResult(null); }}
            className={cn(
              "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all",
              mode === m.id
                ? "bg-blue-600 text-white border-blue-600"
                : "bg-white text-slate-600 border-slate-200 hover:border-blue-300"
            )}
          >
            <m.icon className="w-3.5 h-3.5" />
            {m.label}
          </button>
        ))}
      </div>

      {/* Input area */}
      {mode === "file" ? (
        <div>
          <input ref={fileRef} type="file" accept=".xlsx,.xls,.csv,.json,.xml" className="hidden" onChange={handleFileUpload} />
          <button
            type="button"
            onClick={() => fileRef.current?.click()}
            disabled={loading}
            className="w-full border-2 border-dashed border-slate-300 rounded-lg py-8 flex flex-col items-center gap-2 text-slate-500 hover:border-blue-400 hover:text-blue-600 transition-colors"
          >
            {loading ? <Loader2 className="w-6 h-6 animate-spin" /> : <Upload className="w-6 h-6" />}
            <span className="text-sm font-medium">{loading ? "Analysing file..." : "Click to upload"}</span>
            <span className="text-xs">Supports Excel (.xlsx), CSV, JSON, XML</span>
          </button>
          <p className="text-xs text-slate-400 mt-2">The file should contain table names and column definitions (with optional types).</p>
        </div>
      ) : (
        <div className="space-y-2">
          {mode === "ddl" && (
            <div className="flex items-center gap-2">
              <Database className="w-3.5 h-3.5 text-slate-500" />
              <Label className="text-xs text-slate-600 shrink-0">Database Flavor</Label>
              <Select value={ddlFlavor} onValueChange={(v) => { setDdlFlavor(v); setValidationResult(null); }}>
                <SelectTrigger className="h-7 text-xs w-40">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DDL_FLAVORS.map(f => (
                    <SelectItem key={f.id} value={f.id}>{f.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {platform && PLATFORM_TO_FLAVOR[platform] && (
                <span className="text-xs text-slate-400 ml-1">
                  (from connection)
                </span>
              )}
            </div>
          )}
          <Label className="text-xs text-slate-600">
            {mode === "ddl" && "Paste CREATE TABLE SQL statements"}
            {mode === "json" && 'JSON format: [{ "schema": "dbo", "table": "Users", "columns": [{...}] }]'}
            {mode === "xml" && '<schema name="dbo"><table name="Users"><column name="id" type="INT"/></table></schema>'}
          </Label>
          <Textarea
            value={text}
            onChange={e => setText(e.target.value)}
            placeholder={
              mode === "ddl"
                ? `CREATE TABLE dbo.Customers (\n  id INT PRIMARY KEY,\n  name VARCHAR(255),\n  email VARCHAR(255)\n);`
                : mode === "json"
                ? `[{"schema":"dbo","table":"Customers","columns":[{"name":"id","type":"INT"},{"name":"name","type":"VARCHAR"}]}]`
                : `<schema name="dbo">\n  <table name="Customers">\n    <column name="id" type="INT"/>\n    <column name="name" type="VARCHAR"/>\n  </table>\n</schema>`
            }
            className="font-mono text-xs min-h-[120px] bg-white"
          />
          <Button type="button" size="sm" onClick={parse} disabled={!text.trim()} className="gap-1.5">
            <Code2 className="w-3.5 h-3.5" />
            Parse Schema
          </Button>
        </div>
      )}

      {error && (
        <div className="flex items-start gap-2 text-red-600 bg-red-50 border border-red-200 rounded-lg p-3 text-xs">
          <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
          {error}
        </div>
      )}

      {validationResult && validationResult.warnings.length > 0 && (
        <div className="space-y-1.5">
          {validationResult.detectedFlavor && ddlFlavor === "auto" && (
            <div className="flex items-center gap-1.5 text-xs text-blue-600 bg-blue-50 border border-blue-200 rounded-lg px-3 py-2">
              <Database className="w-3.5 h-3.5 shrink-0" />
              Auto-detected flavor: <span className="font-medium">{DDL_FLAVORS.find(f => f.id === validationResult.detectedFlavor)?.label || validationResult.detectedFlavor}</span>
            </div>
          )}
          {validationResult.warnings.map((w, i) => (
            <div key={`w-${i}`} className="flex items-start gap-2 text-amber-700 bg-amber-50 border border-amber-200 rounded-lg p-2.5 text-xs">
              <AlertTriangle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
              {w}
            </div>
          ))}
        </div>
      )}

      {validationResult && validationResult.errors.length > 0 && (
        <div className="space-y-1.5">
          {validationResult.errors.map((e, i) => (
            <div key={`e-${i}`} className="flex items-start gap-2 text-red-600 bg-red-50 border border-red-200 rounded-lg p-2.5 text-xs">
              <AlertCircle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
              {e}
            </div>
          ))}
        </div>
      )}

      {validationResult && validationResult.detectedFlavor && ddlFlavor === "auto" && validationResult.warnings.length === 0 && validationResult.errors.length === 0 && (
        <div className="flex items-center gap-1.5 text-xs text-blue-600 bg-blue-50 border border-blue-200 rounded-lg px-3 py-2">
          <Database className="w-3.5 h-3.5 shrink-0" />
          Auto-detected flavor: <span className="font-medium">{DDL_FLAVORS.find(f => f.id === validationResult.detectedFlavor)?.label || validationResult.detectedFlavor}</span>
        </div>
      )}

      {/* Parsed result */}
      {parsedSchemas && (
        <div className="space-y-3">
          <div className="border border-slate-200 rounded-lg overflow-hidden bg-white max-h-[280px] overflow-y-auto">
            {Object.entries(parsedSchemas).map(([schemaName, tables]) => {
              const tableList = Object.keys(tables);
              const allSelected = tableList.every(t => selectedTables[`${schemaName}.${t}`]);
              const isExpanded = expandedSchemas.includes(schemaName);
              return (
                <div key={schemaName}>
                  <div
                    className="flex items-center gap-2 px-3 py-2 bg-slate-50 border-b border-slate-200 cursor-pointer hover:bg-slate-100"
                    onClick={() => toggleSchema(schemaName)}
                  >
                    <input
                      type="checkbox"
                      checked={allSelected}
                      onChange={() => toggleAllInSchema(schemaName, tables)}
                      onClick={e => e.stopPropagation()}
                      className="rounded"
                    />
                    {isExpanded ? <ChevronDown className="w-4 h-4 text-slate-400" /> : <ChevronUp className="w-4 h-4 text-slate-400" />}
                    <span className="font-medium text-slate-700 text-sm">{schemaName}</span>
                    <span className="text-xs text-slate-400 ml-auto">{tableList.length} tables</span>
                  </div>
                  {isExpanded && tableList.map(t => (
                    <div key={t} className="flex items-center gap-2 pl-8 pr-3 py-1.5 border-b border-slate-50 hover:bg-slate-50">
                      <input
                        type="checkbox"
                        checked={!!selectedTables[`${schemaName}.${t}`]}
                        onChange={() => toggleTable(schemaName, t)}
                        className="rounded"
                      />
                      <Table2 className="w-3.5 h-3.5 text-slate-400" />
                      <span className="text-sm text-slate-700 flex-1">{t}</span>
                      <span className="text-xs text-slate-400">{tables[t]?.length || 0} cols</span>
                    </div>
                  ))}
                </div>
              );
            })}
          </div>

          <div className="flex items-center justify-between">
            <span className="text-xs text-slate-500">{selectedCount} tables selected</span>
            <div className="flex gap-2">
              <Button type="button" variant="outline" size="sm" onClick={onClose}>Cancel</Button>
              <Button
                type="button"
                size="sm"
                onClick={handleImport}
                disabled={selectedCount === 0}
                className="gap-1.5 bg-blue-600 hover:bg-blue-700"
              >
                <Check className="w-3.5 h-3.5" />
                Import {selectedCount} Table{selectedCount !== 1 ? "s" : ""}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}