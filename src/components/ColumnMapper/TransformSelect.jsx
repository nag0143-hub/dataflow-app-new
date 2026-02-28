import { useState, useRef, useEffect } from "react";
import { Search, ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";

/**
 * Searchable dropdown for selecting a transformation function.
 * Groups options by category and supports keyboard navigation.
 */
export default function TransformSelect({ value, onChange, options = [], disabled = false }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const containerRef = useRef(null);
  const inputRef = useRef(null);

  const selected = options.find(o => o.value === value) || options[0];

  // Close on outside click
  useEffect(() => {
    function handle(e) {
      if (containerRef.current && !containerRef.current.contains(e.target)) {
        setOpen(false);
        setQuery("");
      }
    }
    document.addEventListener("mousedown", handle);
    return () => document.removeEventListener("mousedown", handle);
  }, []);

  useEffect(() => {
    if (open && inputRef.current) inputRef.current.focus();
  }, [open]);

  const filtered = query
    ? options.filter(o =>
        o.label.toLowerCase().includes(query.toLowerCase()) ||
        (o.category && o.category.toLowerCase().includes(query.toLowerCase())) ||
        o.value.toLowerCase().includes(query.toLowerCase())
      )
    : options;

  // Group by category
  const groups = filtered.reduce((acc, o) => {
    const cat = o.category || "Other";
    if (!acc[cat]) acc[cat] = [];
    acc[cat].push(o);
    return acc;
  }, {});

  const CATEGORY_LABELS = {
    "core":         "Direct Copy",
    "spark_native": "Spark Native",
    "spark_udf":    "Spark UDF",
    "custom":       "Custom / Other",
    "Other":        "Other",
  };

  return (
    <div ref={containerRef} className="relative w-full">
      <button
        type="button"
        disabled={disabled}
        onClick={() => setOpen(v => !v)}
        className={cn(
          "w-full h-7 px-2 text-xs flex items-center justify-between gap-1 rounded transition-all",
          "border-0 bg-transparent focus:bg-white focus:border focus:border-blue-400 focus:outline-none hover:bg-white/70",
          disabled && "opacity-50 cursor-not-allowed"
        )}
      >
        <span className="truncate text-left">{selected?.label || "Direct Copy"}</span>
        <ChevronDown className={cn("w-3 h-3 text-slate-400 shrink-0 transition-transform", open && "rotate-180")} />
      </button>

      {open && (
        <div className="absolute z-50 left-0 top-8 w-56 bg-white border border-slate-200 rounded-lg shadow-lg overflow-hidden">
          {/* Search box */}
          <div className="flex items-center gap-1.5 px-2 py-1.5 border-b border-slate-100">
            <Search className="w-3 h-3 text-slate-400 shrink-0" />
            <input
              ref={inputRef}
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder="Search functions..."
              className="w-full text-xs bg-transparent outline-none placeholder:text-slate-400"
              onKeyDown={e => e.key === "Escape" && (setOpen(false), setQuery(""))}
            />
          </div>

          {/* Options grouped by category */}
          <div className="max-h-60 overflow-y-auto">
            {Object.keys(groups).length === 0 && (
              <div className="px-3 py-4 text-xs text-slate-400 text-center">No functions found</div>
            )}
            {Object.entries(groups).map(([cat, items]) => (
              <div key={cat}>
                <div className="px-3 py-1 text-[10px] font-semibold text-slate-400 uppercase tracking-wider bg-slate-50 sticky top-0">
                  {CATEGORY_LABELS[cat] || cat}
                </div>
                {items.map(o => (
                  <button
                    key={o.value}
                    type="button"
                    onClick={() => { onChange(o.value); setOpen(false); setQuery(""); }}
                    className={cn(
                      "w-full text-left px-3 py-1.5 text-xs hover:bg-blue-50 transition-colors",
                      o.value === value && "bg-blue-100 text-blue-800 font-medium"
                    )}
                  >
                    {o.label}
                  </button>
                ))}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}