import { useState } from "react";
import { dataflow } from '@/api/client';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Zap, X, Save } from "lucide-react";
import { Badge } from "@/components/ui/badge";

export default function CustomFunctionQuickAdd({ customFunctions, onFunctionAdded }) {
  const [open, setOpen] = useState(false);
  const [form, setForm] = useState({ name: "", label: "", category: "spark_udf", expression_template: "" });
  const [saving, setSaving] = useState(false);

  const save = async () => {
    if (!form.name || !form.label) return;
    setSaving(true);
    await dataflow.entities.CustomFunction.create(form);
    setSaving(false);
    setForm({ name: "", label: "", category: "spark_udf", expression_template: "" });
    setOpen(false);
    onFunctionAdded?.();
  };

  return (
    <div className="border border-slate-200 rounded-lg bg-slate-50/50 p-3">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <Zap className="w-3.5 h-3.5 text-[#0060AF]" />
          <span className="text-xs font-semibold text-slate-700">Custom Functions</span>
          <span className="text-xs text-slate-400">({customFunctions.length} loaded)</span>
        </div>
        <Button
          size="sm"
          variant="outline"
          className="h-6 text-xs gap-1 border-[#0060AF]/30 text-[#0060AF] hover:bg-blue-50 dark:hover:bg-blue-900/20"
          onClick={() => setOpen(!open)}
        >
          {open ? <X className="w-3 h-3" /> : <Plus className="w-3 h-3" />}
          {open ? "Cancel" : "Add Function"}
        </Button>
      </div>

      {/* Existing functions list */}
      {customFunctions.length > 0 && (
        <div className="flex flex-wrap gap-1 mb-2">
          {customFunctions.map(fn => (
            <Badge key={fn.value} className="text-[10px] bg-blue-100 text-[#0060AF] dark:bg-blue-900/40 dark:text-blue-300 hover:bg-blue-100 font-mono">
              {fn.label}
            </Badge>
          ))}
        </div>
      )}

      {!open && customFunctions.length === 0 && (
        <p className="text-xs text-slate-400">No custom functions yet. Add one to use in transform dropdown.</p>
      )}

      {/* Quick-add form */}
      {open && (
        <div className="mt-2 space-y-2 border-t border-slate-200 pt-2">
          <div className="grid grid-cols-2 gap-2">
            <div>
              <p className="text-[10px] text-slate-500 mb-0.5">Key (no spaces)</p>
              <Input
                value={form.name}
                onChange={e => setForm(f => ({ ...f, name: e.target.value.replace(/\s+/g, "_") }))}
                placeholder="my_udf"
                className="h-7 text-xs font-mono"
              />
            </div>
            <div>
              <p className="text-[10px] text-slate-500 mb-0.5">Display Label</p>
              <Input
                value={form.label}
                onChange={e => setForm(f => ({ ...f, label: e.target.value }))}
                placeholder="My UDF(col)"
                className="h-7 text-xs"
              />
            </div>
            <div>
              <p className="text-[10px] text-slate-500 mb-0.5">Category</p>
              <Select value={form.category} onValueChange={v => setForm(f => ({ ...f, category: v }))}>
                <SelectTrigger className="h-7 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="spark_udf">Spark UDF</SelectItem>
                  <SelectItem value="custom_expression">Custom Expression</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <p className="text-[10px] text-slate-500 mb-0.5">Expression Template</p>
              <Input
                value={form.expression_template}
                onChange={e => setForm(f => ({ ...f, expression_template: e.target.value }))}
                placeholder="my_udf({col})"
                className="h-7 text-xs font-mono"
              />
            </div>
          </div>
          <div className="flex gap-2">
            <Button
              size="sm"
              className="h-7 text-xs gap-1 bg-[#0060AF] hover:bg-[#004d8c] dark:bg-[#0060AF] dark:hover:bg-[#004d8c]"
              disabled={!form.name || !form.label || saving}
              onClick={save}
            >
              <Save className="w-3 h-3" /> {saving ? "Saving…" : "Save"}
            </Button>
            <p className="text-[10px] text-slate-400 self-center">
              Saved functions appear in the Transform dropdown instantly.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}