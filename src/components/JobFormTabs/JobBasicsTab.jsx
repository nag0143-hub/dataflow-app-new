import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { platformConfig } from "@/components/PlatformIcon";
import HelpTooltip from "@/components/HelpTooltip";
import { ArrowRight, AlertCircle, Tag, X } from "lucide-react";
import { Button } from "@/components/ui/button";

const FLAT_FILE_PLATFORMS = ["flat_file_delimited", "flat_file_fixed_width", "cobol_ebcdic", "sftp", "nas", "local_fs"];

const TAG_COLORS = {
  prod: "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300",
  production: "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300",
  dev: "bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300",
  development: "bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300",
  uat: "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300",
  staging: "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300",
  test: "bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300",
};

function ConnectionCard({ conn }) {
  const cfg = platformConfig[conn?.platform] || {};
  const Icon = cfg.icon;
  const tags = conn?.tags || [];
  return (
    <div className="flex items-center gap-2 py-0.5">
      {Icon && <Icon className={`w-4 h-4 ${cfg.color || "text-slate-400"}`} />}
      <span className="font-medium">{conn?.name}</span>
      <span className="text-slate-400 text-xs">({cfg.label || conn?.platform})</span>
      {tags.slice(0, 2).map(tag => (
        <span key={tag} className={`text-[10px] font-medium px-1.5 py-0.5 rounded ${TAG_COLORS[tag.toLowerCase()] || "bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400"}`}>
          {tag}
        </span>
      ))}
    </div>
  );
}

function FieldLabel({ children, required, help }) {
  return (
    <div className="flex items-center gap-1 mb-1.5">
      <Label className="text-sm font-medium text-slate-700 dark:text-slate-300">
        {children}{required && <span className="text-red-500 ml-0.5">*</span>}
      </Label>
      {help && <HelpTooltip text={help} />}
    </div>
  );
}

export default function JobBasicsTab({ formData, setFormData, sourceConnections, targetConnections, errors = {} }) {
  const srcConn = sourceConnections.find(c => c.id === formData.source_connection_id);
  const tgtConn = targetConnections.find(c => c.id === formData.target_connection_id);
  const sameConn = formData.source_connection_id && formData.source_connection_id === formData.target_connection_id;

  const handleSourceChange = (v) => {
    const prevConn = sourceConnections.find(c => c.id === formData.source_connection_id);
    const nextConn = sourceConnections.find(c => c.id === v);
    const wasFile = prevConn && FLAT_FILE_PLATFORMS.includes(prevConn.platform);
    const isFile = nextConn && FLAT_FILE_PLATFORMS.includes(nextConn.platform);

    const updates = { source_connection_id: v };
    if (wasFile && !isFile) {
      updates.file_source_mode = undefined;
      updates.file_source_list = undefined;
      updates.file_source_folder = undefined;
      updates.file_source_extension = undefined;
      updates.file_source_wildcard = undefined;
      updates.file_source_base_dir = undefined;
      updates.file_source_target_path = undefined;
    }
    if (!wasFile && isFile) {
      updates.selected_datasets = [];
    }
    setFormData({ ...formData, ...updates });
  };

  return (
    <div className="space-y-5">
      {/* Pipeline Name */}
      <div>
        <FieldLabel required help="A short, descriptive name for this pipeline. Used in logs, notifications, and the generated DAG ID.">
          Pipeline Name
        </FieldLabel>
        <Input
          value={formData.name}
          onChange={(e) => setFormData({ ...formData, name: e.target.value })}
          placeholder="e.g. Daily Sales Sync, CRM to Warehouse"
          className={errors.name ? "border-red-400 focus-visible:ring-red-400" : ""}
        />
        {errors.name
          ? <p className="text-xs text-red-600 mt-1 flex items-center gap-1"><AlertCircle className="w-3 h-3" />{errors.name}</p>
          : <p className="text-xs text-slate-400 mt-1">Keep it short and meaningful — this becomes the Airflow DAG ID.</p>
        }
      </div>

      {/* Description */}
      <div>
        <FieldLabel help="Optional description of what this pipeline does, why it exists, and any special notes.">
          Description
        </FieldLabel>
        <Textarea
          value={formData.description}
          onChange={(e) => setFormData({ ...formData, description: e.target.value })}
          placeholder="Describe the purpose, source system, and business context of this pipeline..."
          rows={2}
        />
      </div>

      {/* Source → Target */}
      <div className="space-y-3">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-sm font-semibold text-slate-700 dark:text-slate-300">Data Flow</span>
          <HelpTooltip text="Define where data comes FROM (source) and where it goes TO (target). Only connections of the matching type are shown." />
        </div>

        <div className="grid grid-cols-[1fr_auto_1fr] gap-3 items-start">
          {/* Source */}
          <div className="space-y-1.5">
            <Label className="text-xs text-slate-500 uppercase tracking-wide font-semibold">
              📤 Source System
            </Label>
            <Select
              value={formData.source_connection_id}
              onValueChange={handleSourceChange}
            >
              <SelectTrigger className={sameConn || errors.source_connection_id ? "border-red-400" : ""}>
                <SelectValue placeholder="Select source connection">
                  {srcConn && <ConnectionCard conn={srcConn} />}
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                {sourceConnections.length === 0 && (
                  <div className="px-3 py-2 text-xs text-slate-400">No source connections configured</div>
                )}
                {sourceConnections.map(c => (
                  <SelectItem key={c.id} value={c.id}>
                    <ConnectionCard conn={c} />
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {errors.source_connection_id
              ? <p className="text-xs text-red-600 mt-1 flex items-center gap-1"><AlertCircle className="w-3 h-3" />{errors.source_connection_id}</p>
              : <p className="text-xs text-slate-400">Where data is read from</p>
            }
          </div>

          {/* Arrow */}
          <div className="flex items-center justify-center pt-7">
            <div className="bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700 rounded-full p-1.5">
              <ArrowRight className="w-4 h-4 text-blue-500" />
            </div>
          </div>

          {/* Target */}
          <div className="space-y-1.5">
            <Label className="text-xs text-slate-500 uppercase tracking-wide font-semibold">
              📥 Target System
            </Label>
            <Select
              value={formData.target_connection_id}
              onValueChange={(v) => setFormData({ ...formData, target_connection_id: v })}
            >
              <SelectTrigger className={sameConn || errors.target_connection_id ? "border-red-400" : ""}>
                <SelectValue placeholder="Select target connection">
                  {tgtConn && <ConnectionCard conn={tgtConn} />}
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                {targetConnections.length === 0 && (
                  <div className="px-3 py-2 text-xs text-slate-400">No target connections configured</div>
                )}
                {targetConnections.map(c => (
                  <SelectItem key={c.id} value={c.id}>
                    <ConnectionCard conn={c} />
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {errors.target_connection_id
              ? <p className="text-xs text-red-600 mt-1 flex items-center gap-1"><AlertCircle className="w-3 h-3" />{errors.target_connection_id}</p>
              : <p className="text-xs text-slate-400">Where data is written to</p>
            }
          </div>
        </div>

        {sameConn && (
          <div className="flex items-center gap-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-3 py-2">
            <AlertCircle className="w-3.5 h-3.5 shrink-0" />
            Source and target must be different connections.
          </div>
        )}
      </div>

      {/* Organization */}
      <details className="border border-slate-200 dark:border-slate-700 rounded-xl">
        <summary className="cursor-pointer font-medium text-sm text-slate-700 dark:text-slate-300 hover:text-slate-900 dark:hover:text-white px-4 py-3 flex items-center justify-between select-none">
          <span>Organization & Notifications</span>
          <span className="text-xs text-slate-400 font-normal">Optional</span>
        </summary>
        <div className="grid grid-cols-2 gap-3 px-4 pb-4 pt-3 border-t border-slate-100 dark:border-slate-700">
          <div>
            <FieldLabel help="The team responsible for this pipeline — used in Airflow DAG metadata and alerting.">
              Assignment Group
            </FieldLabel>
            <Input
              value={formData.assignment_group}
              onChange={(e) => setFormData({ ...formData, assignment_group: e.target.value })}
              placeholder="Data Engineering"
              className="text-sm"
            />
          </div>
          <div>
            <FieldLabel help="Internal cost center for chargeback or cost allocation.">
              Cost Center
            </FieldLabel>
            <Input
              value={formData.cost_center}
              onChange={(e) => setFormData({ ...formData, cost_center: e.target.value })}
              placeholder="CC-12345"
              className="text-sm"
            />
          </div>
          <div className="col-span-2">
            <FieldLabel help="Email address to notify on pipeline failure or SLA breach.">
              Failure Alert Email
            </FieldLabel>
            <Input
              type="email"
              value={formData.email}
              onChange={(e) => setFormData({ ...formData, email: e.target.value })}
              placeholder="alerts@company.com"
              className="text-sm"
            />
          </div>
          <div className="col-span-2">
            <FieldLabel help="Tags help you organize and group pipelines. Use them to filter and search.">
              Tags
            </FieldLabel>
            <div className="flex flex-wrap gap-1.5 mb-2">
              {(formData.tags || []).map(tag => (
                <span key={tag} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border border-blue-100 dark:border-blue-800">
                  <Tag className="w-3 h-3" />
                  {tag}
                  <button type="button" onClick={() => setFormData({ ...formData, tags: (formData.tags || []).filter(t => t !== tag) })} className="hover:text-red-500">
                    <X className="w-3 h-3" />
                  </button>
                </span>
              ))}
            </div>
            <div className="flex gap-2">
              <Input
                value={formData._tagInput || ""}
                onChange={e => setFormData({ ...formData, _tagInput: e.target.value })}
                placeholder="Add a tag..."
                className="h-8 text-sm"
                onKeyDown={e => {
                  if ((e.key === "Enter" || e.key === ",") && (formData._tagInput || "").trim()) {
                    e.preventDefault();
                    const newTag = formData._tagInput.trim();
                    if (!(formData.tags || []).includes(newTag)) {
                      setFormData({ ...formData, tags: [...(formData.tags || []), newTag], _tagInput: "" });
                    } else {
                      setFormData({ ...formData, _tagInput: "" });
                    }
                  }
                }}
              />
              <Button type="button" size="sm" variant="outline" className="h-8 text-xs" onClick={() => {
                const newTag = (formData._tagInput || "").trim();
                if (newTag && !(formData.tags || []).includes(newTag)) {
                  setFormData({ ...formData, tags: [...(formData.tags || []), newTag], _tagInput: "" });
                } else {
                  setFormData({ ...formData, _tagInput: "" });
                }
              }}>Add</Button>
            </div>
          </div>
        </div>
      </details>
    </div>
  );
}