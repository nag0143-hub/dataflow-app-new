import { useState } from "react";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import { Clock, Calendar, Zap, Settings2, Layers, ChevronDown, CalendarDays, CalendarOff, Columns, FileText, Shield, ShieldCheck, CheckSquare, FolderOpen } from "lucide-react";
import dataflowConfig from "@/dataflow-config";

const SCHEDULE_PRESETS = [
  { value: "manual",       label: "None (Manual)",   icon: Clock,    cron: null,            desc: "Triggered manually or via API only" },
  { value: "hourly",       label: "Hourly",          icon: Clock,    cron: "0 * * * *",     desc: "Runs once every hour at minute :00" },
  { value: "daily",        label: "Daily",           icon: Calendar, cron: "0 6 * * *",     desc: "Runs once per day at 06:00 UTC" },
  { value: "weekly",       label: "Weekly",          icon: Calendar, cron: "0 6 * * 1",     desc: "Runs every Monday at 06:00 UTC" },
  { value: "monthly",      label: "Monthly",         icon: Calendar, cron: "0 6 1 * *",     desc: "Runs on the 1st of each month" },
  { value: "quarterly",    label: "Quarterly",       icon: Calendar, cron: "0 6 1 1,4,7,10 *", desc: "Runs on the 1st of each quarter" },
  { value: "custom",       label: "Cron Expression", icon: Settings2, cron: null,           desc: "Define a custom cron schedule" },
  { value: "event_driven", label: "Sensor / Event",  icon: Zap,      cron: null,            desc: "Triggered by file arrival, DB condition, or upstream DAG" },
];

const DAYS_OF_WEEK = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

const EVENT_SENSOR_OPTS = (dataflowConfig.pipeline_wizard?.event_sensor_options || []).map(o => ({
  value: o.value,
  label: o.label,
  desc: o.description,
}));

function SectionCard({ icon: Icon, title, children, collapsible = false, defaultOpen = true }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="border border-slate-200 dark:border-slate-700 rounded-xl overflow-hidden">
      <button
        type="button"
        onClick={collapsible ? () => setOpen(v => !v) : undefined}
        className={cn(
          "w-full flex items-center gap-2.5 px-4 py-3 text-left",
          collapsible && "cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-800/50",
          !collapsible && "cursor-default"
        )}
      >
        <Icon className="w-4 h-4 text-[#0060AF] shrink-0" />
        <span className="text-sm font-semibold text-slate-800 dark:text-slate-200 flex-1">{title}</span>
        {collapsible && (
          <ChevronDown className={cn("w-4 h-4 text-slate-400 transition-transform", open && "rotate-180")} />
        )}
      </button>
      {(!collapsible || open) && (
        <div className="px-4 pb-4 space-y-3 border-t border-slate-100 dark:border-slate-700 pt-3">
          {children}
        </div>
      )}
    </div>
  );
}

function CronHelper({ value }) {
  if (!value) return null;
  const parts = (value || "").split(/\s+/);
  if (parts.length !== 5) return null;
  const [min, hr, dom, mon, dow] = parts;
  const segments = [];
  if (min === "0" && hr === "*") segments.push("Every hour at :00");
  else if (min.startsWith("*/")) segments.push(`Every ${min.slice(2)} minutes`);
  else if (hr.startsWith("*/")) segments.push(`Every ${hr.slice(2)} hours at :${min.padStart(2, "0")}`);
  else {
    segments.push(`At ${hr.padStart(2, "0")}:${min.padStart(2, "0")} UTC`);
  }
  if (dow !== "*") {
    const days = dow.split(",").map(d => {
      if (d.includes("-")) {
        const [s, e] = d.split("-");
        return `${DAYS_OF_WEEK[s] || s}–${DAYS_OF_WEEK[e] || e}`;
      }
      return DAYS_OF_WEEK[d] || d;
    });
    segments.push(`on ${days.join(", ")}`);
  }
  if (dom !== "*") segments.push(`day ${dom} of month`);
  if (mon !== "*") segments.push(`in month ${mon}`);
  return (
    <p className="text-xs text-slate-500 dark:text-slate-400 bg-slate-50 dark:bg-slate-800 rounded-lg px-3 py-2 font-mono">
      <span className="text-[#0060AF] font-semibold">{value}</span>
      <span className="text-slate-400 mx-2">&mdash;</span>
      {segments.join(" ")}
    </p>
  );
}

export default function ScheduleSettings({ formData, setFormData }) {
  const schedType = formData.schedule_type || "manual";
  const update = (patch) => setFormData(prev => ({ ...prev, ...patch }));
  const cronParts = (formData.cron_expression || "0 6 * * *").split(/\s+/);

  return (
    <div className="space-y-4">

      <SectionCard icon={Clock} title="Schedule Interval">
        <div className="grid grid-cols-4 gap-2">
          {SCHEDULE_PRESETS.map(p => {
            const Icon = p.icon;
            const active = schedType === p.value;
            return (
              <button
                key={p.value}
                type="button"
                onClick={() => {
                  const patch = { schedule_type: p.value };
                  if (p.cron) patch.cron_expression = p.cron;
                  update(patch);
                }}
                className={cn(
                  "flex flex-col items-center gap-1.5 px-3 py-3 rounded-lg border text-center transition-all",
                  active
                    ? "bg-[#0060AF] text-white border-[#0060AF] shadow-sm shadow-[#0060AF]/20"
                    : "bg-white dark:bg-slate-800 text-slate-600 dark:text-slate-400 border-slate-200 dark:border-slate-700 hover:border-[#0060AF]/40 hover:bg-slate-50 dark:hover:bg-slate-800/80"
                )}
              >
                <Icon className={cn("w-4 h-4", active ? "text-white" : "text-slate-400")} />
                <span className="text-xs font-medium leading-tight">{p.label}</span>
              </button>
            );
          })}
        </div>

        {schedType !== "manual" && schedType !== "event_driven" && (
          <p className="text-xs text-slate-400 dark:text-slate-500">
            {SCHEDULE_PRESETS.find(p => p.value === schedType)?.desc}
          </p>
        )}

        {schedType === "manual" && (
          <p className="text-xs text-slate-400">This pipeline will only run when triggered manually or via REST API.</p>
        )}

        {schedType === "hourly" && (
          <div className="space-y-2">
            <Label className="text-xs text-slate-600 dark:text-slate-400">Run at minute</Label>
            <Select
              value={cronParts[0] || "0"}
              onValueChange={v => update({ cron_expression: `${v} * * * *` })}
            >
              <SelectTrigger className="w-32 h-8 text-xs"><SelectValue /></SelectTrigger>
              <SelectContent>
                {[0, 5, 10, 15, 20, 30, 45].map(m => (
                  <SelectItem key={m} value={String(m)}>:{String(m).padStart(2, "0")} past the hour</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <CronHelper value={formData.cron_expression} />
          </div>
        )}

        {schedType === "daily" && (
          <div className="space-y-2">
            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Hour (UTC)</Label>
                <Select
                  value={cronParts[1] || "6"}
                  onValueChange={v => update({ cron_expression: `${cronParts[0] || "0"} ${v} * * *` })}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Array.from({ length: 24 }, (_, i) => i).map(h => (
                      <SelectItem key={h} value={String(h)}>{String(h).padStart(2, "0")}:00 UTC</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Minute</Label>
                <Select
                  value={cronParts[0] || "0"}
                  onValueChange={v => update({ cron_expression: `${v} ${cronParts[1] || "6"} * * *` })}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {[0, 5, 10, 15, 20, 30, 45].map(m => (
                      <SelectItem key={m} value={String(m)}>:{String(m).padStart(2, "0")}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <CronHelper value={formData.cron_expression} />
          </div>
        )}

        {schedType === "weekly" && (
          <div className="space-y-2">
            <div className="grid grid-cols-3 gap-3">
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Day of Week</Label>
                <Select
                  value={cronParts[4] || "1"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[4] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {DAYS_OF_WEEK.map((d, i) => (
                      <SelectItem key={i} value={String(i)}>{d}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Hour (UTC)</Label>
                <Select
                  value={cronParts[1] || "6"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[1] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Array.from({ length: 24 }, (_, i) => i).map(h => (
                      <SelectItem key={h} value={String(h)}>{String(h).padStart(2, "0")}:00</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Minute</Label>
                <Select
                  value={cronParts[0] || "0"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[0] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {[0, 5, 10, 15, 20, 30, 45].map(m => (
                      <SelectItem key={m} value={String(m)}>:{String(m).padStart(2, "0")}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <CronHelper value={formData.cron_expression} />
          </div>
        )}

        {schedType === "monthly" && (
          <div className="space-y-2">
            <div className="grid grid-cols-3 gap-3">
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Day of Month</Label>
                <Select
                  value={cronParts[2] || "1"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[2] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Array.from({ length: 31 }, (_, i) => i + 1).map(d => (
                      <SelectItem key={d} value={String(d)}>{d}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Hour (UTC)</Label>
                <Select
                  value={cronParts[1] || "6"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[1] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Array.from({ length: 24 }, (_, i) => i).map(h => (
                      <SelectItem key={h} value={String(h)}>{String(h).padStart(2, "0")}:00</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Minute</Label>
                <Select
                  value={cronParts[0] || "0"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[0] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {[0, 5, 10, 15, 20, 30, 45].map(m => (
                      <SelectItem key={m} value={String(m)}>:{String(m).padStart(2, "0")}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <CronHelper value={formData.cron_expression} />
          </div>
        )}

        {schedType === "quarterly" && (
          <div className="space-y-2">
            <div className="grid grid-cols-3 gap-3">
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Day of Quarter Start</Label>
                <Select
                  value={cronParts[2] || "1"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[2] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Array.from({ length: 28 }, (_, i) => i + 1).map(d => (
                      <SelectItem key={d} value={String(d)}>{d}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Hour (UTC)</Label>
                <Select
                  value={cronParts[1] || "6"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[1] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {Array.from({ length: 24 }, (_, i) => i).map(h => (
                      <SelectItem key={h} value={String(h)}>{String(h).padStart(2, "0")}:00</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label className="text-xs text-slate-600 dark:text-slate-400">Minute</Label>
                <Select
                  value={cronParts[0] || "0"}
                  onValueChange={v => {
                    const p = [...cronParts]; p[0] = v;
                    update({ cron_expression: p.join(" ") });
                  }}
                >
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {[0, 5, 10, 15, 20, 30, 45].map(m => (
                      <SelectItem key={m} value={String(m)}>:{String(m).padStart(2, "0")}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <p className="text-[10px] text-slate-400">Runs on Jan 1, Apr 1, Jul 1, Oct 1 (quarter boundaries)</p>
            <CronHelper value={formData.cron_expression} />
          </div>
        )}

        {schedType === "custom" && (
          <div className="space-y-2">
            <Label className="text-xs text-slate-600 dark:text-slate-400">Cron Expression</Label>
            <Input
              value={formData.cron_expression || ""}
              onChange={e => update({ cron_expression: e.target.value })}
              placeholder="0 6 * * 1-5"
              className="font-mono text-sm h-9"
            />
            <div className="grid grid-cols-5 gap-1">
              {["Minute (0-59)", "Hour (0-23)", "Day (1-31)", "Month (1-12)", "Weekday (0-6)"].map((label, i) => (
                <p key={i} className="text-[10px] text-center text-slate-400 dark:text-slate-500">{label}</p>
              ))}
            </div>
            <CronHelper value={formData.cron_expression} />
            <div className="flex flex-wrap gap-1.5">
              {[
                { label: "Weekdays 6am", cron: "0 6 * * 1-5" },
                { label: "Every 4 hours", cron: "0 */4 * * *" },
                { label: "Twice daily", cron: "0 6,18 * * *" },
                { label: "Quarterly", cron: "0 6 1 1,4,7,10 *" },
                { label: "Last day of month", cron: "0 6 28-31 * *" },
              ].map(q => (
                <button
                  key={q.cron}
                  type="button"
                  onClick={() => update({ cron_expression: q.cron })}
                  className="px-2 py-1 text-[10px] font-medium rounded border border-slate-200 dark:border-slate-700 text-slate-500 dark:text-slate-400 hover:border-[#0060AF] hover:text-[#0060AF] transition-colors"
                >
                  {q.label}
                </button>
              ))}
            </div>
          </div>
        )}

        {schedType === "event_driven" && (
          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-2">
              {EVENT_SENSOR_OPTS.map(opt => {
                const active = formData.event_sensor_type === opt.value;
                return (
                  <button
                    key={opt.value}
                    type="button"
                    onClick={() => update({ event_sensor_type: opt.value })}
                    className={cn(
                      "flex items-start gap-2.5 px-3 py-2.5 rounded-lg border text-left transition-all",
                      active
                        ? "bg-[#0060AF]/5 border-[#0060AF] dark:bg-[#0060AF]/10"
                        : "bg-white dark:bg-slate-800 border-slate-200 dark:border-slate-700 hover:border-[#0060AF]/40"
                    )}
                  >
                    <div className={cn(
                      "w-3 h-3 rounded-full border-2 mt-0.5 shrink-0",
                      active ? "border-[#0060AF] bg-[#0060AF]" : "border-slate-300 dark:border-slate-600"
                    )} />
                    <div>
                      <p className={cn("text-xs font-semibold", active ? "text-[#0060AF]" : "text-slate-700 dark:text-slate-300")}>{opt.label}</p>
                      <p className="text-[10px] text-slate-400 leading-tight mt-0.5">{opt.desc}</p>
                    </div>
                  </button>
                );
              })}
            </div>

            {formData.event_sensor_type && (
              <div className="space-y-3 pt-3 border-t border-slate-100 dark:border-slate-700">
                {(formData.event_sensor_type === "file_watcher" || formData.event_sensor_type === "sftp_sensor") && (
                  <div>
                    <Label className="text-xs text-slate-600 dark:text-slate-400">Watch Path / Glob Pattern</Label>
                    <Input
                      value={formData.event_config?.watch_path || ""}
                      onChange={e => update({ event_config: { ...formData.event_config, watch_path: e.target.value } })}
                      placeholder="/data/inbound/*.csv"
                      className="font-mono text-sm h-8 mt-1"
                    />
                  </div>
                )}
                {formData.event_sensor_type === "s3_event" && (
                  <div>
                    <Label className="text-xs text-slate-600 dark:text-slate-400">Bucket / Container Prefix</Label>
                    <Input
                      value={formData.event_config?.watch_path || ""}
                      onChange={e => update({ event_config: { ...formData.event_config, watch_path: e.target.value } })}
                      placeholder="s3://my-bucket/prefix/"
                      className="font-mono text-sm h-8 mt-1"
                    />
                  </div>
                )}
                {formData.event_sensor_type === "db_sensor" && (
                  <div>
                    <Label className="text-xs text-slate-600 dark:text-slate-400">SQL Condition</Label>
                    <Input
                      value={formData.event_config?.sql_condition || ""}
                      onChange={e => update({ event_config: { ...formData.event_config, sql_condition: e.target.value } })}
                      placeholder="SELECT COUNT(*) > 0 FROM control_table WHERE status='ready'"
                      className="font-mono text-sm h-8 mt-1"
                    />
                  </div>
                )}
                {formData.event_sensor_type === "api_webhook" && (
                  <div>
                    <Label className="text-xs text-slate-600 dark:text-slate-400">Webhook Endpoint</Label>
                    <Input
                      value={`/api/webhooks/pipeline/${formData.name || "<pipeline>"}/trigger`}
                      readOnly
                      className="font-mono text-sm h-8 mt-1 bg-slate-50 dark:bg-slate-800 text-slate-500"
                    />
                    <p className="text-[10px] text-slate-400 mt-1">POST to this endpoint with API key to trigger.</p>
                  </div>
                )}
                {formData.event_sensor_type === "upstream_job" && (
                  <div>
                    <Label className="text-xs text-slate-600 dark:text-slate-400">Upstream DAG ID</Label>
                    <Input
                      value={formData.event_config?.upstream_job || ""}
                      onChange={e => update({ event_config: { ...formData.event_config, upstream_job: e.target.value } })}
                      placeholder="upstream_pipeline_dag"
                      className="font-mono text-sm h-8 mt-1"
                    />
                  </div>
                )}

                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <Label className="text-xs text-slate-600 dark:text-slate-400">Poke Interval</Label>
                    <Select
                      value={formData.event_config?.poll_interval || "60"}
                      onValueChange={v => update({ event_config: { ...formData.event_config, poll_interval: v } })}
                    >
                      <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="30">30 sec</SelectItem>
                        <SelectItem value="60">1 min</SelectItem>
                        <SelectItem value="120">2 min</SelectItem>
                        <SelectItem value="300">5 min</SelectItem>
                        <SelectItem value="600">10 min</SelectItem>
                        <SelectItem value="1800">30 min</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label className="text-xs text-slate-600 dark:text-slate-400">Timeout</Label>
                    <Select
                      value={formData.event_config?.timeout_hours || "24"}
                      onValueChange={v => update({ event_config: { ...formData.event_config, timeout_hours: v } })}
                    >
                      <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="1">1 hour</SelectItem>
                        <SelectItem value="4">4 hours</SelectItem>
                        <SelectItem value="8">8 hours</SelectItem>
                        <SelectItem value="12">12 hours</SelectItem>
                        <SelectItem value="24">24 hours</SelectItem>
                        <SelectItem value="48">48 hours</SelectItem>
                        <SelectItem value="72">72 hours</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label className="text-xs text-slate-600 dark:text-slate-400">Mode</Label>
                    <Select
                      value={formData.event_config?.sensor_mode || "reschedule"}
                      onValueChange={v => update({ event_config: { ...formData.event_config, sensor_mode: v } })}
                    >
                      <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="poke">Poke (holds worker)</SelectItem>
                        <SelectItem value="reschedule">Reschedule (frees worker)</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-xs font-medium text-slate-700 dark:text-slate-300">Soft Fail</p>
                    <p className="text-[10px] text-slate-400">Mark as skipped instead of failed on timeout</p>
                  </div>
                  <Switch
                    checked={!!formData.event_config?.soft_fail}
                    onCheckedChange={v => update({ event_config: { ...formData.event_config, soft_fail: v } })}
                  />
                </div>
              </div>
            )}
          </div>
        )}
      </SectionCard>


      {schedType !== "manual" && (
        <SectionCard icon={CalendarDays} title="Calendar Timetable" collapsible defaultOpen={false}>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs font-medium text-slate-700 dark:text-slate-300">Use Calendar-Based Scheduling</p>
              <p className="text-[10px] text-slate-400">Apply include/exclude calendar rules to control when the DAG runs (bank holidays, business days, etc.)</p>
            </div>
            <Switch
              checked={!!formData.use_custom_calendar}
              onCheckedChange={checked => setFormData(prev => ({ ...prev, use_custom_calendar: checked }))}
            />
          </div>
          {formData.use_custom_calendar && (
            <>
              <div className="grid grid-cols-2 gap-3 pt-1">
                <div>
                  <Label className="text-xs text-slate-600 dark:text-slate-400 flex items-center gap-1.5">
                    <CalendarDays className="w-3 h-3 text-emerald-500" />
                    Include Calendar
                  </Label>
                  <Select
                    value={formData.include_calendar_id || "none"}
                    onValueChange={v => setFormData(prev => ({ ...prev, include_calendar_id: v === "none" ? null : v }))}
                  >
                    <SelectTrigger className="h-8 text-xs mt-1"><SelectValue placeholder="None" /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="none">None (run all matching intervals)</SelectItem>
                      <SelectItem value="business_days">Business Days Only (Mon–Fri)</SelectItem>
                      <SelectItem value="us_trading_days">US Trading Days (NYSE)</SelectItem>
                      <SelectItem value="month_end_business">Month-End Business Days</SelectItem>
                      <SelectItem value="quarter_end_business">Quarter-End Business Days</SelectItem>
                      <SelectItem value="custom_include">Custom Include Calendar…</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-[10px] text-slate-400 mt-1">Only run on dates matching this calendar</p>
                </div>
                <div>
                  <Label className="text-xs text-slate-600 dark:text-slate-400 flex items-center gap-1.5">
                    <CalendarOff className="w-3 h-3 text-red-400" />
                    Exclude Calendar
                  </Label>
                  <Select
                    value={formData.exclude_calendar_id || "none"}
                    onValueChange={v => setFormData(prev => ({ ...prev, exclude_calendar_id: v === "none" ? null : v }))}
                  >
                    <SelectTrigger className="h-8 text-xs mt-1"><SelectValue placeholder="None" /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="none">None (no exclusions)</SelectItem>
                      <SelectItem value="us_bank_holidays">US Bank Holidays (Federal)</SelectItem>
                      <SelectItem value="us_market_holidays">US Market Holidays (NYSE/NASDAQ)</SelectItem>
                      <SelectItem value="weekends">Weekends (Sat & Sun)</SelectItem>
                      <SelectItem value="year_end_freeze">Year-End Freeze (Dec 20 – Jan 2)</SelectItem>
                      <SelectItem value="custom_exclude">Custom Exclude Calendar…</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-[10px] text-slate-400 mt-1">Skip DAG runs on dates matching this calendar</p>
                </div>
              </div>

              {(formData.include_calendar_id === "custom_include" || formData.exclude_calendar_id === "custom_exclude") && (
                <div className="space-y-2 pt-2">
                  {formData.include_calendar_id === "custom_include" && (
                    <div>
                      <Label className="text-xs text-slate-600 dark:text-slate-400">Custom Include Dates (comma-separated YYYY-MM-DD or cron)</Label>
                      <Input
                        value={formData.custom_include_dates || ""}
                        onChange={e => update({ custom_include_dates: e.target.value })}
                        placeholder="2025-01-15, 2025-04-15 or 0 6 15 * *"
                        className="h-8 text-xs font-mono mt-1"
                      />
                    </div>
                  )}
                  {formData.exclude_calendar_id === "custom_exclude" && (
                    <div>
                      <Label className="text-xs text-slate-600 dark:text-slate-400">Custom Exclude Dates (comma-separated YYYY-MM-DD)</Label>
                      <Input
                        value={formData.custom_exclude_dates || ""}
                        onChange={e => update({ custom_exclude_dates: e.target.value })}
                        placeholder="2025-12-25, 2025-01-01, 2025-07-04"
                        className="h-8 text-xs font-mono mt-1"
                      />
                    </div>
                  )}
                </div>
              )}

              <div className="bg-slate-50 dark:bg-slate-800/50 rounded-lg px-3 py-2 border border-slate-100 dark:border-slate-700">
                <p className="text-[10px] text-slate-500 dark:text-slate-400">
                  <span className="font-semibold text-slate-600 dark:text-slate-300">Timetable logic:</span>{" "}
                  {formData.include_calendar_id && formData.include_calendar_id !== "none"
                    ? `Only run on ${formData.include_calendar_id.replace(/_/g, " ")} dates`
                    : "Run on all cron-matched dates"}
                  {formData.exclude_calendar_id && formData.exclude_calendar_id !== "none"
                    ? `, excluding ${formData.exclude_calendar_id.replace(/_/g, " ")} dates`
                    : ""}
                  .
                </p>
              </div>
            </>
          )}
        </SectionCard>
      )}

      <SectionCard icon={FolderOpen} title="DAG Callable Base Path" collapsible defaultOpen={false}>
        <div className="space-y-2">
          <Label className="text-xs text-slate-600 dark:text-slate-400">Base path for DAG callable files</Label>
          <Input
            value={formData.dag_callable_base_path || ""}
            onChange={e => update({ dag_callable_base_path: e.target.value })}
            placeholder={dataflowConfig.airflow?.dag_callable_base_path || "/data/dags/"}
            className="font-mono text-sm h-9"
          />
          <p className="text-[10px] text-slate-400 dark:text-slate-500">
            The directory where Airflow expects Python callable files (e.g., task scripts and Spark apps). Leave blank to use the default: <code className="bg-slate-100 dark:bg-slate-800 px-1 rounded font-mono">{dataflowConfig.airflow?.dag_callable_base_path || "/data/dags/"}</code>
          </p>
        </div>
      </SectionCard>

      <SectionCard icon={Layers} title="Advanced Pipeline Features" collapsible defaultOpen={false}>
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs font-medium text-slate-700 dark:text-slate-300">Enable Advanced Tab</p>
            <p className="text-[10px] text-slate-400">Unlock data transformations, quality checks, masking, and SLA for each dataset</p>
          </div>
          <Switch
            checked={!!formData.enable_advanced}
            onCheckedChange={(checked) => {
              const patch = { enable_advanced: checked };
              if (checked) {
                patch.advanced_features = {
                  column_mapping: true,
                  data_cleansing: !!formData.advanced_features?.data_cleansing,
                  data_quality: !!formData.advanced_features?.data_quality,
                  security: !!formData.advanced_features?.security,
                  sla: !!formData.advanced_features?.sla,
                };
              }
              setFormData(prev => ({ ...prev, ...patch }));
            }}
          />
        </div>
        {formData.enable_advanced && (
          <div className="space-y-2 pt-1">
            <div className="flex items-center justify-between pb-1.5">
              <p className="text-[10px] text-slate-500 dark:text-slate-400 uppercase tracking-wider font-semibold">Select features to configure</p>
              <button
                type="button"
                onClick={() => setFormData(prev => ({
                  ...prev,
                  advanced_features: { column_mapping: true, data_cleansing: true, data_quality: true, security: true, sla: true }
                }))}
                className="text-[10px] text-[#0060AF] hover:underline font-medium"
              >
                Select All
              </button>
            </div>
            {(() => {
              const ICON_MAP = { column_mapping: Columns, data_cleansing: Zap, data_quality: FileText, security: Shield, sla: ShieldCheck };
              const advCfg = dataflowConfig.pipeline_wizard?.advanced_features || {};
              return Object.entries(advCfg)
                .filter(([, v]) => v.enabled !== false)
                .map(([key, v]) => ({
                  key,
                  label: v.label || key,
                  icon: ICON_MAP[key] || FileText,
                  desc: v.description || "",
                  locked: v.always_on === true,
                }));
            })().map(feat => {
              const Icon = feat.icon;
              const isOn = feat.locked || !!formData.advanced_features?.[feat.key];
              return (
                <button
                  key={feat.key}
                  type="button"
                  disabled={feat.locked}
                  onClick={() => {
                    if (feat.locked) return;
                    setFormData(prev => ({
                      ...prev,
                      advanced_features: { ...prev.advanced_features, [feat.key]: !prev.advanced_features?.[feat.key] }
                    }));
                  }}
                  className={cn(
                    "w-full flex items-center gap-3 px-3 py-2.5 rounded-lg border text-left transition-all",
                    isOn
                      ? "bg-[#0060AF]/5 border-[#0060AF]/30 dark:bg-[#0060AF]/10 dark:border-[#0060AF]/40"
                      : "bg-white dark:bg-slate-800 border-slate-200 dark:border-slate-700 hover:border-slate-300 dark:hover:border-slate-600"
                  )}
                >
                  <div className={cn(
                    "w-5 h-5 rounded flex items-center justify-center shrink-0 border transition-all",
                    isOn
                      ? "bg-[#0060AF] border-[#0060AF] text-white"
                      : "bg-white dark:bg-slate-800 border-slate-300 dark:border-slate-600"
                  )}>
                    {isOn && <CheckSquare className="w-3.5 h-3.5" />}
                  </div>
                  <Icon className={cn("w-4 h-4 shrink-0", isOn ? "text-[#0060AF]" : "text-slate-400")} />
                  <div className="flex-1 min-w-0">
                    <p className={cn("text-xs font-medium", isOn ? "text-slate-800 dark:text-slate-200" : "text-slate-500 dark:text-slate-400")}>
                      {feat.label}
                      {feat.locked && <span className="text-[10px] text-[#0060AF] ml-1.5 font-normal">(always on)</span>}
                    </p>
                    <p className="text-[10px] text-slate-400 truncate">{feat.desc}</p>
                  </div>
                </button>
              );
            })}
          </div>
        )}
      </SectionCard>

    </div>
  );
}
