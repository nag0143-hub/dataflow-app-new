import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Clock, Target, AlertTriangle } from "lucide-react";
import { cn } from "@/lib/utils";

export default function SLAConfig({ value = {}, onChange }) {
  const sla = value || {
    enabled: false,
    max_duration_minutes: 60,
    alert_threshold_percent: 80,
    escalation_enabled: false,
    escalation_email: "",
  };

  const updateSLA = (field, val) => {
    onChange({ ...sla, [field]: val });
  };

  return (
    <Card className="dark:bg-slate-800 dark:border-slate-700">
      <CardHeader>
        <div className="flex items-center gap-2">
          <Target className="w-5 h-5 text-blue-600" />
          <CardTitle>SLA Management</CardTitle>
        </div>
        <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
          Define and track service level agreement compliance
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="space-y-0.5">
            <Label className="text-sm font-medium dark:text-white">Enable SLA Tracking</Label>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Monitor job execution time against defined SLA
            </p>
          </div>
          <Switch
            checked={sla.enabled}
            onCheckedChange={(checked) => updateSLA("enabled", checked)}
          />
        </div>

        {sla.enabled && (
          <>
            <div className="h-px bg-slate-200 dark:bg-slate-700" />
            
            <div className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label className="text-sm dark:text-white">
                    <Clock className="w-4 h-4 inline mr-1" />
                    Max Duration (minutes)
                  </Label>
                  <Input
                    type="number"
                    min="1"
                    value={sla.max_duration_minutes}
                    onChange={(e) => updateSLA("max_duration_minutes", parseInt(e.target.value))}
                    className="dark:bg-slate-700 dark:border-slate-600"
                    placeholder="60"
                  />
                  <p className="text-xs text-slate-500 dark:text-slate-400">
                    Job should complete within this time
                  </p>
                </div>

                <div className="space-y-2">
                  <Label className="text-sm dark:text-white">
                    <AlertTriangle className="w-4 h-4 inline mr-1" />
                    Alert Threshold (%)
                  </Label>
                  <Input
                    type="number"
                    min="1"
                    max="100"
                    value={sla.alert_threshold_percent}
                    onChange={(e) => updateSLA("alert_threshold_percent", parseInt(e.target.value))}
                    className="dark:bg-slate-700 dark:border-slate-600"
                    placeholder="80"
                  />
                  <p className="text-xs text-slate-500 dark:text-slate-400">
                    Alert when {sla.alert_threshold_percent}% of SLA time consumed
                  </p>
                </div>
              </div>

              <div className="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-800 rounded-lg">
                <div className="space-y-0.5">
                  <Label className="text-sm font-medium dark:text-white">Enable Escalation</Label>
                  <p className="text-xs text-slate-500 dark:text-slate-400">
                    Send alerts when SLA is at risk or breached
                  </p>
                </div>
                <Switch
                  checked={sla.escalation_enabled}
                  onCheckedChange={(checked) => updateSLA("escalation_enabled", checked)}
                />
              </div>

              {sla.escalation_enabled && (
                <div className="space-y-2">
                  <Label className="text-sm dark:text-white">Escalation Email</Label>
                  <Input
                    type="email"
                    value={sla.escalation_email}
                    onChange={(e) => updateSLA("escalation_email", e.target.value)}
                    className="dark:bg-slate-700 dark:border-slate-600"
                    placeholder="team@company.com"
                  />
                  <p className="text-xs text-slate-500 dark:text-slate-400">
                    Notifications will be sent to this email
                  </p>
                </div>
              )}

              <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
                <div className="flex gap-3">
                  <Target className="w-5 h-5 text-blue-600 shrink-0 mt-0.5" />
                  <div className="space-y-1">
                    <p className="text-sm font-medium text-blue-900 dark:text-blue-300">
                      SLA Summary
                    </p>
                    <p className="text-xs text-blue-700 dark:text-blue-400">
                      Job must complete in <strong>{sla.max_duration_minutes} minutes</strong>.
                      Alert at <strong>{sla.alert_threshold_percent}%</strong> ({Math.round(sla.max_duration_minutes * sla.alert_threshold_percent / 100)} min).
                      {sla.escalation_enabled && ` Escalations sent to ${sla.escalation_email || "configured email"}.`}
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}