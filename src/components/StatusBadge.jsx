import { cn } from "@/lib/utils";
import { CheckCircle2, XCircle, Clock, Loader2, AlertTriangle, Pause, FileEdit } from "lucide-react";

const statusConfig = {
  // Job statuses
  draft: { color: "bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-200", icon: FileEdit, label: "Draft" },
  idle: { color: "bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-200", icon: Clock, label: "Idle" },
  running: { color: "bg-blue-200 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300", icon: Loader2, label: "Running", animate: true },
  completed: { color: "bg-green-200 text-green-800 dark:bg-green-900/40 dark:text-green-300", icon: CheckCircle2, label: "Completed" },
  failed: { color: "bg-red-200 text-red-800 dark:bg-red-900/40 dark:text-red-300", icon: XCircle, label: "Failed" },
  paused: { color: "bg-amber-200 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300", icon: Pause, label: "Paused" },
  retrying: { color: "bg-orange-200 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300", icon: Loader2, label: "Retrying", animate: true },
  cancelled: { color: "bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-200", icon: XCircle, label: "Cancelled" },
  
  // Connection statuses
  active: { color: "bg-green-200 text-green-800 dark:bg-green-900/40 dark:text-green-300", icon: CheckCircle2, label: "Active" },
  inactive: { color: "bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-200", icon: Clock, label: "Inactive" },
  error: { color: "bg-red-200 text-red-800 dark:bg-red-900/40 dark:text-red-300", icon: AlertTriangle, label: "Error" },
  pending_setup: { color: "bg-amber-200 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300", icon: AlertTriangle, label: "Pending Setup" },
  
  // Log types
  info: { color: "bg-blue-200 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300", icon: Clock, label: "Info" },
  warning: { color: "bg-amber-200 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300", icon: AlertTriangle, label: "Warning" },
  success: { color: "bg-green-200 text-green-800 dark:bg-green-900/40 dark:text-green-300", icon: CheckCircle2, label: "Success" },
};

export default function StatusBadge({ status, size = "default" }) {
  const config = statusConfig[status] || statusConfig.idle;
  const Icon = config.icon;
  
  return (
    <span className={cn(
      "inline-flex items-center gap-1.5 rounded-full font-medium",
      config.color,
      size === "sm" ? "px-2 py-0.5 text-xs" : "px-3 py-1 text-sm"
    )}>
      <Icon className={cn(
        size === "sm" ? "w-3 h-3" : "w-4 h-4",
        config.animate && "animate-spin"
      )} />
      {config.label}
    </span>
  );
}