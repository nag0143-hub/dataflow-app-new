import { cn } from "@/lib/utils";

const variantStyles = {
  default: {
    wrapper: "",
    iconBg: "bg-slate-100 dark:bg-slate-700",
    iconColor: "text-slate-600 dark:text-slate-300",
  },
  blue: {
    wrapper: "border-l-[3px] !border-l-[#0060AF]",
    iconBg: "bg-blue-50 dark:bg-blue-900/30",
    iconColor: "text-[#0060AF] dark:text-blue-400",
  },
  green: {
    wrapper: "border-l-[3px] !border-l-emerald-500",
    iconBg: "bg-emerald-50 dark:bg-emerald-900/30",
    iconColor: "text-emerald-600 dark:text-emerald-400",
  },
  red: {
    wrapper: "border-l-[3px] !border-l-red-500",
    iconBg: "bg-red-50 dark:bg-red-900/30",
    iconColor: "text-red-600 dark:text-red-400",
  },
  amber: {
    wrapper: "border-l-[3px] !border-l-amber-500",
    iconBg: "bg-amber-50 dark:bg-amber-900/30",
    iconColor: "text-amber-600 dark:text-amber-400",
  },
};

export default function StatCard({ title, value, subtitle, icon: Icon, trend, variant = "default", className }) {
  const styles = variantStyles[variant] || variantStyles.default;

  return (
    <div className={cn(
      "bg-card rounded-xl border p-5 hover:shadow-md transition-shadow duration-300",
      styles.wrapper,
      className
    )}>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm font-medium text-muted-foreground">{title}</p>
          <p className="text-2xl font-bold text-card-foreground mt-1.5 tracking-tight">{value}</p>
          {subtitle && (
            <p className="text-sm text-muted-foreground mt-0.5">{subtitle}</p>
          )}
          {trend && (
            <div className={cn(
              "inline-flex items-center gap-1 mt-1.5 text-sm font-medium",
              trend.positive ? "text-emerald-600" : "text-red-600"
            )}>
              <span>{trend.positive ? "\u2191" : "\u2193"} {trend.value}</span>
              <span className="text-muted-foreground">vs last week</span>
            </div>
          )}
        </div>
        {Icon && (
          <div className={cn("w-10 h-10 rounded-lg flex items-center justify-center", styles.iconBg)}>
            <Icon className={cn("w-5 h-5", styles.iconColor)} />
          </div>
        )}
      </div>
    </div>
  );
}
