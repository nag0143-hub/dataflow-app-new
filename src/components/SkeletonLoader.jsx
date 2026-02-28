import { cn } from "@/lib/utils";

export default function SkeletonLoader({ count = 3, height = "h-12", className = "" }) {
  return (
    <div className="space-y-3">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className={cn(height, "bg-slate-200 dark:bg-slate-700 rounded-lg animate-pulse", className)} />
      ))}
    </div>
  );
}