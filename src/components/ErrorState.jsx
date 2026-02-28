import { AlertTriangle, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function ErrorState({ title = "Failed to load data", message, onRetry }) {
  return (
    <div className="flex flex-col items-center justify-center min-h-[40vh] gap-4 text-center p-8">
      <div className="w-16 h-16 rounded-full bg-red-50 dark:bg-red-900/20 flex items-center justify-center">
        <AlertTriangle className="w-8 h-8 text-red-500" />
      </div>
      <div>
        <h2 className="text-xl font-semibold text-slate-900 dark:text-white mb-1">{title}</h2>
        {message && (
          <p className="text-sm text-slate-500 dark:text-slate-400 max-w-sm">{message}</p>
        )}
      </div>
      {onRetry && (
        <Button variant="outline" onClick={onRetry} className="gap-2">
          <RefreshCw className="w-4 h-4" />
          Try Again
        </Button>
      )}
    </div>
  );
}