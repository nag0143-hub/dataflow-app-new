import { Check, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

export default function PipelineStepIndicator({ steps, activeStep, onStepClick, completedSteps = [] }) {
  const activeIdx = steps.findIndex(s => s.key === activeStep);

  return (
    <div className="flex items-center w-full mb-6">
      {steps.map((step, idx) => {
        const isActive = step.key === activeStep;
        const isCompleted = completedSteps.includes(step.key);
        const isPast = idx < activeIdx;
        const isLast = idx === steps.length - 1;
        const connectorDone = idx < activeIdx;

        return (
          <div key={step.key} className="flex items-center flex-1 min-w-0">
            <button
              type="button"
              onClick={() => onStepClick(step.key)}
              className="flex flex-col items-center gap-1.5 flex-1 min-w-0 group"
            >
              <div className={cn(
                "w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold border-2 transition-all duration-300 shrink-0",
                isCompleted && !isActive
                  ? "bg-emerald-500 border-emerald-500 text-white"
                  : isActive
                  ? "bg-[#0060AF] border-[#0060AF] text-white ring-4 ring-[#0060AF]/10"
                  : isPast
                  ? "bg-[#0060AF]/10 border-[#0060AF] text-[#0060AF]"
                  : "bg-white dark:bg-slate-800 border-slate-300 dark:border-slate-600 text-slate-400 group-hover:border-[#0060AF]/50 group-hover:text-[#0060AF]"
              )}>
                {isCompleted && !isActive ? <Check className="w-3.5 h-3.5" /> : idx + 1}
              </div>
              <span className={cn(
                "text-xs sm:text-sm font-semibold truncate max-w-full text-center transition-colors",
                isActive ? "text-[#0060AF] dark:text-[#4d9de0]" : isCompleted ? "text-emerald-600 dark:text-emerald-400" : isPast ? "text-slate-600 dark:text-slate-400" : "text-slate-400 dark:text-slate-500"
              )}>
                {step.label}
              </span>
            </button>

            {!isLast && (
              <ChevronRight className={cn(
                "w-4 h-4 shrink-0 mx-0.5 transition-colors duration-300",
                connectorDone ? "text-[#0060AF]" : "text-slate-300 dark:text-slate-600"
              )} />
            )}
          </div>
        );
      })}
    </div>
  );
}
