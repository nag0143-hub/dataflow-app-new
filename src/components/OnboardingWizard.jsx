import { useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Link } from "react-router-dom";
import { createPageUrl } from "@/utils";
import { CheckCircle2, Cable, Play, ArrowRight } from "lucide-react";
import { cn } from "@/lib/utils";

export default function OnboardingWizard({ open, onClose, connections, jobs }) {
  const [currentStep, setCurrentStep] = useState(0);

  const hasConnections = connections.length > 0;
  const hasJobs = jobs.length > 0;

  const steps = [
    {
      title: "Welcome to DataFlow",
      description: "Let's get you started with your first data transfer in 3 simple steps",
      completed: false,
      action: null
    },
    {
      title: "Step 1: Create Connections",
      description: "Set up source and target connections for your data transfers",
      completed: hasConnections,
      action: { label: "Create Connection", href: createPageUrl("Connections") }
    },
    {
      title: "Step 2: Create Your First Pipeline",
      description: "Configure a data transfer pipeline between your connections",
      completed: hasJobs,
      disabled: !hasConnections,
      action: { label: "Create Pipeline", onClick: () => { onClose(); } }
    },
    {
      title: "All Set!",
      description: "You're ready to start transferring data. Check the dashboard for monitoring.",
      completed: hasJobs,
      action: { label: "Go to Dashboard", href: createPageUrl("Dashboard") }
    }
  ];

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Getting Started Guide</DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          {steps.map((step, idx) => (
            <div
              key={idx}
              className={cn(
                "flex items-start gap-4 p-4 rounded-lg border transition-all",
                step.completed
                  ? "bg-emerald-50 border-emerald-200"
                  : currentStep === idx
                  ? "bg-blue-50 border-blue-300"
                  : "bg-slate-50 border-slate-200",
                step.disabled && "opacity-50"
              )}
            >
              <div className={cn(
                "w-8 h-8 rounded-full flex items-center justify-center shrink-0 mt-0.5",
                step.completed
                  ? "bg-emerald-600 text-white"
                  : currentStep === idx
                  ? "bg-blue-600 text-white"
                  : "bg-slate-300 text-slate-600"
              )}>
                {step.completed ? <CheckCircle2 className="w-5 h-5" /> : idx + 1}
              </div>
              
              <div className="flex-1">
                <h3 className="font-semibold text-slate-900 mb-1">{step.title}</h3>
                <p className="text-sm text-slate-600 mb-3">{step.description}</p>
                
                {step.action && !step.disabled && (
                  step.action.href ? (
                    <Link to={step.action.href}>
                      <Button
                        variant={step.completed ? "outline" : "default"}
                        size="sm"
                        onClick={onClose}
                      >
                        {step.action.label}
                        <ArrowRight className="w-4 h-4 ml-2" />
                      </Button>
                    </Link>
                  ) : (
                    <Button
                      variant={step.completed ? "outline" : "default"}
                      size="sm"
                      onClick={() => { step.action.onClick(); onClose(); }}
                    >
                      {step.action.label}
                      <ArrowRight className="w-4 h-4 ml-2" />
                    </Button>
                  )
                )}
              </div>
            </div>
          ))}
        </div>

        <div className="flex justify-end gap-2 mt-4">
          <Button variant="outline" onClick={onClose}>
            {hasJobs ? "Close" : "Skip for Now"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}