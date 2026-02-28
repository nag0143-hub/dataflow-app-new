import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { BookOpen, ArrowRight, Cable, Play, BarChart3 } from "lucide-react";

const defaultSteps = [
  { number: 1, label: "Create a connection", desc: "Set up your source and target systems", icon: Cable },
  { number: 2, label: "Build a pipeline", desc: "Select datasets and configure transfer rules", icon: Play },
  { number: 3, label: "Monitor runs", desc: "Track status, rows processed, and errors", icon: BarChart3 },
];

export default function EmptyStateGuide({ icon: Icon, title, description, primaryAction, secondaryLinks, steps }) {
  const displaySteps = steps || defaultSteps;

  return (
    <Card>
      <CardContent className="py-12 text-center">
        {Icon && (
          <div className="w-16 h-16 rounded-2xl bg-blue-50 dark:bg-blue-900/20 flex items-center justify-center mx-auto mb-5">
            <Icon className="w-8 h-8 text-[#0060AF] dark:text-blue-400" />
          </div>
        )}
        <h3 className="text-lg font-semibold text-foreground mb-1">{title}</h3>
        <p className="text-muted-foreground mb-8 max-w-md mx-auto">{description}</p>

        <div className="flex items-start justify-center gap-4 mb-8 max-w-lg mx-auto">
          {displaySteps.map((step, i) => {
            const StepIcon = step.icon;
            return (
              <div key={step.number} className="flex items-start gap-2 flex-1">
                {i > 0 && (
                  <div className="mt-4 w-8 border-t-2 border-dashed border-border flex-shrink-0" />
                )}
                <div className="flex flex-col items-center text-center flex-1">
                  <div className="w-9 h-9 rounded-full bg-blue-100 dark:bg-blue-900/30 text-[#0060AF] dark:text-blue-400 flex items-center justify-center text-sm font-bold mb-2">
                    {step.number}
                  </div>
                  <p className="text-sm font-medium text-foreground leading-tight">{step.label}</p>
                  <p className="text-xs text-muted-foreground mt-0.5">{step.desc}</p>
                </div>
              </div>
            );
          })}
        </div>

        <div className="flex flex-col sm:flex-row gap-3 justify-center items-center">
          {primaryAction && (
            <Button onClick={primaryAction.onClick} className="gap-2 bg-[#0060AF] hover:bg-[#004d8c] text-white">
              {primaryAction.icon}
              {primaryAction.label}
            </Button>
          )}
          {secondaryLinks && secondaryLinks.length > 0 && (
            <div className="flex gap-2">
              {secondaryLinks.map((link) => (
                <a
                  key={link.label}
                  href={link.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm text-[#0060AF] hover:text-[#004d8c] font-medium flex items-center gap-1"
                >
                  <BookOpen className="w-3.5 h-3.5" />
                  {link.label}
                </a>
              ))}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
