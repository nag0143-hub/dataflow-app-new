import { ChevronRight, Database, Zap, CheckCircle2, Lock, Shield } from "lucide-react";

const nodeTypeConfig = {
  source: {
    bg: "bg-blue-50",
    border: "border-blue-200",
    icon: Database,
    textColor: "text-blue-900",
    badge: "bg-blue-100 text-blue-700"
  },
  transformation: {
    bg: "bg-purple-50",
    border: "border-purple-200",
    icon: Zap,
    textColor: "text-purple-900",
    badge: "bg-purple-100 text-purple-700"
  },
  target: {
    bg: "bg-green-50",
    border: "border-green-200",
    icon: CheckCircle2,
    textColor: "text-green-900",
    badge: "bg-green-100 text-green-700"
  },
  security: {
    bg: "bg-amber-50",
    border: "border-amber-200",
    icon: Shield,
    textColor: "text-amber-900",
    badge: "bg-amber-100 text-amber-700"
  }
};

export default function FlowNode({ 
  type = "transformation", 
  title, 
  description, 
  metadata = [],
  isActive = true 
}) {
  const config = nodeTypeConfig[type] || nodeTypeConfig.transformation;
  const Icon = config.icon;

  return (
    <div className={`rounded-lg border-2 p-3 flex-shrink-0 w-48 ${config.bg} ${config.border} ${isActive ? "opacity-100" : "opacity-50"}`}>
      <div className="flex items-start gap-2">
        <Icon className={`w-4 h-4 ${config.textColor} flex-shrink-0 mt-1`} />
        <div className="flex-1 min-w-0">
          <h3 className={`text-sm font-semibold ${config.textColor} truncate`}>{title}</h3>
          {description && (
            <p className={`text-xs ${config.textColor} opacity-70 mt-1 line-clamp-2`}>{description}</p>
          )}
        </div>
      </div>
      
      {metadata.length > 0 && (
        <div className="mt-2 space-y-1">
          {metadata.slice(0, 3).map((item, i) => (
            <div key={i} className={`text-xs px-2 py-1 rounded ${config.badge} font-mono`}>
              {item.label}: <span className="font-semibold">{item.value}</span>
            </div>
          ))}
          {metadata.length > 3 && (
            <div className={`text-xs px-2 py-1 ${config.textColor} opacity-60`}>
              +{metadata.length - 3} more
            </div>
          )}
        </div>
      )}
    </div>
  );
}