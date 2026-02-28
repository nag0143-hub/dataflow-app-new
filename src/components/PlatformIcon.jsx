import { Database, Server, Cloud, HardDrive, FileText, FolderOpen, Network } from "lucide-react";
import { cn } from "@/lib/utils";

const platformConfig = {
  sql_server:           { icon: Server,      color: "bg-red-100 text-red-600",     borderColor: "border-l-red-500",     label: "SQL Server" },
  oracle:               { icon: Database,    color: "bg-orange-100 text-orange-600", borderColor: "border-l-orange-500", label: "Oracle" },
  postgresql:           { icon: Database,    color: "bg-blue-100 text-blue-600",   borderColor: "border-l-blue-500",    label: "PostgreSQL" },
  mysql:                { icon: Database,    color: "bg-cyan-100 text-cyan-600",    borderColor: "border-l-cyan-500",   label: "MySQL" },
  mongodb:              { icon: Database,    color: "bg-green-100 text-green-600",  borderColor: "border-l-green-500",  label: "MongoDB" },
  adls2:                { icon: Cloud,       color: "bg-sky-100 text-sky-600",      borderColor: "border-l-sky-500",    label: "Azure ADLS Gen2" },
  s3:                   { icon: HardDrive,   color: "bg-amber-100 text-amber-600",  borderColor: "border-l-amber-500",  label: "AWS S3" },
  flat_file_delimited:  { icon: FileText,    color: "bg-violet-100 text-violet-600", borderColor: "border-l-violet-500", label: "Flat File (Delimited)" },
  flat_file_fixed_width:{ icon: FileText,    color: "bg-purple-100 text-purple-600", borderColor: "border-l-purple-500", label: "Flat File (Fixed Width)" },
  cobol_ebcdic:         { icon: FileText,    color: "bg-rose-100 text-rose-600",    borderColor: "border-l-rose-500",   label: "COBOL / EBCDIC" },
  sftp:                 { icon: Network,     color: "bg-teal-100 text-teal-600",    borderColor: "border-l-teal-500",   label: "SFTP" },
  nas:                  { icon: FolderOpen,  color: "bg-lime-100 text-lime-700",    borderColor: "border-l-lime-500",   label: "NAS / Network Share" },
  local_fs:             { icon: HardDrive,   color: "bg-slate-100 text-slate-600",  borderColor: "border-l-slate-500",  label: "Local Filesystem" },
  airflow:              { icon: Network,     color: "bg-cyan-100 text-cyan-600",    borderColor: "border-l-cyan-500",   label: "Apache Airflow" },
};

export default function PlatformIcon({ platform, showLabel = false, size = "default" }) {
  const config = platformConfig[platform] || { icon: Database, color: "bg-slate-100 text-slate-600", label: platform };
  const Icon = config.icon;

  return (
    <div className="flex items-center gap-2">
      <div className={cn(
        "rounded-lg flex items-center justify-center",
        config.color,
        size === "sm" ? "w-7 h-7" : "w-9 h-9"
      )}>
        <Icon className={size === "sm" ? "w-4 h-4" : "w-5 h-5"} />
      </div>
      {showLabel && (
        <span className="text-sm font-medium text-slate-700">{config.label}</span>
      )}
    </div>
  );
}

export { platformConfig };