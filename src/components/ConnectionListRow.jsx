import { MoreVertical, Edit, Trash2, TestTube, Shield, Loader2, Wifi } from "lucide-react";
import { Button } from "@/components/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import StatusBadge from "@/components/StatusBadge";
import PlatformIcon, { platformConfig } from "@/components/PlatformIcon";
import moment from "moment";

export default function ConnectionListRow({
  connection,
  getPrereqSummary,
  testingId,
  setPrereqDialogConn,
  handleTestConnection,
  handleEdit,
  handleDelete,
}) {
  const prereq = getPrereqSummary(connection.id);

  return (
    <div className="flex items-center gap-4 px-4 py-3 border-b border-slate-100 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
      {/* Platform Icon */}
      <div className="shrink-0">
        <PlatformIcon platform={connection.platform} />
      </div>

      {/* Name + type */}
      <div className="w-52 min-w-0">
        <p className="font-medium text-slate-900 dark:text-white truncate">{connection.name}</p>
        <p className="text-xs text-slate-500 capitalize">{platformConfig[connection.platform]?.label || connection.platform}</p>
      </div>

      {/* Platform label */}
      <div className="w-40 min-w-0 hidden sm:block">
        <p className="text-sm text-slate-700 dark:text-slate-300 truncate">{platformConfig[connection.platform]?.label || connection.platform}</p>
      </div>

      {/* Host / System */}
      <div className="flex-1 min-w-0 hidden md:block">
        <p className="text-sm text-slate-500 truncate">
          {connection.host || connection.source_system_name || connection.bucket_container || "—"}
        </p>
      </div>

      {/* Tags */}
      <div className="hidden lg:flex items-center gap-1 w-40 flex-wrap">
        {(connection.tags || []).slice(0, 3).map(tag => (
          <span key={tag} className="inline-flex items-center px-1.5 py-0.5 rounded-full text-xs bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border border-blue-100 dark:border-blue-800">
            {tag}
          </span>
        ))}
        {(connection.tags?.length || 0) > 3 && (
          <span className="text-xs text-slate-400">+{connection.tags.length - 3}</span>
        )}
      </div>

      {/* Status */}
      <div className="shrink-0">
        <StatusBadge status={connection.status} size="sm" />
      </div>

      {/* Last tested */}
      <div className="w-24 shrink-0 hidden xl:block">
        <p className="text-xs text-slate-400 text-right">{connection.last_tested ? moment(connection.last_tested).fromNow() : "—"}</p>
      </div>

      {/* Actions */}
      <div className="flex items-center gap-1 shrink-0">
        <Button variant="ghost" size="sm" className="h-7 gap-1 text-xs text-slate-500 hover:text-slate-800" onClick={() => setPrereqDialogConn(connection)}>
          <Shield className="w-3.5 h-3.5" />
          <span className="hidden sm:inline">{prereq.total > 0 ? `${prereq.done}/${prereq.total}` : "Setup"}</span>
        </Button>
        <Button size="sm" variant="outline" className="h-7 text-xs gap-1" onClick={() => handleTestConnection(connection)} disabled={testingId === connection.id}>
          {testingId === connection.id ? <Loader2 className="w-3 h-3 animate-spin" /> : <Wifi className="w-3 h-3" />}
          <span className="hidden sm:inline">{testingId === connection.id ? "Testing…" : "Test"}</span>
        </Button>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon" className="h-7 w-7"><MoreVertical className="w-4 h-4" /></Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={() => handleEdit(connection)}><Edit className="w-4 h-4 mr-2" />Edit</DropdownMenuItem>
            <DropdownMenuItem onClick={() => handleDelete(connection)} className="text-red-600 focus:text-red-600"><Trash2 className="w-4 h-4 mr-2" />Delete</DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </div>
  );
}