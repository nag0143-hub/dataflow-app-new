import { memo } from "react";
import { MoreVertical, Edit, Trash2, TestTube, Shield, Loader2, Wifi, Lock } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import StatusBadge from "@/components/StatusBadge";
import PlatformIcon, { platformConfig } from "@/components/PlatformIcon";
import moment from "moment";

const ConnectionCard = memo(function ConnectionCard({
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
    <Card className={`hover:shadow-md transition-shadow border-l-4 ${platformConfig[connection.platform]?.borderColor || "border-l-slate-300"}`}>
      <CardContent className="p-5">
        <div className="flex items-start justify-between mb-4">
          <div className="flex items-center gap-3">
            <PlatformIcon platform={connection.platform} />
            <div>
              <h3 className="font-semibold text-slate-900 dark:text-white">{connection.name}</h3>
              <p className="text-xs text-slate-500 dark:text-slate-400 capitalize">{platformConfig[connection.platform]?.label || connection.platform}</p>
            </div>
          </div>
          <div className="flex items-center gap-1">
            <Button variant="ghost" size="sm" className="h-8 gap-1 text-xs text-slate-500 hover:text-slate-800" onClick={() => setPrereqDialogConn(connection)}>
              <Shield className="w-3.5 h-3.5" />
              {prereq.total > 0 ? `${prereq.done}/${prereq.total}` : "Setup"}
            </Button>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon" className="h-8 w-8"><MoreVertical className="w-4 h-4" /></Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={() => handleTestConnection(connection)}><TestTube className="w-4 h-4 mr-2" />Test Connection</DropdownMenuItem>
                <DropdownMenuItem onClick={() => handleEdit(connection)}><Edit className="w-4 h-4 mr-2" />Edit</DropdownMenuItem>
                <DropdownMenuItem onClick={() => handleDelete(connection)} className="text-red-600 focus:text-red-600"><Trash2 className="w-4 h-4 mr-2" />Delete</DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>

        <div className="space-y-2 text-sm">
          <div className="flex justify-between"><span className="text-slate-500">Platform</span><span className="text-slate-700 dark:text-slate-300 font-medium">{platformConfig[connection.platform]?.label || connection.platform}</span></div>
          {connection.source_system_name && <div className="flex justify-between"><span className="text-slate-500">System</span><span className="text-slate-700 dark:text-slate-300">{connection.source_system_name}</span></div>}
          {connection.car_id && <div className="flex justify-between"><span className="text-slate-500">CarID</span><span className="text-slate-700 dark:text-slate-300">{connection.car_id}</span></div>}
          {connection.host && <div className="flex justify-between"><span className="text-slate-500">Host</span><span className="text-slate-700 dark:text-slate-300 truncate ml-2 max-w-[150px]">{connection.host}</span></div>}
          {connection.database && <div className="flex justify-between"><span className="text-slate-500">Database</span><span className="text-slate-700 dark:text-slate-300">{connection.database}</span></div>}
          {connection.bucket_container && <div className="flex justify-between"><span className="text-slate-500">Container</span><span className="text-slate-700 dark:text-slate-300">{connection.bucket_container}</span></div>}
          {connection.auth_method === "vault_credentials" && (
            <div className="flex items-center gap-1.5 mt-1">
              <Lock className="w-3 h-3 text-amber-600 dark:text-amber-400" />
              <span className="text-xs text-amber-700 dark:text-amber-300 font-medium">Vault Credentials</span>
            </div>
          )}
        </div>

        {connection.tags?.length > 0 && (
          <div className="mt-3 flex flex-wrap gap-1">
            {connection.tags.map(tag => (
              <span key={tag} className="inline-flex items-center px-2 py-0.5 rounded-full text-xs bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border border-blue-100 dark:border-blue-800">
                {tag}
              </span>
            ))}
          </div>
        )}

        <div className="mt-4 pt-4 border-t border-slate-100 dark:border-slate-700 flex items-center justify-between gap-2">
          <StatusBadge status={connection.status} size="sm" />
          <div className="flex items-center gap-2">
            {connection.last_tested && <span className="text-xs text-slate-400">{moment(connection.last_tested).fromNow()}</span>}
            <Button size="sm" variant="outline" className="h-7 text-xs gap-1.5" onClick={() => handleTestConnection(connection)} disabled={testingId === connection.id}>
              {testingId === connection.id ? <Loader2 className="w-3 h-3 animate-spin" /> : <Wifi className="w-3 h-3" />}
              {testingId === connection.id ? "Testing..." : "Test"}
            </Button>
          </div>
        </div>
      </CardContent>
      </Card>
      );
      });

      export default ConnectionCard;