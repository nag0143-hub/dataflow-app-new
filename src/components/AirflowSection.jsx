import { useState, useEffect, useCallback } from "react";
import { Link } from "react-router-dom";
import { RefreshCw, AlertCircle, ExternalLink, CheckCircle2, XCircle, Clock, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { toast } from "sonner";
import { createPageUrl } from "@/utils";

const API = '/api/airflow';

const STATE_ICON = {
  success: CheckCircle2, failed: XCircle, running: Loader2, queued: Clock,
};
const STATE_COLOR = {
  success: "text-emerald-600", failed: "text-red-600", running: "text-blue-600 animate-spin", queued: "text-amber-500",
};

export default function AirflowSection() {
  const [connections, setConnections] = useState([]);
  const [selectedId, setSelectedId] = useState("");
  const [dags, setDags] = useState([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      const resp = await fetch(`${API}/connections`);
      const conns = await resp.json();
      setConnections(conns);
      if (conns.length > 0 && !selectedId) setSelectedId(conns[0].id);
    } catch { /* silent */ }
    finally { setLoading(false); }
  }, [selectedId]);

  const loadDags = useCallback(async () => {
    if (!selectedId) return;
    try {
      const resp = await fetch(`${API}/${selectedId}/dags?limit=6`);
      const data = await resp.json();
      setDags(data.dags || []);
    } catch { /* silent */ }
  }, [selectedId]);

  useEffect(() => { load(); }, []);
  useEffect(() => { if (selectedId) loadDags(); }, [selectedId, loadDags]);

  if (loading) return <div className="flex items-center justify-center h-20"><Loader2 className="w-5 h-5 text-blue-600 animate-spin" /></div>;

  if (connections.length === 0) {
    return (
      <div className="text-center py-6">
        <AlertCircle className="w-8 h-8 text-slate-300 mx-auto mb-2" />
        <p className="text-sm text-slate-500 dark:text-slate-400 mb-2">No Airflow instances connected</p>
        <Link to={createPageUrl("Airflow")} className="text-sm text-blue-600 hover:text-blue-800 font-medium">
          Go to Airflow page to connect
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <Select value={selectedId} onValueChange={setSelectedId}>
          <SelectTrigger className="h-8 text-xs flex-1"><SelectValue /></SelectTrigger>
          <SelectContent>
            {connections.map(c => <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>)}
          </SelectContent>
        </Select>
        <Button size="sm" variant="ghost" onClick={loadDags} className="h-8 w-8 p-0">
          <RefreshCw className="w-3.5 h-3.5" />
        </Button>
      </div>

      {dags.length === 0 && (
        <p className="text-xs text-slate-400 text-center py-4">No DAGs found</p>
      )}

      <div className="space-y-1.5">
        {dags.slice(0, 5).map(dag => {
          const Icon = STATE_ICON[dag.is_paused ? 'queued' : 'success'] || CheckCircle2;
          const color = dag.is_paused ? 'text-amber-500' : 'text-emerald-500';
          return (
            <div key={dag.dag_id} className="flex items-center gap-2 py-1.5 px-2 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700/50 text-xs">
              <Icon className={`w-3.5 h-3.5 shrink-0 ${color}`} />
              <span className="font-mono text-slate-700 dark:text-slate-200 truncate flex-1">{dag.dag_id}</span>
              <span className="text-slate-400 shrink-0">{dag.schedule || dag.schedule_interval || 'manual'}</span>
            </div>
          );
        })}
      </div>

      {dags.length > 0 && (
        <Link to={createPageUrl("Airflow")} className="block text-center text-xs text-blue-600 hover:text-blue-800 font-medium pt-1">
          View all DAGs →
        </Link>
      )}
    </div>
  );
}