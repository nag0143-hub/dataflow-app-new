import { useState, useEffect, useCallback } from "react";
import { Link } from "react-router-dom";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { createPageUrl } from "@/utils";
import {
  RefreshCw, Plus, Search, Play, Pause, ExternalLink, X,
  CheckCircle2, Clock, XCircle, Loader2, ChevronDown, ChevronUp,
  FileText, Zap, Wind, AlertCircle, ArrowRight
} from "lucide-react";

const API = '/api/airflow';

const STATE_STYLES = {
  success:  { bg: "bg-emerald-100 dark:bg-emerald-900/30", text: "text-emerald-700 dark:text-emerald-400", icon: CheckCircle2 },
  failed:   { bg: "bg-red-100 dark:bg-red-900/30", text: "text-red-700 dark:text-red-400", icon: XCircle },
  running:  { bg: "bg-blue-100 dark:bg-blue-900/30", text: "text-blue-700 dark:text-blue-400", icon: Loader2 },
  queued:   { bg: "bg-amber-100 dark:bg-amber-900/30", text: "text-amber-700 dark:text-amber-400", icon: Clock },
  skipped:  { bg: "bg-slate-100 dark:bg-slate-700", text: "text-slate-500 dark:text-slate-400", icon: null },
  up_for_retry: { bg: "bg-orange-100 dark:bg-orange-900/30", text: "text-orange-700 dark:text-orange-400", icon: RefreshCw },
  upstream_failed: { bg: "bg-red-50 dark:bg-red-900/20", text: "text-red-500 dark:text-red-400", icon: XCircle },
  no_status: { bg: "bg-slate-50 dark:bg-slate-700", text: "text-slate-400", icon: null },
};

function StateBadge({ state }) {
  const s = STATE_STYLES[state] || STATE_STYLES.no_status;
  const Icon = s.icon;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${s.bg} ${s.text}`}>
      {Icon && <Icon className={`w-3 h-3 ${state === 'running' ? 'animate-spin' : ''}`} />}
      {state || 'none'}
    </span>
  );
}

function formatDuration(seconds) {
  if (!seconds || seconds < 0) return '—';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function formatTime(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function AddInstanceForm({ onAdded, onCancel }) {
  const [form, setForm] = useState({ name: '', host: '', auth_method: 'bearer', api_token: '', username: '', password: '', api_version: 'v2' });
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);

  const handleSave = async () => {
    if (!form.name.trim() || !form.host.trim()) { toast.error('Name and Airflow URL are required'); return; }
    setSaving(true);
    try {
      const resp = await fetch(`${API}/connections`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(form),
      });
      if (resp.ok) { toast.success('Airflow instance added'); onAdded(); }
      else { const e = await resp.json(); toast.error(e.error || 'Failed to add'); }
    } catch (err) { toast.error(err.message); }
    finally { setSaving(false); }
  };

  const handleTest = async () => {
    if (!form.host.trim()) { toast.error('Airflow URL is required'); return; }
    if (form.auth_method === 'basic' && (!form.username.trim() || !form.password.trim())) {
      toast.error('Username and password are required for Basic Auth');
      return;
    }
    if (form.auth_method === 'bearer' && !form.api_token.trim()) {
      toast.error('API token is required for Bearer Auth');
      return;
    }
    setTesting(true); setTestResult(null);
    try {
      const testResp = await fetch(`${API}/connections/test`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          host: form.host,
          auth_method: form.auth_method,
          username: form.username,
          password: form.password,
          api_token: form.api_token,
          api_version: form.api_version,
        }),
      });
      const result = await testResp.json();
      setTestResult(result);
      if (result.success) {
        toast.success(`Connected successfully (${result.latency_ms}ms)`);
      }
    } catch (err) { setTestResult({ success: false, error: err.message }); }
    finally { setTesting(false); }
  };

  return (
    <div className="border border-slate-200 dark:border-slate-700 rounded-xl bg-white dark:bg-slate-800 p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="font-semibold text-slate-900 dark:text-white">Add Airflow Instance</h3>
        <button onClick={onCancel} className="text-slate-400 hover:text-slate-600"><X className="w-5 h-5" /></button>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div>
          <Label className="text-sm">Instance Name *</Label>
          <Input placeholder="Production Airflow" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} />
        </div>
        <div>
          <Label className="text-sm">Airflow URL *</Label>
          <Input placeholder="https://airflow.example.com" value={form.host} onChange={e => setForm({ ...form, host: e.target.value })} />
        </div>
      </div>
      <div>
        <Label className="text-sm">Auth Method</Label>
        <Select value={form.auth_method} onValueChange={v => setForm({ ...form, auth_method: v })}>
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="bearer">Bearer Token</SelectItem>
            <SelectItem value="basic">Basic Auth (Username/Password)</SelectItem>
          </SelectContent>
        </Select>
      </div>
      {form.auth_method === 'bearer' ? (
        <div>
          <Label className="text-sm">API Token</Label>
          <Input type="password" placeholder="Your Airflow REST API token" value={form.api_token} onChange={e => setForm({ ...form, api_token: e.target.value })} />
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-4">
          <div>
            <Label className="text-sm">Username</Label>
            <Input placeholder="airflow" value={form.username} onChange={e => setForm({ ...form, username: e.target.value })} />
          </div>
          <div>
            <Label className="text-sm">Password</Label>
            <Input type="password" placeholder="Password" value={form.password} onChange={e => setForm({ ...form, password: e.target.value })} />
          </div>
        </div>
      )}
      <div>
        <Label className="text-sm">API Version</Label>
        <Select value={form.api_version} onValueChange={v => setForm({ ...form, api_version: v })}>
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="v2">v2 (Airflow 3.x)</SelectItem>
            <SelectItem value="v1">v1 (Legacy)</SelectItem>
          </SelectContent>
        </Select>
      </div>
      {testResult && (
        <div className={`flex items-center gap-2 text-xs px-3 py-2 rounded-lg border ${testResult.success ? 'bg-emerald-50 border-emerald-200 text-emerald-700' : 'bg-red-50 border-red-200 text-red-700'}`}>
          {testResult.success ? <CheckCircle2 className="w-3.5 h-3.5" /> : <XCircle className="w-3.5 h-3.5" />}
          {testResult.success ? `Connected (${testResult.latency_ms}ms)` : testResult.error}
        </div>
      )}
      <div className="flex justify-end gap-2">
        <Button variant="outline" onClick={onCancel}>Cancel</Button>
        <Button variant="outline" onClick={handleTest} disabled={testing}>
          {testing ? 'Testing...' : 'Test Connection'}
        </Button>
        <Button onClick={handleSave} disabled={saving} className="bg-[#0060AF] hover:bg-[#004d8c]">
          {saving ? 'Adding...' : 'Add Instance'}
        </Button>
      </div>
    </div>
  );
}

function DAGRow({ dag, connectionId, airflowHost, onRefreshDags }) {
  const [expanded, setExpanded] = useState(false);
  const [runs, setRuns] = useState(null);
  const [tasks, setTasks] = useState(null);
  const [taskInstances, setTaskInstances] = useState(null);
  const [selectedRun, setSelectedRun] = useState(null);
  const [loadingRuns, setLoadingRuns] = useState(false);
  const [triggering, setTriggering] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [logContent, setLogContent] = useState(null);
  const [logLoading, setLogLoading] = useState(false);
  const [logTask, setLogTask] = useState(null);

  const fetchRuns = useCallback(async () => {
    setLoadingRuns(true);
    try {
      const resp = await fetch(`${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}/dagRuns?limit=10`);
      const data = await resp.json();
      setRuns(data.dag_runs || []);
      if (data.dag_runs?.length > 0) setSelectedRun(data.dag_runs[0]);
    } catch { toast.error('Failed to fetch runs'); }
    finally { setLoadingRuns(false); }
  }, [connectionId, dag.dag_id]);

  const fetchTasks = useCallback(async () => {
    try {
      const resp = await fetch(`${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}/tasks`);
      const data = await resp.json();
      setTasks(data.tasks || []);
    } catch {}
  }, [connectionId, dag.dag_id]);

  const fetchTaskInstances = useCallback(async (run) => {
    if (!run) return;
    try {
      const resp = await fetch(
        `${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}/dagRuns/${encodeURIComponent(run.dag_run_id)}/taskInstances`
      );
      const data = await resp.json();
      setTaskInstances(data.task_instances || []);
    } catch { setTaskInstances([]); }
  }, [connectionId, dag.dag_id]);

  useEffect(() => {
    if (expanded && !runs) { fetchRuns(); fetchTasks(); }
  }, [expanded, runs, fetchRuns, fetchTasks]);

  useEffect(() => {
    if (selectedRun) fetchTaskInstances(selectedRun);
  }, [selectedRun, fetchTaskInstances]);

  const handleTrigger = async () => {
    setTriggering(true);
    try {
      const resp = await fetch(`${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}/dagRuns`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ conf: {} }),
      });
      if (resp.ok) { toast.success(`Triggered "${dag.dag_id}"`); fetchRuns(); }
      else { const e = await resp.json(); toast.error(e.error || 'Failed to trigger'); }
    } catch (err) { toast.error(err.message); }
    finally { setTriggering(false); }
  };

  const handleTogglePause = async () => {
    setToggling(true);
    try {
      const resp = await fetch(`${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ is_paused: !dag.is_paused }),
      });
      if (resp.ok) { toast.success(dag.is_paused ? 'DAG unpaused' : 'DAG paused'); onRefreshDags?.(); }
      else { const e = await resp.json(); toast.error(e.error || 'Failed'); }
    } catch (err) { toast.error(err.message); }
    finally { setToggling(false); }
  };

  const fetchLog = async (ti) => {
    setLogTask(ti.task_id);
    setLogLoading(true);
    setLogContent(null);
    try {
      const resp = await fetch(
        `${API}/${connectionId}/dags/${encodeURIComponent(dag.dag_id)}/dagRuns/${encodeURIComponent(selectedRun.dag_run_id)}/taskInstances/${encodeURIComponent(ti.task_id)}/logs/${ti.try_number || 1}`
      );
      if (resp.ok) setLogContent(await resp.text());
      else setLogContent('Failed to fetch log.');
    } catch { setLogContent('Error fetching log.'); }
    finally { setLogLoading(false); }
  };

  const latestRun = runs?.[0];

  return (
    <div className="border border-slate-200 dark:border-slate-700 rounded-xl bg-white dark:bg-slate-800 overflow-hidden">
      <div
        className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className={`w-2.5 h-2.5 rounded-full shrink-0 ${dag.is_paused ? 'bg-amber-400' : dag.is_active ? 'bg-emerald-400' : 'bg-slate-300'}`} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-mono text-sm font-semibold text-slate-800 dark:text-slate-100 truncate">{dag.dag_id}</span>
            {dag.is_paused && <span className="text-xs px-1.5 py-0.5 rounded bg-amber-100 text-amber-700 font-medium">Paused</span>}
          </div>
          <div className="flex items-center gap-3 mt-0.5 text-xs text-slate-400">
            {(dag.schedule || dag.schedule_interval) && <span>{dag.schedule || dag.schedule_interval}</span>}
            {dag.owners?.length > 0 && <span>by {dag.owners.join(', ')}</span>}
            {dag.tags?.length > 0 && dag.tags.map(t => (
              <span key={t} className="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-600 text-slate-500 dark:text-slate-300">{t}</span>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {latestRun && <StateBadge state={latestRun.state} />}
          {expanded ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
        </div>
      </div>

      {expanded && (
        <div className="border-t border-slate-100 dark:border-slate-700 px-4 py-4 space-y-4">
          {dag.description && <p className="text-xs text-slate-500 dark:text-slate-400">{dag.description}</p>}

          <div className="flex flex-wrap gap-2">
            <Button size="sm" variant="outline" className="gap-1.5 text-xs" onClick={handleTogglePause} disabled={toggling}>
              {dag.is_paused ? <Play className="w-3.5 h-3.5" /> : <Pause className="w-3.5 h-3.5" />}
              {toggling ? '...' : dag.is_paused ? 'Unpause' : 'Pause'}
            </Button>
            <Button size="sm" className="gap-1.5 text-xs bg-emerald-600 hover:bg-emerald-700" onClick={handleTrigger} disabled={triggering || dag.is_paused}>
              <Zap className="w-3.5 h-3.5" />
              {triggering ? 'Triggering...' : 'Trigger Run'}
            </Button>
            <Button size="sm" variant="outline" className="gap-1.5 text-xs" onClick={() => { setRuns(null); fetchRuns(); }}>
              <RefreshCw className="w-3.5 h-3.5" /> Refresh
            </Button>
            {airflowHost && (
              <a href={`${airflowHost}/dags/${dag.dag_id}`} target="_blank" rel="noopener noreferrer"
                className="inline-flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-md border border-slate-200 dark:border-slate-600 hover:bg-slate-50 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300 font-medium">
                <ExternalLink className="w-3.5 h-3.5" /> Open in Airflow
              </a>
            )}
          </div>

          {tasks && tasks.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-slate-700 dark:text-slate-300 mb-2">Tasks ({tasks.length})</p>
              <div className="flex flex-wrap gap-1.5">
                {tasks.map(t => (
                  <span key={t.task_id} className="text-xs px-2 py-1 rounded-lg bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 font-mono">
                    {t.task_id}
                  </span>
                ))}
              </div>
            </div>
          )}

          <div>
            <p className="text-xs font-semibold text-slate-700 dark:text-slate-300 mb-2">Recent Runs</p>
            {loadingRuns && <div className="flex items-center gap-2 text-xs text-slate-400"><Loader2 className="w-3.5 h-3.5 animate-spin" /> Loading...</div>}
            {runs && runs.length === 0 && <p className="text-xs text-slate-400">No runs found</p>}
            {runs && runs.length > 0 && (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-slate-100 dark:border-slate-700">
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Status</th>
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Run ID</th>
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Executed</th>
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Duration</th>
                    </tr>
                  </thead>
                  <tbody>
                    {runs.map(run => (
                      <tr
                        key={run.dag_run_id}
                        onClick={() => setSelectedRun(run)}
                        className={`cursor-pointer transition-colors text-xs ${
                          selectedRun?.dag_run_id === run.dag_run_id
                            ? 'bg-blue-50 dark:bg-blue-900/20'
                            : 'hover:bg-slate-50 dark:hover:bg-slate-700/50'
                        }`}
                      >
                        <td className="py-2 px-3"><StateBadge state={run.state} /></td>
                        <td className="py-2 px-3 font-mono text-slate-600 dark:text-slate-300 truncate max-w-[200px]">{run.dag_run_id}</td>
                        <td className="py-2 px-3 text-slate-500">{formatTime(run.logical_date || run.execution_date)}</td>
                        <td className="py-2 px-3 text-slate-500">
                          {run.start_date && run.end_date
                            ? formatDuration((new Date(run.end_date) - new Date(run.start_date)) / 1000)
                            : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {selectedRun && taskInstances && (
            <div>
              <p className="text-xs font-semibold text-slate-700 dark:text-slate-300 mb-2">
                Task Instances — <span className="font-mono">{selectedRun.dag_run_id}</span>
              </p>
              {taskInstances.length === 0 && <p className="text-xs text-slate-400">No task instances</p>}
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-slate-100 dark:border-slate-700">
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Status</th>
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Task</th>
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Operator</th>
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Duration</th>
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Try</th>
                      <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Log</th>
                    </tr>
                  </thead>
                  <tbody>
                    {taskInstances.map(ti => (
                      <tr key={ti.task_id} className="hover:bg-slate-50 dark:hover:bg-slate-700/50 text-xs">
                        <td className="py-2 px-3"><StateBadge state={ti.state} /></td>
                        <td className="py-2 px-3 font-mono text-slate-700 dark:text-slate-200">{ti.task_id}</td>
                        <td className="py-2 px-3 text-slate-500">{ti.operator}</td>
                        <td className="py-2 px-3 text-slate-500">{formatDuration(ti.duration)}</td>
                        <td className="py-2 px-3 text-slate-500">{ti.try_number}</td>
                        <td className="py-2 px-3">
                          <button
                            onClick={() => fetchLog(ti)}
                            className="text-[#0060AF] hover:text-[#004d8c] font-medium"
                            title="View log"
                          >
                            <FileText className="w-3.5 h-3.5" />
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {logTask && (
            <div className="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
              <div className="flex items-center justify-between bg-slate-100 dark:bg-slate-700 px-3 py-2">
                <span className="text-xs font-semibold text-slate-700 dark:text-slate-200">Log: {logTask}</span>
                <button onClick={() => { setLogTask(null); setLogContent(null); }} className="text-slate-400 hover:text-slate-600">
                  <X className="w-4 h-4" />
                </button>
              </div>
              <div className="bg-slate-900 text-slate-100 p-3 max-h-60 overflow-auto">
                {logLoading
                  ? <div className="flex items-center gap-2 text-xs text-slate-400"><Loader2 className="w-3.5 h-3.5 animate-spin" /> Loading log...</div>
                  : <pre className="text-xs font-mono whitespace-pre-wrap">{logContent}</pre>
                }
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default function OrchestrationPanel({ variant = "full" }) {
  const [connections, setConnections] = useState([]);
  const [selectedConnId, setSelectedConnId] = useState('');
  const [dags, setDags] = useState([]);
  const [totalDags, setTotalDags] = useState(0);
  const [loading, setLoading] = useState(true);
  const [loadingDags, setLoadingDags] = useState(false);
  const [search, setSearch] = useState('');
  const [showAdd, setShowAdd] = useState(false);

  const loadConnections = useCallback(async () => {
    try {
      const resp = await fetch(`${API}/connections`);
      const data = await resp.json();
      setConnections(data);
      if (data.length > 0 && !selectedConnId) setSelectedConnId(data[0].id);
    } catch {}
    finally { setLoading(false); }
  }, [selectedConnId]);

  const loadDags = useCallback(async () => {
    if (!selectedConnId) return;
    setLoadingDags(true);
    try {
      const params = new URLSearchParams({ limit: variant === 'summary' ? '6' : '100' });
      if (search) params.set('search', search);
      const resp = await fetch(`${API}/${selectedConnId}/dags?${params}`);
      const data = await resp.json();
      if (data.error) { setDags([]); }
      else { setDags(data.dags || []); setTotalDags(data.total_entries || 0); }
    } catch { setDags([]); }
    finally { setLoadingDags(false); }
  }, [selectedConnId, search, variant]);

  useEffect(() => { loadConnections(); }, []);
  useEffect(() => { if (selectedConnId) loadDags(); }, [selectedConnId, loadDags]);

  const selectedConn = connections.find(c => c.id === selectedConnId);
  const activeDags = dags.filter(d => !d.is_paused).length;
  const pausedDags = dags.filter(d => d.is_paused).length;

  if (loading) {
    return (
      <div className="flex items-center justify-center h-20">
        <Loader2 className="w-5 h-5 text-[#0060AF] animate-spin" />
      </div>
    );
  }

  if (variant === 'summary') {
    return (
      <div className="space-y-3">
        {connections.length === 0 ? (
          <div className="text-center py-4">
            <Wind className="w-8 h-8 text-slate-300 mx-auto mb-2" />
            <p className="text-sm text-slate-500 dark:text-slate-400 mb-2">No Airflow instances connected</p>
            <Button size="sm" variant="outline" onClick={() => setShowAdd(true)} className="gap-1.5 text-xs">
              <Plus className="w-3.5 h-3.5" /> Connect Airflow
            </Button>
            {showAdd && (
              <div className="mt-3 text-left">
                <AddInstanceForm
                  onAdded={() => { setShowAdd(false); loadConnections(); }}
                  onCancel={() => setShowAdd(false)}
                />
              </div>
            )}
          </div>
        ) : (
          <>
            <div className="flex items-center gap-2">
              <Select value={selectedConnId} onValueChange={setSelectedConnId}>
                <SelectTrigger className="h-8 text-xs flex-1"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {connections.map(c => <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>)}
                </SelectContent>
              </Select>
              <Button size="sm" variant="ghost" onClick={loadDags} className="h-8 w-8 p-0">
                <RefreshCw className="w-3.5 h-3.5" />
              </Button>
            </div>

            {loadingDags ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="w-4 h-4 text-[#0060AF] animate-spin" />
              </div>
            ) : dags.length === 0 ? (
              <p className="text-xs text-slate-400 text-center py-4">No DAGs found</p>
            ) : (
              <>
                <div className="flex items-center gap-4 text-xs text-slate-500 px-1">
                  <span>{totalDags} DAGs total</span>
                  <span className="text-emerald-600">{activeDags} active</span>
                  {pausedDags > 0 && <span className="text-amber-600">{pausedDags} paused</span>}
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-slate-100 dark:border-slate-700">
                        <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">DAG</th>
                        <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Status</th>
                        <th className="text-left text-xs font-medium text-slate-500 uppercase tracking-wider py-2 px-3">Schedule</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dags.slice(0, 6).map(dag => (
                        <tr key={dag.dag_id} className="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
                          <td className="py-2 px-3">
                            <span className="font-mono text-xs text-slate-700 dark:text-slate-200 truncate">{dag.dag_id}</span>
                          </td>
                          <td className="py-2 px-3">
                            {dag.is_paused
                              ? <span className="inline-flex items-center gap-1 text-xs text-amber-600"><Clock className="w-3 h-3" /> Paused</span>
                              : <span className="inline-flex items-center gap-1 text-xs text-emerald-600"><CheckCircle2 className="w-3 h-3" /> Active</span>
                            }
                          </td>
                          <td className="py-2 px-3 text-xs text-slate-400">{dag.schedule || dag.schedule_interval || 'manual'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <Link to={createPageUrl("Pipelines")} className="block text-center text-xs text-[#0060AF] hover:text-[#004d8c] font-medium pt-2">
                  View all DAGs on Pipelines <ArrowRight className="w-3 h-3 inline" />
                </Link>
              </>
            )}
          </>
        )}
      </div>
    );
  }

  return (
    <Card className="border-slate-200 dark:border-slate-700">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold flex items-center gap-2">
            <Wind className="w-5 h-5 text-[#0060AF]" />
            Orchestration
          </CardTitle>
          <div className="flex gap-2">
            <Button size="sm" variant="outline" onClick={() => setShowAdd(v => !v)} className="gap-1.5 text-xs">
              <Plus className="w-3.5 h-3.5" /> Add Instance
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {showAdd && (
          <AddInstanceForm
            onAdded={() => { setShowAdd(false); loadConnections(); }}
            onCancel={() => setShowAdd(false)}
          />
        )}

        {connections.length === 0 && !showAdd ? (
          <div className="text-center py-8">
            <Wind className="w-12 h-12 text-slate-300 mx-auto mb-3" />
            <h3 className="text-sm font-semibold text-slate-600 dark:text-slate-300 mb-1">No Airflow Instances Connected</h3>
            <p className="text-xs text-slate-400 mb-4 max-w-md mx-auto">
              Connect your Airflow instance to monitor DAGs, view run history, trigger executions, and read task logs.
            </p>
            <Button size="sm" onClick={() => setShowAdd(true)} className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c]">
              <Plus className="w-3.5 h-3.5" /> Connect Airflow
            </Button>
          </div>
        ) : connections.length > 0 && (
          <>
            <div className="flex items-center gap-3 flex-wrap">
              <div className="min-w-[200px] flex-1 max-w-xs">
                <Label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Instance</Label>
                <Select value={selectedConnId} onValueChange={v => { setSelectedConnId(v); setSearch(''); }}>
                  <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {connections.map(c => (
                      <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="flex-1 min-w-[180px]">
                <Label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Search DAGs</Label>
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                  <Input
                    placeholder="Filter by DAG ID..."
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    className="pl-9 h-9"
                  />
                </div>
              </div>
              <div className="flex gap-2 pt-5">
                <Button size="sm" variant="outline" onClick={loadDags} className="gap-1.5">
                  <RefreshCw className="w-3.5 h-3.5" /> Refresh
                </Button>
                {selectedConn?.host && (
                  <a href={selectedConn.host} target="_blank" rel="noopener noreferrer">
                    <Button size="sm" variant="outline" className="gap-1.5">
                      <ExternalLink className="w-3.5 h-3.5" /> Open Airflow
                    </Button>
                  </a>
                )}
              </div>
            </div>

            <div className="flex items-center gap-4 text-xs text-slate-500 px-1">
              <span>{totalDags} DAGs total</span>
              <span className="text-emerald-600">{activeDags} active</span>
              {pausedDags > 0 && <span className="text-amber-600">{pausedDags} paused</span>}
            </div>

            {loadingDags ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="w-6 h-6 text-[#0060AF] animate-spin" />
              </div>
            ) : dags.length === 0 ? (
              <div className="text-center py-6">
                <AlertCircle className="w-8 h-8 text-slate-300 mx-auto mb-2" />
                <p className="text-sm text-slate-500">No DAGs found{search ? ` matching "${search}"` : ''}</p>
              </div>
            ) : (
              <div className="space-y-3">
                {dags.map(dag => (
                  <DAGRow
                    key={dag.dag_id}
                    dag={dag}
                    connectionId={selectedConnId}
                    airflowHost={selectedConn?.host}
                    onRefreshDags={loadDags}
                  />
                ))}
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
