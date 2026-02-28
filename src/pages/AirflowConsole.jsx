import { useState, useEffect } from "react";
import { Wind, ExternalLink, AlertTriangle, MonitorDot, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

export default function AirflowConsole() {
  const [connections, setConnections] = useState([]);
  const [selectedId, setSelectedId] = useState("");
  const [loading, setLoading] = useState(true);
  const [iframeKey, setIframeKey] = useState(0);
  const [iframeError, setIframeError] = useState(false);

  useEffect(() => {
    fetch("/api/airflow/connections")
      .then(r => r.json())
      .then(data => {
        const conns = data.connections || [];
        setConnections(conns);
        if (conns.length > 0 && !selectedId) {
          setSelectedId(conns[0].id);
        }
      })
      .catch(() => setConnections([]))
      .finally(() => setLoading(false));
  }, []);

  const selected = connections.find(c => c.id === selectedId);
  const airflowUrl = selected?.host ? (
    selected.host.startsWith("http") ? selected.host : `https://${selected.host}`
  ).replace(/\/+$/, "") : null;

  const handleRefresh = () => {
    setIframeError(false);
    setIframeKey(k => k + 1);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-[60vh] text-slate-400 dark:text-slate-500">
        <Wind className="w-5 h-5 animate-pulse mr-2" />
        Loading Airflow connections...
      </div>
    );
  }

  if (connections.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-[60vh] text-center px-4">
        <div className="w-16 h-16 rounded-2xl bg-slate-100 dark:bg-slate-800 flex items-center justify-center mb-4">
          <MonitorDot className="w-8 h-8 text-slate-400" />
        </div>
        <h2 className="text-lg font-semibold text-slate-900 dark:text-white mb-2">No Airflow Instances</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 max-w-md">
          Add an Airflow connection on the Airflow admin page to use the console. The console embeds the Airflow web UI directly in DataFlow.
        </p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-[calc(100vh-2rem)]">
      <div className="flex items-center justify-between gap-4 px-6 py-3 border-b border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 shrink-0">
        <div className="flex items-center gap-3">
          <Wind className="w-5 h-5 text-[#0060AF]" />
          <h1 className="text-lg font-semibold text-slate-900 dark:text-white">Airflow Console</h1>
        </div>

        <div className="flex items-center gap-2">
          <Select value={selectedId} onValueChange={v => { setSelectedId(v); setIframeError(false); setIframeKey(k => k + 1); }}>
            <SelectTrigger className="w-[220px] h-8 text-sm">
              <SelectValue placeholder="Select instance" />
            </SelectTrigger>
            <SelectContent>
              {connections.map(c => (
                <SelectItem key={c.id} value={c.id}>
                  <span className="flex items-center gap-2">
                    <Wind className="w-3.5 h-3.5 text-slate-400" />
                    {c.name}
                  </span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Button size="sm" variant="outline" className="h-8 gap-1.5 text-xs" onClick={handleRefresh}>
            <RefreshCw className="w-3.5 h-3.5" />
            Reload
          </Button>

          {airflowUrl && (
            <a
              href={airflowUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 h-8 px-3 text-xs font-medium border rounded-md border-slate-200 dark:border-slate-600 hover:bg-slate-50 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300"
            >
              <ExternalLink className="w-3.5 h-3.5" />
              Open in New Tab
            </a>
          )}
        </div>
      </div>

      {iframeError && (
        <div className="mx-6 mt-4 rounded-lg border border-amber-200 dark:border-amber-800/50 bg-amber-50 dark:bg-amber-900/20 p-4">
          <div className="flex items-start gap-3">
            <AlertTriangle className="w-5 h-5 text-amber-600 dark:text-amber-400 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-medium text-amber-800 dark:text-amber-300">Unable to embed Airflow UI</p>
              <p className="text-xs text-amber-700 dark:text-amber-400 mt-1">
                The Airflow server may block embedding via <code className="bg-amber-100 dark:bg-amber-800/50 px-1 rounded">X-Frame-Options</code> or <code className="bg-amber-100 dark:bg-amber-800/50 px-1 rounded">Content-Security-Policy</code> headers.
                To enable embedding, configure your Airflow instance:
              </p>
              <pre className="mt-2 text-xs bg-amber-100 dark:bg-amber-800/40 rounded p-2 text-amber-900 dark:text-amber-200 overflow-x-auto">
{`# airflow.cfg
[webserver]
x_frame_enabled = True

# Or set the environment variable:
AIRFLOW__WEBSERVER__X_FRAME_ENABLED=True`}
              </pre>
              <div className="mt-3 flex gap-2">
                {airflowUrl && (
                  <a
                    href={airflowUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 text-xs font-medium text-[#0060AF] hover:underline"
                  >
                    <ExternalLink className="w-3 h-3" />
                    Open Airflow in a new tab instead
                  </a>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {airflowUrl ? (
        <div className="flex-1 min-h-0 p-2">
          <iframe
            key={iframeKey}
            src={airflowUrl}
            title={`Airflow Console — ${selected?.name || "Instance"}`}
            className="w-full h-full rounded-lg border border-slate-200 dark:border-slate-700 bg-white"
            sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox"
            onError={() => setIframeError(true)}
            onLoad={(e) => {
              try {
                const doc = e.target.contentDocument;
                if (!doc || !doc.body || doc.body.innerHTML === "") {
                  setIframeError(true);
                }
              } catch {
                setIframeError(true);
              }
            }}
          />
        </div>
      ) : (
        <div className="flex-1 flex items-center justify-center text-slate-400 dark:text-slate-500 text-sm">
          Select an Airflow instance to view its console
        </div>
      )}
    </div>
  );
}
