import { useState, useEffect, useMemo } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Copy, Download, Check, Rocket, FileCode2, FileText, ChevronDown, ChevronUp, X, GitBranch, LayoutTemplate, Loader2, AlertCircle, ExternalLink, CheckCircle2, Lock, Eye, EyeOff, Info } from "lucide-react";
import { buildJobSpec, FLAT_FILE_PLATFORMS } from "@/components/JobSpecExport";
import { BUILTIN_TEMPLATES, getAllTemplates, getTemplatesForSource, getDefaultTemplateId, fillTemplate } from "@/components/DagTemplates";
import { dataflow } from "@/api/client";
import { toYaml } from "@/utils/toYaml";
import { cn } from "@/lib/utils";

function ArtifactCard({ title, filename, icon: Icon, content }) {
  const [expanded, setExpanded] = useState(false);
  const [copied, setCopied] = useState(false);

  const handleCopy = async (e) => {
    e.stopPropagation();
    await navigator.clipboard.writeText(content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = (e) => {
    e.stopPropagation();
    const blob = new Blob([content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
      <div
        className="flex items-center gap-3 px-3 py-2.5 bg-slate-50 dark:bg-slate-800/50 cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors"
        onClick={() => setExpanded(v => !v)}
      >
        <Icon className="w-4 h-4 text-[#0060AF] shrink-0" />
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium text-slate-800 dark:text-slate-200">{title}</p>
          <p className="text-[10px] text-slate-400 font-mono truncate">{filename}</p>
        </div>
        <div className="flex items-center gap-1.5 ml-auto">
          <Button type="button" size="sm" variant="ghost" className="h-6 px-1.5 text-[10px] gap-1" onClick={handleCopy}>
            {copied ? <Check className="w-3 h-3 text-emerald-600" /> : <Copy className="w-3 h-3" />}
          </Button>
          <Button type="button" size="sm" variant="ghost" className="h-6 px-1.5 text-[10px]" onClick={handleDownload}>
            <Download className="w-3 h-3" />
          </Button>
          {expanded ? <ChevronUp className="w-3.5 h-3.5 text-slate-400" /> : <ChevronDown className="w-3.5 h-3.5 text-slate-400" />}
        </div>
      </div>
      {expanded && (
        <div className="bg-slate-950 overflow-hidden">
          <pre className="p-3 text-xs text-emerald-300 font-mono whitespace-pre overflow-auto max-h-56 leading-relaxed">
            {content}
          </pre>
        </div>
      )}
    </div>
  );
}

function GitLabIcon({ className }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M22.65 14.39L12 22.13 1.35 14.39a.84.84 0 01-.3-.94l1.22-3.78 2.44-7.51A.42.42 0 014.82 2a.43.43 0 01.58 0 .42.42 0 01.11.18l2.44 7.49h8.1l2.44-7.51A.42.42 0 0118.6 2a.43.43 0 01.58 0 .42.42 0 01.11.18l2.44 7.51L23 13.45a.84.84 0 01-.35.94z" />
    </svg>
  );
}

export default function GitCheckinDialog({ open, onOpenChange, pipelineData, connections }) {
  if (!pipelineData) return null;

  const sourceConn = connections.find(c => c.id === pipelineData.source_connection_id);
  const sourcePlatform = sourceConn?.platform || "";
  const isFlatFile = FLAT_FILE_PLATFORMS.includes(sourcePlatform);

  const [customTemplates, setCustomTemplates] = useState([]);
  const [templateOverride, setTemplateOverride] = useState(null);
  const [commitBranch, setCommitBranch] = useState("main");
  const [commitMsg, setCommitMsg] = useState("");
  const [deploying, setDeploying] = useState(false);
  const [deployResult, setDeployResult] = useState(null);
  const [deployError, setDeployError] = useState(null);
  const [glConfig, setGlConfig] = useState(null);
  const [glUsername, setGlUsername] = useState("");
  const [glPassword, setGlPassword] = useState("");
  const [glStatus, setGlStatus] = useState(null);
  const [glAuthenticating, setGlAuthenticating] = useState(false);
  const [showPassword, setShowPassword] = useState(false);

  useEffect(() => {
    dataflow.entities.DagTemplate.list().then(res => {
      if (Array.isArray(res)) setCustomTemplates(res);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    if (open) {
      setDeployResult(null);
      setDeployError(null);
      setDeploying(false);
      setGlStatus(null);
      fetch("/api/gitlab/config").then(r => r.json()).then(setGlConfig).catch(() => setGlConfig({ configured: false }));
    }
  }, [open]);

  const allTemplates = getAllTemplates(customTemplates);
  const selectedTemplateId = templateOverride || pipelineData.dag_template_id || getDefaultTemplateId(sourcePlatform);

  const nameClean = (pipelineData.name || "pipeline").replace(/[^a-z0-9_-]/gi, "_").toLowerCase();
  const dagFilename = `${nameClean}-airflow-dag.yaml`;
  const specFilename = `${nameClean}-pipelinespec.yaml`;
  const repoPath = `specs/${nameClean}/`;

  const spec = buildJobSpec({ id: pipelineData.id || "(unsaved)", ...pipelineData, dq_rules: pipelineData.dq_rules || {} }, connections);
  const specContent = `# DataFlow Pipeline Spec — ${pipelineData.name || "untitled"}\n` + toYaml(JSON.parse(JSON.stringify(spec)));
  const airflowDagYaml = fillTemplate(selectedTemplateId, pipelineData, connections, customTemplates);
  const selectedTmpl = allTemplates.find(t => t.id === selectedTemplateId);

  const builtinForSource = BUILTIN_TEMPLATES.filter(t => t.sourceType === (isFlatFile ? "flat_file" : "database"));
  const builtinOther = BUILTIN_TEMPLATES.filter(t => t.sourceType !== (isFlatFile ? "flat_file" : "database"));
  const customForSource = customTemplates.filter(t => !t.sourceType || t.sourceType === "any" || t.sourceType === (isFlatFile ? "flat_file" : "database"));
  const customOther = customTemplates.filter(t => t.sourceType && t.sourceType !== "any" && t.sourceType !== (isFlatFile ? "flat_file" : "database"));

  const defaultCommitMsg = pipelineData._isUpdate
    ? `Update pipeline: ${pipelineData.name}`
    : `Add pipeline: ${pipelineData.name}`;

  const filePayload = [
    { path: `${repoPath}${dagFilename}`, content: airflowDagYaml },
    { path: `${repoPath}${specFilename}`, content: specContent },
  ];

  const handleGitLabAuth = async () => {
    if (!glUsername || !glPassword) return;
    setGlAuthenticating(true);
    setGlStatus(null);
    try {
      const res = await fetch("/api/gitlab/status", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: glUsername, password: glPassword }),
      });
      setGlStatus(await res.json());
    } catch (err) {
      setGlStatus({ connected: false, error: err.message });
    } finally {
      setGlAuthenticating(false);
    }
  };

  const handleDeploy = async () => {
    setDeploying(true);
    setDeployResult(null);
    setDeployError(null);
    try {
      const res = await fetch("/api/gitlab/commit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: glUsername,
          password: glPassword,
          branch: commitBranch || "main",
          commitMessage: commitMsg || defaultCommitMsg,
          files: filePayload,
        }),
      });
      const data = await res.json();
      if (!res.ok || !data.success) throw new Error(data.error || "Commit failed");
      setDeployResult({ ...data, provider: "gitlab" });
      if (pipelineData?.id) {
        try {
          await dataflow.entities.Pipeline.update(pipelineData.id, { ...pipelineData, status: "active" });
        } catch {}
      }
    } catch (err) {
      setDeployError(err.message);
    } finally {
      setDeploying(false);
    }
  };

  const canDeploy = glStatus?.connected && glUsername && glPassword;

  return (
    <Dialog open={open} onOpenChange={(v) => { if (!deploying && !v) onOpenChange(false); }}>
      <DialogContent
        preventClose={deploying}
        className="w-[95vw] max-w-3xl max-h-[90vh] overflow-y-auto"
        onPointerDownOutside={(e) => e.preventDefault()}
        onInteractOutside={(e) => e.preventDefault()}
        onEscapeKeyDown={(e) => { if (deploying) e.preventDefault(); }}
      >
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <Rocket className="w-4 h-4 text-[#0060AF]" />
            Deploy — {pipelineData.name}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-4">

          <div className="border border-slate-200 dark:border-slate-700 rounded-xl overflow-hidden">
            <div className="flex items-center gap-2.5 px-4 py-2.5 bg-slate-50 dark:bg-slate-800/50 border-b border-slate-200 dark:border-slate-700">
              <GitLabIcon className="w-4 h-4 text-orange-600" />
              <span className="text-sm font-semibold text-slate-800 dark:text-slate-200">GitLab (LDAP)</span>
              {glStatus && (
                <span className={cn(
                  "ml-auto inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-medium",
                  glStatus.connected
                    ? "bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400"
                    : "bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400"
                )}>
                  <span className={cn("w-1.5 h-1.5 rounded-full", glStatus.connected ? "bg-emerald-500" : "bg-red-500")} />
                  {glStatus.connected ? glStatus.login : "Not connected"}
                </span>
              )}
            </div>

            <div className="p-4 space-y-3">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <Label className="text-xs text-slate-500 dark:text-slate-400">GitLab URL</Label>
                  <Input value={glConfig?.url || ""} readOnly className="h-8 font-mono text-xs bg-slate-50 dark:bg-slate-800 text-slate-600 dark:text-slate-400 mt-1" placeholder="GITLAB_URL not set" />
                </div>
                <div>
                  <Label className="text-xs text-slate-500 dark:text-slate-400">Project</Label>
                  <Input value={glConfig?.project || ""} readOnly className="h-8 font-mono text-xs bg-slate-50 dark:bg-slate-800 text-slate-600 dark:text-slate-400 mt-1" placeholder="GITLAB_PROJECT not set" />
                </div>
              </div>

              {!glConfig?.configured && (
                <p className="text-xs text-amber-600 dark:text-amber-400 flex items-center gap-1.5 bg-amber-50 dark:bg-amber-900/20 rounded-lg px-3 py-2">
                  <Info className="w-3.5 h-3.5 shrink-0" />
                  Set <code className="font-mono font-semibold">GITLAB_URL</code> and <code className="font-mono font-semibold">GITLAB_PROJECT</code> to configure.
                </p>
              )}

              <div className="border-t border-slate-100 dark:border-slate-700 pt-3">
                <div className="flex items-center gap-1.5 mb-2">
                  <Lock className="w-3 h-3 text-slate-400" />
                  <Label className="text-xs font-semibold text-slate-600 dark:text-slate-400">LDAP Credentials</Label>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <Input
                    type="text"
                    value={glUsername}
                    onChange={e => { setGlUsername(e.target.value); setGlStatus(null); }}
                    className="h-8 text-xs"
                    placeholder="Username"
                    autoComplete="username"
                  />
                  <div className="relative">
                    <Input
                      type={showPassword ? "text" : "password"}
                      value={glPassword}
                      onChange={e => { setGlPassword(e.target.value); setGlStatus(null); }}
                      className="h-8 text-xs pr-8"
                      placeholder="Password"
                      autoComplete="current-password"
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(v => !v)}
                      className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
                      tabIndex={-1}
                    >
                      {showPassword ? <EyeOff className="w-3 h-3" /> : <Eye className="w-3 h-3" />}
                    </button>
                  </div>
                </div>
                <div className="flex items-center gap-2 mt-2">
                  <Button
                    type="button"
                    size="sm"
                    disabled={!glUsername || !glPassword || glAuthenticating || !glConfig?.configured}
                    onClick={handleGitLabAuth}
                    className="h-7 px-3 text-xs bg-[#0060AF] hover:bg-[#004d8c] text-white"
                  >
                    {glAuthenticating ? <><Loader2 className="w-3 h-3 animate-spin mr-1" /> Verifying...</> : "Authenticate"}
                  </Button>
                  {glStatus && !glStatus.connected && (
                    <span className="text-xs text-red-500">{glStatus.error || "Authentication failed"}</span>
                  )}
                </div>
                <p className="text-[10px] text-slate-400 mt-1.5">Credentials are used for this commit only and are never stored.</p>
              </div>

              <div className="border-t border-slate-100 dark:border-slate-700 pt-3">
                <div className="grid grid-cols-3 gap-2">
                  <div>
                    <Label className="text-xs text-slate-500 dark:text-slate-400">Branch</Label>
                    <Input value={commitBranch} onChange={e => setCommitBranch(e.target.value)} className="h-8 font-mono text-xs mt-1" placeholder="main" />
                  </div>
                  <div className="col-span-2">
                    <Label className="text-xs text-slate-500 dark:text-slate-400">Commit Message</Label>
                    <Input value={commitMsg} onChange={e => setCommitMsg(e.target.value)} className="h-8 text-xs mt-1" placeholder={defaultCommitMsg} />
                  </div>
                </div>
                <div className="flex items-center gap-2 mt-2 text-[10px] text-slate-400">
                  <GitBranch className="w-3 h-3 shrink-0" />
                  <span>Target path:</span>
                  <code className="font-mono text-[#0060AF] font-semibold">{repoPath}</code>
                </div>
              </div>
            </div>
          </div>

          <div className="border border-[#0060AF]/15 rounded-xl p-3 space-y-2 bg-[#0060AF]/[0.02]">
            <div className="flex items-center gap-2">
              <LayoutTemplate className="w-3.5 h-3.5 text-[#0060AF]" />
              <span className="text-xs font-semibold text-slate-700 dark:text-slate-300">DAG Template</span>
            </div>
            <select
              value={selectedTemplateId}
              onChange={e => setTemplateOverride(e.target.value)}
              className="w-full px-3 py-1.5 text-xs border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 focus:ring-2 focus:ring-[#0060AF]/30 focus:border-[#0060AF] outline-none font-medium"
            >
              {builtinForSource.length > 0 && (
                <optgroup label={isFlatFile ? "Flat File Templates" : "Database Templates"}>
                  {builtinForSource.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                </optgroup>
              )}
              {customForSource.length > 0 && (
                <optgroup label="Custom Templates">
                  {customForSource.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                </optgroup>
              )}
              {(builtinOther.length > 0 || customOther.length > 0) && (
                <optgroup label="Other Templates">
                  {builtinOther.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                  {customOther.map(t => <option key={t.id} value={t.id}>{t.name} (custom)</option>)}
                </optgroup>
              )}
            </select>
            {selectedTmpl && <p className="text-[10px] text-slate-400">{selectedTmpl.description}</p>}
          </div>

          <div className="space-y-2">
            <Label className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Artifacts</Label>
            <ArtifactCard
              title={`Airflow DAG — ${selectedTmpl?.name || "Template"}`}
              filename={dagFilename}
              icon={FileCode2}
              content={airflowDagYaml}
            />
            <ArtifactCard
              title="Pipeline Specification"
              filename={specFilename}
              icon={FileText}
              content={specContent}
            />
          </div>

          {deployResult && (
            <div className="rounded-xl border border-emerald-200 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-900/20 p-4 space-y-2">
              <div className="flex items-center gap-2">
                <CheckCircle2 className="w-4 h-4 text-emerald-600 shrink-0" />
                <p className="text-sm font-semibold text-emerald-800 dark:text-emerald-300">Committed to GitLab</p>
              </div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-emerald-700 dark:text-emerald-400">
                <p><span className="font-medium">Branch:</span> <code className="bg-emerald-100 dark:bg-emerald-800/50 px-1 rounded">{deployResult.branch}</code></p>
                <p><span className="font-medium">SHA:</span> <code className="bg-emerald-100 dark:bg-emerald-800/50 px-1 rounded font-mono">{(deployResult.short_sha || deployResult.sha || "").substring(0, 10)}</code></p>
                {deployResult.author && <p className="col-span-2"><span className="font-medium">Author:</span> {deployResult.author}</p>}
              </div>
              {deployResult.url && (
                <a href={deployResult.url} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1.5 text-xs font-medium text-[#0060AF] hover:underline">
                  <ExternalLink className="w-3 h-3" /> View on GitLab
                </a>
              )}
              <div className="flex items-center gap-2 pt-2 border-t border-emerald-200 dark:border-emerald-800">
                <Rocket className="w-3.5 h-3.5 text-[#0060AF] shrink-0" />
                <p className="text-[10px] text-[#0060AF]">CI/CD will auto-trigger to deploy the DAG to Airflow.</p>
              </div>
            </div>
          )}

          {deployError && (
            <div className="rounded-xl border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-4 flex items-start gap-2">
              <AlertCircle className="w-4 h-4 text-red-600 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-semibold text-red-800 dark:text-red-300">Commit failed</p>
                <p className="text-xs text-red-600 dark:text-red-400 mt-0.5">{deployError}</p>
              </div>
            </div>
          )}

          <div className="flex justify-end gap-2 pt-1 border-t border-slate-100 dark:border-slate-800">
            <Button type="button" variant="outline" size="sm" onClick={() => { if (!deploying) onOpenChange(false); }} disabled={deploying}>
              Close
            </Button>
            {!deployResult && (
              <Button
                type="button"
                size="sm"
                disabled={deploying || !canDeploy}
                onClick={(e) => { e.preventDefault(); e.stopPropagation(); handleDeploy(); }}
                className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] text-white"
              >
                {deploying ? <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Committing...</> : <><Rocket className="w-3.5 h-3.5" /> Deploy to GitLab</>}
              </Button>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
