import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Copy, Download, Check, Rocket, FileCode2, FileText, ChevronDown, ChevronUp, GitBranch, Loader2, AlertCircle, ExternalLink, CheckCircle2, Lock, Eye, EyeOff, FolderUp, Wind, Globe } from "lucide-react";
import { buildJobSpec, FLAT_FILE_PLATFORMS } from "@/components/JobSpecExport";
import { getAllTemplates, getDefaultTemplateId, fillTemplate } from "@/components/DagTemplates";
import { dataflow } from "@/api/client";
import { toYaml } from "@/utils/toYaml";
import { cn } from "@/lib/utils";

function GitLabIcon({ className }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M22.65 14.39L12 22.13 1.35 14.39a.84.84 0 01-.3-.94l1.22-3.78 2.44-7.51A.42.42 0 014.82 2a.43.43 0 01.58 0 .42.42 0 01.11.18l2.44 7.49h8.1l2.44-7.51A.42.42 0 0118.6 2a.43.43 0 01.58 0 .42.42 0 01.11.18l2.44 7.51L23 13.45a.84.84 0 01-.35.94z" />
    </svg>
  );
}

function StepIndicator({ steps }) {
  return (
    <div className="flex items-center gap-1 py-1.5">
      {steps.map((step, i) => (
        <div key={i} className="flex items-center gap-1">
          {i > 0 && <div className={cn("w-5 h-px", step.status !== "pending" ? "bg-emerald-400" : "bg-slate-200 dark:bg-slate-700")} />}
          <div className="flex items-center gap-1">
            {step.status === "done" && <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500 shrink-0" />}
            {step.status === "active" && <Loader2 className="w-3.5 h-3.5 text-[#0060AF] animate-spin shrink-0" />}
            {step.status === "error" && <AlertCircle className="w-3.5 h-3.5 text-red-500 shrink-0" />}
            {step.status === "pending" && <div className="w-3.5 h-3.5 rounded-full border-2 border-slate-300 dark:border-slate-600 shrink-0" />}
            <span className={cn(
              "text-[11px] font-medium whitespace-nowrap",
              step.status === "done" && "text-emerald-600 dark:text-emerald-400",
              step.status === "active" && "text-[#0060AF] dark:text-blue-400",
              step.status === "error" && "text-red-500",
              step.status === "pending" && "text-slate-400 dark:text-slate-500"
            )}>{step.label}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function ArtifactRow({ icon: Icon, filename, content }) {
  const [copied, setCopied] = useState(false);
  const [expanded, setExpanded] = useState(false);

  return (
    <div>
      <div className="flex items-center gap-2 py-1">
        <Icon className="w-3.5 h-3.5 text-[#0060AF] shrink-0" />
        <span className="text-xs font-mono text-slate-600 dark:text-slate-300 truncate flex-1">{filename}</span>
        <button type="button" onClick={() => setExpanded(v => !v)} className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 p-0.5">
          {expanded ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
        </button>
        <button type="button" onClick={async () => { await navigator.clipboard.writeText(content); setCopied(true); setTimeout(() => setCopied(false), 1500); }} className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 p-0.5">
          {copied ? <Check className="w-3 h-3 text-emerald-500" /> : <Copy className="w-3 h-3" />}
        </button>
        <button type="button" onClick={() => { const b = new Blob([content], { type: "text/plain" }); const u = URL.createObjectURL(b); const a = document.createElement("a"); a.href = u; a.download = filename; a.click(); URL.revokeObjectURL(u); }} className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 p-0.5">
          <Download className="w-3 h-3" />
        </button>
      </div>
      {expanded && (
        <pre className="mt-1 mb-2 p-2.5 text-[11px] text-emerald-300 font-mono whitespace-pre overflow-auto max-h-44 bg-slate-950 rounded-lg leading-relaxed">{content}</pre>
      )}
    </div>
  );
}

export default function DeployTabContent({ formData, connections, onDeploySuccess, onSavePipeline, onDeployStatusChange, editingJob }) {
  const sourceConn = connections.find(c => c.id === formData.source_connection_id);
  const sourcePlatform = sourceConn?.platform || "";

  const [customTemplates, setCustomTemplates] = useState([]);
  const [commitBranch, setCommitBranch] = useState("main");
  const [commitMsg, setCommitMsg] = useState("");
  const [glConfig, setGlConfig] = useState(null);
  const [glEnabled, setGlEnabled] = useState(false);
  const [glUrl, setGlUrl] = useState("");
  const [glProject, setGlProject] = useState("");
  const [glUsername, setGlUsername] = useState("");
  const [glPassword, setGlPassword] = useState("");
  const [glStatus, setGlStatus] = useState(null);
  const [glAuthenticating, setGlAuthenticating] = useState(false);
  const [showPassword, setShowPassword] = useState(false);

  const [airflowConns, setAirflowConns] = useState([]);
  const [selectedAirflowConn, setSelectedAirflowConn] = useState("");
  const [dagEnabled, setDagEnabled] = useState(false);

  const [dagSteps, setDagSteps] = useState([
    { label: "Save", status: "pending" },
    { label: "Generate", status: "pending" },
    { label: "Deploy", status: "pending" },
    { label: "Done", status: "pending" },
  ]);
  const [dagDeploying, setDagDeploying] = useState(false);
  const [dagResult, setDagResult] = useState(null);
  const [dagError, setDagError] = useState(null);

  const [glSteps, setGlSteps] = useState([
    { label: "Save", status: "pending" },
    { label: "Generate", status: "pending" },
    { label: "Commit", status: "pending" },
    { label: "Done", status: "pending" },
  ]);
  const [glDeploying, setGlDeploying] = useState(false);
  const [glResult, setGlResult] = useState(null);
  const [glError, setGlError] = useState(null);

  useEffect(() => {
    dataflow.entities.DagTemplate.list().then(res => {
      if (Array.isArray(res)) setCustomTemplates(res);
    }).catch(() => {});
    fetch("/api/gitlab/config").then(r => r.json()).then(cfg => {
      setGlConfig(cfg);
      if (cfg?.url) setGlUrl(cfg.url);
      if (cfg?.project) setGlProject(cfg.project);
    }).catch(() => setGlConfig({}));
    fetch("/api/airflow/connections").then(r => r.json()).then(conns => {
      if (Array.isArray(conns)) {
        setAirflowConns(conns.filter(c => c.dags_folder));
        if (conns.length > 0) {
          const withFolder = conns.find(c => c.dags_folder);
          if (withFolder) setSelectedAirflowConn(String(withFolder.id));
        }
      }
    }).catch(() => {});
  }, []);

  const selectedTemplateId = formData.dag_template_id || getDefaultTemplateId(sourcePlatform);
  const nameClean = (formData.name || "pipeline").replace(/[^a-z0-9_-]/gi, "_").toLowerCase();
  const dagFilename = `${nameClean}-airflow-dag.yaml`;
  const specFilename = `${nameClean}-pipelinespec.yaml`;
  const repoPath = `specs/${nameClean}/`;

  const spec = buildJobSpec({ id: formData.id || "(unsaved)", ...formData, dq_rules: formData.dq_rules || {} }, connections);
  const specContent = `# DataFlow Pipeline Spec — ${formData.name || "untitled"}\n` + toYaml(JSON.parse(JSON.stringify(spec)));
  const airflowDagYaml = fillTemplate(selectedTemplateId, formData, connections, customTemplates);

  const defaultCommitMsg = editingJob
    ? `Update pipeline: ${formData.name}`
    : `Add pipeline: ${formData.name}`;

  const filePayload = [
    { path: `${repoPath}${dagFilename}`, content: airflowDagYaml },
    { path: `${repoPath}${specFilename}`, content: specContent },
  ];

  const glRepoUrl = glUrl && glProject
    ? `${glUrl.replace(/\/+$/, '')}/${glProject}`
    : null;

  const handleGitLabAuth = async () => {
    if (!glUsername || !glPassword) return;
    setGlAuthenticating(true);
    setGlStatus(null);
    try {
      const res = await fetch("/api/gitlab/status", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: glUsername, password: glPassword, gitlabUrl: glUrl || undefined, project: glProject || undefined }),
      });
      setGlStatus(await res.json());
    } catch (err) {
      setGlStatus({ connected: false, error: err.message });
    } finally {
      setGlAuthenticating(false);
    }
  };

  const pipelineId = editingJob?.id || formData.id;

  const generateDeployPackage = async () => {
    if (!pipelineId) throw new Error("Pipeline must be saved before deploying");
    const res = await fetch(`/api/airflow/pipeline/${pipelineId}/generate-deploy-package`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    const data = await res.json();
    if (!res.ok || !data.success) throw new Error(data.error || "Failed to generate deploy package");
    const allFiles = [
      { filename: dagFilename, content: airflowDagYaml, subfolder: "dataflow", type: "dag_yaml" },
      { filename: specFilename, content: specContent, subfolder: "dataflow", type: "pipeline_spec" },
      ...data.files,
    ];
    return { ...data, files: allFiles };
  };

  const handleDagFolderDeploy = async () => {
    if (!selectedAirflowConn) return;
    setDagDeploying(true);
    setDagResult(null);
    setDagError(null);
    const mkSteps = (s, g, d, done) => [
      { label: "Save", status: s },
      { label: "Generate", status: g },
      { label: "Deploy", status: d },
      { label: "Done", status: done },
    ];
    setDagSteps(mkSteps("active", "pending", "pending", "pending"));
    if (onDeployStatusChange) onDeployStatusChange("deploying");
    try {
      if (onSavePipeline) await onSavePipeline();
      setDagSteps(mkSteps("done", "active", "pending", "pending"));

      const pkg = await generateDeployPackage();
      setDagSteps(mkSteps("done", "done", "active", "pending"));

      const deployedFiles = [];
      for (const file of pkg.files) {
        const checkinRes = await fetch(`/api/airflow/${selectedAirflowConn}/dags/checkin`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            filename: file.filename,
            content: file.content,
            subfolder: file.subfolder === null ? undefined : (file.subfolder || "dataflow"),
          }),
        });
        const checkinData = await checkinRes.json();
        if (!checkinRes.ok || !checkinData.success) throw new Error(checkinData.error || `Failed to deploy ${file.filename}`);
        deployedFiles.push({ filename: file.filename, path: checkinData.file_path, type: file.type });
      }
      setDagSteps(mkSteps("done", "done", "done", "done"));
      setDagResult({ success: true, deployed_files: deployedFiles, file_count: deployedFiles.length });
      if (onDeploySuccess) onDeploySuccess({ success: true });
    } catch (err) {
      setDagSteps(prev => prev.map(s => s.status === "active" ? { ...s, status: "error" } : s));
      setDagError(err.message);
      if (onDeployStatusChange) onDeployStatusChange("error");
    } finally {
      setDagDeploying(false);
    }
  };

  const handleGitLabDeploy = async () => {
    setGlDeploying(true);
    setGlResult(null);
    setGlError(null);
    const mkSteps = (s, g, c, done) => [
      { label: "Save", status: s },
      { label: "Generate", status: g },
      { label: "Commit", status: c },
      { label: "Done", status: done },
    ];
    setGlSteps(mkSteps("active", "pending", "pending", "pending"));
    if (onDeployStatusChange) onDeployStatusChange("deploying");
    try {
      if (onSavePipeline) await onSavePipeline();
      setGlSteps(mkSteps("done", "active", "pending", "pending"));

      const pkg = await generateDeployPackage();
      const allFilePayload = pkg.files.map(f => ({
        path: `${repoPath}${f.subfolder ? f.subfolder + '/' : ''}${f.filename}`,
        content: f.content,
      }));
      setGlSteps(mkSteps("done", "done", "active", "pending"));

      const res = await fetch("/api/gitlab/commit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: glUsername,
          password: glPassword,
          gitlabUrl: glUrl || undefined,
          project: glProject || undefined,
          branch: commitBranch || "main",
          commitMessage: commitMsg || defaultCommitMsg,
          files: allFilePayload,
        }),
      });
      const data = await res.json();
      if (!res.ok || !data.success) throw new Error(data.error || "Commit failed");
      setGlSteps(mkSteps("done", "done", "done", "done"));
      setGlResult({ ...data, provider: "gitlab", deployed_files: pkg.files.map(f => ({ filename: f.filename, type: f.type })), file_count: pkg.files.length });
      if (onDeploySuccess) onDeploySuccess(data);
    } catch (err) {
      setGlSteps(prev => prev.map(s => s.status === "active" ? { ...s, status: "error" } : s));
      setGlError(err.message);
      if (onDeployStatusChange) onDeployStatusChange("error");
    } finally {
      setGlDeploying(false);
    }
  };

  const canGitLabDeploy = glStatus?.connected && glUsername && glPassword && glUrl && glProject;
  const selectedAfConn = airflowConns.find(c => String(c.id) === selectedAirflowConn);
  const hasDagFolderOption = airflowConns.length > 0;

  return (
    <div className="space-y-4">

      <div className={cn(
        "rounded-xl border overflow-hidden transition-colors",
        dagEnabled
          ? "border-slate-200 dark:border-slate-700"
          : "border-slate-100 dark:border-slate-800"
      )}>
        <div className="flex items-center gap-2 px-4 py-2.5 bg-slate-50 dark:bg-slate-800/50 border-b border-slate-100 dark:border-slate-800">
          <FolderUp className="w-4 h-4 text-[#0060AF]" />
          <span className={cn(
            "text-sm font-semibold transition-colors",
            dagEnabled ? "text-slate-800 dark:text-slate-200" : "text-slate-400 dark:text-slate-500"
          )}>Deploy to DAG Folder</span>
          <div className="ml-auto flex items-center gap-2">
            <span className="text-[9px] font-semibold bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 px-1.5 py-0.5 rounded">DEV</span>
            {!hasDagFolderOption && (
              <span className="text-[10px] text-slate-400 dark:text-slate-500">No instances</span>
            )}
            <Switch
              id="dag-toggle"
              checked={dagEnabled}
              disabled={!hasDagFolderOption}
              onCheckedChange={(v) => { setDagEnabled(v); if (!v) { setDagResult(null); setDagError(null); setDagSteps([{ label: "Save", status: "pending" }, { label: "Generate", status: "pending" }, { label: "Deploy", status: "pending" }, { label: "Done", status: "pending" }]); } }}
              aria-label="Enable DAG folder deploy"
              className="data-[state=checked]:bg-[#0060AF]"
            />
          </div>
        </div>

        {dagEnabled && hasDagFolderOption && (
          <div className="p-4 space-y-3">
            <div className="rounded-lg border border-slate-100 dark:border-slate-700/50 p-2.5">
              <p className="text-[10px] font-semibold text-slate-400 dark:text-slate-500 uppercase tracking-wider mb-1">Artifacts</p>
              <ArtifactRow icon={FileCode2} filename={dagFilename} content={airflowDagYaml} />
              <ArtifactRow icon={FileText} filename={specFilename} content={specContent} />
              <p className="text-[10px] text-slate-400 dark:text-slate-500 mt-1 italic">Runtime scripts and mappings generated on deploy</p>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label className="text-[11px] text-slate-500 dark:text-slate-400">Airflow Instance</Label>
                <Select value={selectedAirflowConn} onValueChange={setSelectedAirflowConn}>
                  <SelectTrigger className="h-8 text-xs mt-1"><SelectValue placeholder="Select instance" /></SelectTrigger>
                  <SelectContent>
                    {airflowConns.map(c => (
                      <SelectItem key={c.id} value={String(c.id)}>
                        <span className="flex items-center gap-1.5"><Wind className="w-3 h-3" />{c.name}</span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label className="text-[11px] text-slate-500 dark:text-slate-400">Target Path</Label>
                <div className="mt-1 h-8 flex items-center px-2.5 rounded-md border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50">
                  <span className="text-[10px] font-mono text-slate-500 dark:text-slate-400 truncate">
                    {selectedAfConn ? `${selectedAfConn.dags_folder}/dataflow/${dagFilename}` : "Select an instance"}
                  </span>
                </div>
              </div>
            </div>

            <StepIndicator steps={dagSteps} />

            {dagResult && (
              <div className="rounded-lg bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-800 px-3 py-2">
                <div className="flex items-center gap-1.5">
                  <CheckCircle2 className="w-3.5 h-3.5 text-emerald-600" />
                  <span className="text-xs font-semibold text-emerald-800 dark:text-emerald-300">Deployed {dagResult.file_count} file{dagResult.file_count !== 1 ? 's' : ''} successfully</span>
                </div>
                {dagResult.deployed_files?.map((f, i) => (
                  <p key={i} className="text-[10px] text-emerald-600 dark:text-emerald-400 font-mono mt-0.5 truncate">{f.path || f.filename}</p>
                ))}
              </div>
            )}

            {dagError && (
              <div className="rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 px-3 py-2 flex items-start gap-1.5">
                <AlertCircle className="w-3.5 h-3.5 text-red-500 mt-0.5 shrink-0" />
                <p className="text-xs text-red-600 dark:text-red-400">{dagError}</p>
              </div>
            )}

            <Button
              type="button"
              disabled={dagDeploying || !selectedAirflowConn || !!dagResult}
              onClick={(e) => { e.preventDefault(); e.stopPropagation(); handleDagFolderDeploy(); }}
              className="w-full gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] text-white h-9 text-xs"
            >
              {dagDeploying ? <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Deploying...</> : dagResult ? <><CheckCircle2 className="w-3.5 h-3.5" /> Deployed</> : <><FolderUp className="w-3.5 h-3.5" /> Deploy to DAG Folder</>}
            </Button>
          </div>
        )}
      </div>

      <div className={cn(
        "rounded-xl border overflow-hidden transition-colors",
        glEnabled
          ? "border-slate-200 dark:border-slate-700"
          : "border-slate-100 dark:border-slate-800"
      )}>
        <div className="flex items-center gap-2.5 px-4 py-2.5 bg-slate-50 dark:bg-slate-800/50 border-b border-slate-100 dark:border-slate-800">
          <GitLabIcon className="w-4 h-4 text-orange-600" />
          <span className={cn(
            "text-sm font-semibold transition-colors",
            glEnabled ? "text-slate-800 dark:text-slate-200" : "text-slate-400 dark:text-slate-500"
          )}>GitLab Commit</span>

          <div className="ml-auto flex items-center gap-2">
            {glStatus?.connected && glEnabled && (
              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-full text-[9px] font-semibold bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                {glStatus.login}
              </span>
            )}
            <Switch
              id="gl-toggle"
              checked={glEnabled}
              onCheckedChange={(v) => { setGlEnabled(v); if (!v) { setGlResult(null); setGlError(null); setGlStatus(null); setGlSteps([{ label: "Save", status: "pending" }, { label: "Generate", status: "pending" }, { label: "Commit", status: "pending" }, { label: "Done", status: "pending" }]); } }}
              aria-label="Enable GitLab commit"
              className="data-[state=checked]:bg-[#0060AF]"
            />
          </div>
        </div>

        {glEnabled && (
          <div className="p-4 space-y-3">
            <div className="rounded-lg border border-slate-100 dark:border-slate-700/50 p-2.5 space-y-2">
              <div className="flex items-center justify-between">
                <p className="text-[10px] font-semibold text-slate-400 dark:text-slate-500 uppercase tracking-wider">Repository</p>
                {glRepoUrl && (
                  <a href={glRepoUrl} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-[10px] text-[#0060AF] hover:underline">
                    <ExternalLink className="w-3 h-3" /> Open
                  </a>
                )}
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label className="text-[10px] text-slate-400 dark:text-slate-500">GitLab URL</Label>
                  <Input
                    value={glUrl}
                    onChange={e => setGlUrl(e.target.value)}
                    placeholder="https://gitlab.example.com"
                    className="h-7 text-xs font-mono mt-0.5"
                  />
                </div>
                <div>
                  <Label className="text-[10px] text-slate-400 dark:text-slate-500">Project</Label>
                  <Input
                    value={glProject}
                    onChange={e => setGlProject(e.target.value)}
                    placeholder="group/repo-name"
                    className="h-7 text-xs font-mono mt-0.5"
                  />
                </div>
              </div>
              <div>
                <span className="text-[10px] text-slate-400 dark:text-slate-500">Target Path</span>
                <p className="text-xs font-mono text-[#0060AF] font-semibold truncate">{repoPath}</p>
              </div>
            </div>

            <div className="rounded-lg border border-slate-100 dark:border-slate-700/50 p-2.5">
              <p className="text-[10px] font-semibold text-slate-400 dark:text-slate-500 uppercase tracking-wider mb-1">Artifacts</p>
              <ArtifactRow icon={FileCode2} filename={dagFilename} content={airflowDagYaml} />
              <ArtifactRow icon={FileText} filename={specFilename} content={specContent} />
            </div>

            <div className="space-y-2">
              <div className="flex items-center gap-1">
                <Lock className="w-3 h-3 text-slate-400" />
                <span className="text-[11px] font-semibold text-slate-500 dark:text-slate-400">LDAP Credentials</span>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <Input
                  type="text"
                  value={glUsername}
                  onChange={e => { setGlUsername(e.target.value); setGlStatus(null); }}
                  className="h-7 text-xs"
                  placeholder="Username"
                  autoComplete="username"
                />
                <div className="relative">
                  <Input
                    type={showPassword ? "text" : "password"}
                    value={glPassword}
                    onChange={e => { setGlPassword(e.target.value); setGlStatus(null); }}
                    className="h-7 text-xs pr-7"
                    placeholder="Password"
                    autoComplete="current-password"
                  />
                  <button type="button" onClick={() => setShowPassword(v => !v)} className="absolute right-1.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600" tabIndex={-1}>
                    {showPassword ? <EyeOff className="w-3 h-3" /> : <Eye className="w-3 h-3" />}
                  </button>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Button
                  type="button"
                  size="sm"
                  disabled={!glUsername || !glPassword || glAuthenticating}
                  onClick={handleGitLabAuth}
                  className="h-6 px-2.5 text-[10px] bg-[#0060AF] hover:bg-[#004d8c] text-white"
                >
                  {glAuthenticating ? <><Loader2 className="w-3 h-3 animate-spin mr-1" /> Verifying</> : "Authenticate"}
                </Button>
                {glStatus && !glStatus.connected && (
                  <span className="text-[10px] text-red-500 truncate">{glStatus.error || "Auth failed"}</span>
                )}
              </div>
            </div>

            <div className="grid grid-cols-3 gap-2">
              <div>
                <Label className="text-[11px] text-slate-500 dark:text-slate-400">Branch</Label>
                <Input value={commitBranch} onChange={e => setCommitBranch(e.target.value)} className="h-7 font-mono text-xs mt-0.5" placeholder="main" />
              </div>
              <div className="col-span-2">
                <Label className="text-[11px] text-slate-500 dark:text-slate-400">Commit Message</Label>
                <Input value={commitMsg} onChange={e => setCommitMsg(e.target.value)} className="h-7 text-xs mt-0.5" placeholder={defaultCommitMsg} />
              </div>
            </div>

            <StepIndicator steps={glSteps} />

            {glResult && (
              <div className="rounded-lg bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-800 px-3 py-2 space-y-1">
                <div className="flex items-center gap-1.5">
                  <CheckCircle2 className="w-3.5 h-3.5 text-emerald-600" />
                  <span className="text-xs font-semibold text-emerald-800 dark:text-emerald-300">Committed {glResult.file_count} file{glResult.file_count !== 1 ? 's' : ''}</span>
                </div>
                <div className="flex items-center gap-3 text-[10px] text-emerald-600 dark:text-emerald-400 font-mono">
                  <span><GitBranch className="w-3 h-3 inline mr-0.5" />{glResult.branch}</span>
                  <span>{(glResult.short_sha || glResult.sha || "").substring(0, 8)}</span>
                </div>
                {glResult.deployed_files?.map((f, i) => (
                  <p key={i} className="text-[10px] text-emerald-600 dark:text-emerald-400 font-mono truncate">{f.filename}</p>
                ))}
                {glResult.url && (
                  <a href={glResult.url} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-[10px] font-medium text-[#0060AF] hover:underline">
                    <ExternalLink className="w-3 h-3" /> View on GitLab
                  </a>
                )}
              </div>
            )}

            {glError && (
              <div className="rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 px-3 py-2 flex items-start gap-1.5">
                <AlertCircle className="w-3.5 h-3.5 text-red-500 mt-0.5 shrink-0" />
                <p className="text-xs text-red-600 dark:text-red-400">{glError}</p>
              </div>
            )}

            <Button
              type="button"
              disabled={glDeploying || !canGitLabDeploy || !!glResult}
              onClick={(e) => { e.preventDefault(); e.stopPropagation(); handleGitLabDeploy(); }}
              className="w-full gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] text-white h-9 text-xs"
            >
              {glDeploying ? <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Committing...</> : glResult ? <><CheckCircle2 className="w-3.5 h-3.5" /> Committed</> : <><Rocket className="w-3.5 h-3.5" /> Deploy to GitLab</>}
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
