import { useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent } from "@/components/ui/tabs";
import JobBasicsTab from "@/components/JobFormTabs/JobBasicsTab.jsx";
import JobDataTab from "@/components/JobFormTabs/JobDataTab.jsx";
import PipelineStepIndicator from "@/components/PipelineStepIndicator";
import AdvancedTabContent from "@/components/JobFormTabs/AdvancedTabContent";
import JobSpecTabPreview from "@/components/JobSpecTabPreview";
import ScheduleSettings from "@/components/JobFormTabs/ScheduleSettings";
import { FLAT_FILE_PLATFORMS } from "@/components/JobSpecExport";
import DeployTabContent from "@/components/DeployTabContent";
import { dataflow } from '@/api/client';
import { toast } from "sonner";
import { Maximize2, Minimize2, CheckCircle2, ChevronLeft, ChevronRight, Save, X } from "lucide-react";

export default function JobFormDialog({
  open,
  onOpenChange,
  editingJob,
  formData,
  setFormData,
  connections,
  onSaveSuccess,
  currentUser,
  pipelines = []
}) {
  const [activeTab, setActiveTab] = useState("general");
  const [saving, setSaving] = useState(false);
  const [touched, setTouched] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [deployStatus, setDeployStatus] = useState(null);

  const sourceConnections = connections;
  const targetConnections = connections;
  const sourceConn = connections.find(c => c.id === formData.source_connection_id);
  const isFileWildcard = sourceConn && FLAT_FILE_PLATFORMS.includes(sourceConn.platform) && formData.file_source_mode === "wildcard";
  const canShowAdvanced = formData.enable_advanced && !isFileWildcard;

  const allTabs = ["general", "datasets", "settings"];
  if (canShowAdvanced) allTabs.push("advanced");
  allTabs.push("review", "deploy");

  const currentTabIndex = allTabs.indexOf(activeTab);

  const handleNext = () => {
    setTouched(true);
    if (activeTab === "general") {
      const e = getErrors();
      if (e.name || e.source_connection_id || e.target_connection_id) return;
    }
    if (activeTab === "datasets") {
      const e = getErrors();
      if (e.datasets) { toast.error(e.datasets); return; }
    }
    if (currentTabIndex < allTabs.length - 1) {
      setActiveTab(allTabs[currentTabIndex + 1]);
    }
  };

  const getErrors = () => {
    const e = {};
    if (!formData.name?.trim()) e.name = "Pipeline name is required.";
    else if (pipelines.some(p => p.name?.toLowerCase() === formData.name.trim().toLowerCase() && p.id !== editingJob?.id)) {
      e.name = "A pipeline with this name already exists.";
    }
    if (!formData.source_connection_id) e.source_connection_id = "Please select a source connection.";
    if (!formData.target_connection_id) e.target_connection_id = "Please select a target connection.";
    if (formData.source_connection_id && formData.source_connection_id === formData.target_connection_id) {
      e.source_connection_id = "Source and target must be different.";
      e.target_connection_id = "Source and target must be different.";
    }
    if (sourceConn && FLAT_FILE_PLATFORMS.includes(sourceConn?.platform)) {
      const mode = formData.file_source_mode || "file_list";
      if (mode === "file_list" && (!formData.file_source_list || formData.file_source_list.length === 0)) {
        e.datasets = "At least one file must be added.";
      } else if (mode === "folder" && !formData.file_source_folder?.trim()) {
        e.datasets = "Folder path is required.";
      } else if (mode === "wildcard" && !formData.file_source_wildcard?.trim()) {
        e.datasets = "Wildcard pattern is required.";
      }
    } else {
      if (!formData.selected_datasets || formData.selected_datasets.length === 0) e.datasets = "At least one dataset must be selected.";
    }
    if (formData.schedule_type === "custom" && !formData.cron_expression?.trim()) e.cron_expression = "Cron expression is required for custom schedule.";
    return e;
  };

  const fieldErrors = touched ? getErrors() : {};
  const isValid = Object.keys(getErrors()).length === 0;

  const validateJob = (silent = false) => {
    const errors = getErrors();
    if (Object.keys(errors).length > 0) {
      if (!silent) toast.error(Object.values(errors)[0]);
      return false;
    }
    return true;
  };

  const handleSaveDraft = async () => {
    setSaving(true);
    const payload = {
      ...formData,
      _isDraft: true,
      total_runs: editingJob?.total_runs || 0,
      successful_runs: editingJob?.successful_runs || 0,
      failed_runs: editingJob?.failed_runs || 0,
    };

    try {
      if (editingJob) {
        await dataflow.entities.Pipeline.update(editingJob.id, payload);
        toast.success("Pipeline draft saved");
        onSaveSuccess?.();
      } else {
        await dataflow.entities.Pipeline.create(payload);
        toast.success("Pipeline draft saved");
        onSaveSuccess?.();
      }
    } catch (err) {
      toast.error(err.message || "Failed to save pipeline draft");
    } finally {
      setSaving(false);
    }
  };

  const handleSaveAndClose = async () => {
    setTouched(true);
    if (!validateJob()) return;
    setSaving(true);
    const payload = {
      ...formData,
      _isDraft: false,
      total_runs: editingJob?.total_runs || 0,
      successful_runs: editingJob?.successful_runs || 0,
      failed_runs: editingJob?.failed_runs || 0,
    };
    try {
      if (editingJob) {
        await dataflow.entities.Pipeline.update(editingJob.id, payload);
        await dataflow.entities.ActivityLog.create({
          log_type: "info",
          category: "job",
          job_id: editingJob.id,
          message: `Pipeline "${formData.name}" updated`,
        });
        toast.success("Pipeline updated");
      } else {
        const created = await dataflow.entities.Pipeline.create(payload);
        await dataflow.entities.ActivityLog.create({
          log_type: "success",
          category: "job",
          job_id: created.id,
          message: `Pipeline "${formData.name}" created`,
        });
        toast.success("Pipeline created");
      }
      onOpenChange(false);
      onSaveSuccess?.();
    } catch (err) {
      toast.error(err.message || "Failed to save pipeline");
    } finally {
      setSaving(false);
    }
  };

  const savedPipelineIdRef = { current: editingJob?.id || null };

  const handleSavePipeline = async () => {
    const payload = {
      ...formData,
      _isDraft: false,
      total_runs: editingJob?.total_runs || 0,
      successful_runs: editingJob?.successful_runs || 0,
      failed_runs: editingJob?.failed_runs || 0,
    };
    if (editingJob || savedPipelineIdRef.current) {
      const id = savedPipelineIdRef.current || editingJob.id;
      await dataflow.entities.Pipeline.update(id, payload);
      savedPipelineIdRef.current = id;
    } else {
      const created = await dataflow.entities.Pipeline.create(payload);
      savedPipelineIdRef.current = created.id;
    }
  };

  const handleDeploySuccess = async (result) => {
    setDeployStatus("success");
    setFormData(prev => ({ ...prev, status: "active" }));
    try {
      const pipelineId = savedPipelineIdRef.current;
      if (pipelineId) {
        await dataflow.entities.Pipeline.update(pipelineId, { ...formData, status: "active" });
      }
      const isGitLab = result.short_sha || result.sha;
      const msg = isGitLab
        ? `Pipeline "${formData.name}" deployed to git (SHA: ${(result.short_sha || result.sha || "").substring(0, 8)})`
        : `Pipeline "${formData.name}" deployed to DAG folder`;
      await dataflow.entities.ActivityLog.create({
        log_type: "success",
        category: "job",
        job_id: pipelineId,
        message: msg,
      });
      toast.success(isGitLab ? "Pipeline deployed to GitLab — status set to Active" : "Pipeline deployed to DAG folder — status set to Active");
      onSaveSuccess?.();
    } catch (err) {
      toast.error("Deploy succeeded but failed to log activity: " + err.message);
    }
  };

  const stepLabels = [
    { key: "general", label: "Basics" },
    { key: "datasets", label: "Datasets" },
    { key: "settings", label: "Schedule" },
    ...(canShowAdvanced ? [{ key: "advanced", label: "Advanced" }] : []),
    { key: "review", label: "Review" },
    { key: "deploy", label: "Deploy" },
  ];

  const getCompletedSteps = () => {
    const completed = [];
    if (formData.name?.trim() && formData.source_connection_id && formData.target_connection_id) completed.push("general");
    if (sourceConn && FLAT_FILE_PLATFORMS.includes(sourceConn?.platform)) {
      const mode = formData.file_source_mode || "file_list";
      if ((mode === "file_list" && formData.file_source_list?.length > 0) ||
          (mode === "folder" && formData.file_source_folder?.trim()) ||
          (mode === "wildcard" && formData.file_source_wildcard?.trim())) {
        completed.push("datasets");
      }
    } else if (formData.selected_datasets?.length > 0) {
      completed.push("datasets");
    }
    if (formData.schedule_type) completed.push("settings");
    return completed;
  };

  return (
    <div>
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        preventClose={deployStatus === "deploying"}
        className={`overflow-y-auto transition-all duration-300 ${
          isExpanded
            ? 'max-w-[100vw] max-h-[100vh] w-[100vw] h-[100vh] rounded-none'
            : activeTab === "review" || activeTab === "deploy" || activeTab === "advanced"
              ? 'max-w-[98vw] max-h-[98vh] w-[98vw] h-[98vh]'
              : 'max-w-3xl max-h-[92vh] w-auto h-auto'
        }`}
      >
        <DialogHeader className="flex items-center justify-between">
          <DialogTitle>
            {editingJob ? "Edit Pipeline" : "Create Pipeline"}
          </DialogTitle>
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setIsExpanded(!isExpanded)}
            className="h-8 w-8"
            title={isExpanded ? "Collapse" : "Expand"}
          >
            {isExpanded ? <Minimize2 className="h-4 w-4" /> : <Maximize2 className="h-4 w-4" />}
          </Button>
        </DialogHeader>

        <form onSubmit={(e) => { e.preventDefault(); }}>
          <PipelineStepIndicator
            steps={stepLabels}
            activeStep={activeTab}
            onStepClick={setActiveTab}
            completedSteps={getCompletedSteps()}
          />

          <Tabs value={activeTab} onValueChange={setActiveTab}>
            <div className={`overflow-y-auto ${
              isExpanded
                ? 'max-h-[calc(100vh-220px)]'
                : activeTab === "review" || activeTab === "deploy" || activeTab === "advanced"
                  ? 'max-h-[calc(98vh-240px)]'
                  : 'max-h-[calc(92vh-320px)]'
            }`}>
              <TabsContent value="general" className="space-y-4 mt-0">
                <JobBasicsTab
                  formData={formData}
                  setFormData={setFormData}
                  sourceConnections={sourceConnections}
                  targetConnections={targetConnections}
                  errors={fieldErrors}
                />
              </TabsContent>

              <TabsContent value="datasets" className="space-y-4 mt-0">
                {fieldErrors.datasets && (
                  <div className="flex items-center gap-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-3 py-2">
                    <span className="w-3.5 h-3.5">{"⚠"}</span>{fieldErrors.datasets}
                  </div>
                )}
                <JobDataTab
                  formData={formData}
                  setFormData={setFormData}
                  connections={connections}
                />
              </TabsContent>

              <TabsContent value="settings" className="space-y-4 mt-4">
                <ScheduleSettings formData={formData} setFormData={setFormData} />
              </TabsContent>

              {canShowAdvanced && (
                <TabsContent value="advanced" className="space-y-5 mt-4">
                  <AdvancedTabContent formData={formData} setFormData={setFormData} />
                </TabsContent>
              )}

              <TabsContent value="review" className="mt-4">
                <JobSpecTabPreview formData={formData} connections={connections} setFormData={setFormData} />
              </TabsContent>

              <TabsContent value="deploy" className="mt-4">
                <DeployTabContent
                  formData={formData}
                  connections={connections}
                  editingJob={editingJob}
                  onSavePipeline={handleSavePipeline}
                  onDeploySuccess={handleDeploySuccess}
                  onDeployStatusChange={setDeployStatus}
                />
              </TabsContent>
            </div>
          </Tabs>

          <div className="mt-6 pt-4 border-t border-slate-200 dark:border-slate-700 flex items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              {currentTabIndex > 0 && activeTab !== "deploy" && (
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => setActiveTab(allTabs[currentTabIndex - 1])}
                  className="gap-1.5"
                >
                  <ChevronLeft className="w-4 h-4" />
                  Back
                </Button>
              )}
              {activeTab === "deploy" && (
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => setActiveTab("review")}
                  disabled={deployStatus === "deploying"}
                  className="gap-1.5"
                >
                  <ChevronLeft className="w-4 h-4" />
                  Back to Review
                </Button>
              )}
            </div>

            <div className="flex items-center gap-2">
              {deployStatus === "success" ? (
                <Button
                  type="button"
                  onClick={() => onOpenChange(false)}
                  className="gap-1.5 bg-emerald-600 hover:bg-emerald-700 text-white"
                >
                  <CheckCircle2 className="w-4 h-4" />
                  Done — Close
                </Button>
              ) : (
                <>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => onOpenChange(false)}
                    disabled={deployStatus === "deploying"}
                    className="text-slate-500"
                  >
                    <X className="w-4 h-4 mr-1" />
                    Cancel
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={handleSaveDraft}
                    disabled={saving || deployStatus === "deploying"}
                    className="gap-1.5"
                  >
                    <Save className="w-3.5 h-3.5" />
                    {saving ? "Saving..." : "Save Draft"}
                  </Button>
                  {activeTab === "review" ? (
                    <Button
                      type="button"
                      onClick={() => {
                        if (isValid) {
                          handleNext();
                        } else {
                          toast.error("Please complete all required fields before proceeding to deploy");
                        }
                      }}
                      disabled={!isValid}
                      className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] text-white"
                    >
                      Proceed to Deploy
                      <ChevronRight className="w-4 h-4" />
                    </Button>
                  ) : activeTab !== "deploy" ? (
                    <Button
                      type="button"
                      onClick={handleNext}
                      className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] text-white"
                    >
                      {activeTab === "settings" ? (canShowAdvanced ? "Next: Advanced" : "Next: Review") : "Next"}
                      <ChevronRight className="w-4 h-4" />
                    </Button>
                  ) : (
                    <Button
                      type="button"
                      onClick={handleSaveAndClose}
                      disabled={saving || !isValid || deployStatus === "deploying"}
                      className="gap-1.5 bg-[#0060AF] hover:bg-[#004d8c] text-white"
                    >
                      {saving ? "Saving..." : "Save & Close"}
                    </Button>
                  )}
                </>
              )}
            </div>
          </div>
        </form>
      </DialogContent>
    </Dialog>
    </div>
  );
}
