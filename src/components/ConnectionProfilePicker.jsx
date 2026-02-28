import { useState, useEffect } from "react";
import { dataflow } from '@/api/client';
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import PlatformIcon, { platformConfig } from "@/components/PlatformIcon";
import { BookOpen, Plus, Trash2, Search, Check } from "lucide-react";
import { toast } from "sonner";

export default function ConnectionProfilePicker({ onApply }) {
  const [open, setOpen] = useState(false);
  const [profiles, setProfiles] = useState([]);
  const [search, setSearch] = useState("");
  const [creating, setCreating] = useState(false);
  const [newProfile, setNewProfile] = useState({ name: "", description: "", platform: "" });
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (open) loadProfiles();
  }, [open]);

  const loadProfiles = async () => {
    const data = await dataflow.entities.ConnectionProfile.list();
    setProfiles(data);
  };

  const handleApply = (profile) => {
    const { id, created_date, updated_date, created_by, name: profileName, description, ...fields } = profile;
    onApply(fields);
    setOpen(false);
    toast.success(`Profile "${profileName}" applied`);
  };

  const handleDelete = async (profile, e) => {
    e.stopPropagation();
    if (!confirm(`Delete profile "${profile.name}"?`)) return;
    await dataflow.entities.ConnectionProfile.delete(profile.id);
    loadProfiles();
    toast.success("Profile deleted");
  };

  const handleSaveProfile = async (e) => {
    e.preventDefault();
    if (!newProfile.name || !newProfile.platform) return;
    setSaving(true);
    await dataflow.entities.ConnectionProfile.create(newProfile);
    toast.success(`Profile "${newProfile.name}" saved`);
    setNewProfile({ name: "", description: "", platform: "" });
    setCreating(false);
    setSaving(false);
    loadProfiles();
  };

  const filtered = profiles.filter(p =>
    p.name.toLowerCase().includes(search.toLowerCase()) ||
    p.platform?.toLowerCase().includes(search.toLowerCase()) ||
    p.description?.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <>
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="gap-1.5 text-xs"
        onClick={() => setOpen(true)}
      >
        <BookOpen className="w-3.5 h-3.5" />
        Load Profile
      </Button>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-lg max-h-[85vh] flex flex-col">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <BookOpen className="w-4 h-4 text-blue-600" />
              Connection Profiles
            </DialogTitle>
          </DialogHeader>

          <div className="flex flex-col gap-4 overflow-hidden flex-1">
            {/* Search + New */}
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400" />
                <Input
                  placeholder="Search profiles..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  className="pl-9 h-8 text-sm"
                />
              </div>
              <Button
                type="button"
                size="sm"
                variant="outline"
                className="gap-1.5 text-xs shrink-0"
                onClick={() => setCreating(!creating)}
              >
                <Plus className="w-3.5 h-3.5" />
                New Profile
              </Button>
            </div>

            {/* Create Form */}
            {creating && (
              <form onSubmit={handleSaveProfile} className="border rounded-lg p-4 space-y-3 bg-slate-50">
                <p className="text-xs font-semibold text-slate-700 uppercase tracking-wide">Save as Profile</p>
                <div className="grid grid-cols-2 gap-3">
                  <div className="col-span-2">
                    <Label className="text-xs">Profile Name *</Label>
                    <Input
                      value={newProfile.name}
                      onChange={e => setNewProfile({ ...newProfile, name: e.target.value })}
                      placeholder="e.g. Prod SQL Server"
                      className="h-8 text-sm"
                      required
                    />
                  </div>
                  <div>
                    <Label className="text-xs">Platform *</Label>
                    <Select value={newProfile.platform} onValueChange={v => setNewProfile({ ...newProfile, platform: v })}>
                      <SelectTrigger className="h-8 text-xs"><SelectValue placeholder="Select" /></SelectTrigger>
                      <SelectContent>
                        {Object.entries(platformConfig).map(([key, cfg]) => (
                          <SelectItem key={key} value={key} className="text-xs">{cfg.label}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="col-span-2">
                    <Label className="text-xs">Description</Label>
                    <Textarea
                      value={newProfile.description}
                      onChange={e => setNewProfile({ ...newProfile, description: e.target.value })}
                      placeholder="Optional description..."
                      rows={2}
                      className="text-sm resize-none"
                    />
                  </div>
                </div>
                <div className="flex justify-end gap-2">
                  <Button type="button" size="sm" variant="ghost" onClick={() => setCreating(false)} className="text-xs">Cancel</Button>
                  <Button type="submit" size="sm" disabled={saving} className="text-xs gap-1.5">
                    <Check className="w-3.5 h-3.5" />
                    {saving ? "Saving..." : "Save Profile"}
                  </Button>
                </div>
              </form>
            )}

            {/* Profile List */}
            <div className="overflow-y-auto flex-1 space-y-2 pr-1">
              {filtered.length === 0 ? (
                <div className="text-center py-10 text-slate-400 text-sm">
                  {profiles.length === 0 ? "No profiles yet. Create one to get started." : "No profiles match your search."}
                </div>
              ) : (
                filtered.map(profile => (
                  <button
                    key={profile.id}
                    type="button"
                    onClick={() => handleApply(profile)}
                    className="w-full text-left flex items-center gap-3 p-3 rounded-lg border border-slate-200 hover:border-blue-400 hover:bg-blue-50 transition-all group"
                  >
                    <PlatformIcon platform={profile.platform} size="sm" />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-semibold text-slate-800 truncate">{profile.name}</p>
                      <p className="text-xs text-slate-500 truncate">
                        {platformConfig[profile.platform]?.label || profile.platform}
                        {profile.description && ` · ${profile.description}`}
                      </p>
                    </div>
                    <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <span className="text-xs text-blue-600 font-medium">Apply</span>
                      <button
                        type="button"
                        onClick={(e) => handleDelete(profile, e)}
                        className="ml-1 p-1 rounded hover:bg-red-100 text-slate-400 hover:text-red-600 transition-colors"
                        title="Delete profile"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </div>
                  </button>
                ))
              )}
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}