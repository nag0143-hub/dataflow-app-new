import { useState, useEffect } from "react";
import { dataflow } from '@/api/client';
import { useQuery } from "@tanstack/react-query";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Search,
  Database,
  Tag,
  Calendar,
  BarChart3,
  Shield,
  Clock,
  Plus,
  FileText,
  TrendingUp,
  Eye
} from "lucide-react";
import { cn } from "@/lib/utils";
import PlatformIcon from "@/components/PlatformIcon";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import SkeletonLoader from "@/components/SkeletonLoader";
import { useTenant } from "@/components/useTenant";

export default function DataCatalog() {
  const { user, loading: userLoading, scope } = useTenant();
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedTag, setSelectedTag] = useState(null);
  const [selectedClassification, setSelectedClassification] = useState(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [viewEntry, setViewEntry] = useState(null);
  const [formData, setFormData] = useState({});

  const { data: catalogEntries = [], isLoading, refetch } = useQuery({
    queryKey: ['catalog-entries'],
    queryFn: () => dataflow.entities.DataCatalogEntry.list('-created_date', 500)
  });

  const { data: connections = [] } = useQuery({
    queryKey: ['connections'],
    queryFn: () => dataflow.entities.Connection.list()
  });

  const { data: jobs = [] } = useQuery({
    queryKey: ['jobs'],
    queryFn: () => dataflow.entities.Pipeline.list()
  });

  // Extract all unique tags
  const allTags = [...new Set(catalogEntries.flatMap(e => e.tags || []))];

  // Filter entries
  const filteredEntries = catalogEntries.filter(entry => {
    const matchesSearch = !searchQuery || 
      entry.dataset_name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      entry.description?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      entry.schema_name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      entry.table_name?.toLowerCase().includes(searchQuery.toLowerCase());
    
    const matchesTag = !selectedTag || entry.tags?.includes(selectedTag);
    const matchesClassification = !selectedClassification || entry.data_classification === selectedClassification;
    
    return matchesSearch && matchesTag && matchesClassification;
  });

  const classificationColors = {
    public: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
    internal: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300",
    confidential: "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300",
    restricted: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300",
    pii: "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300"
  };

  const formatBytes = (bytes) => {
    if (!bytes) return "N/A";
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round((bytes / Math.pow(1024, i)) * 100) / 100 + ' ' + sizes[i];
  };

  const formatNumber = (num) => {
    if (!num && num !== 0) return "N/A";
    return num.toLocaleString();
  };

  const handleCreate = async () => {
    await dataflow.entities.DataCatalogEntry.create(formData);
    refetch();
    setDialogOpen(false);
    setFormData({});
  };

  if (isLoading || userLoading) {
    return (
      <div className="space-y-6 max-w-7xl mx-auto">
        <div className="h-8 bg-slate-200 dark:bg-slate-700 rounded w-64 animate-pulse" />
        <SkeletonLoader count={6} height="h-40" />
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground tracking-tight">Data Catalog</h1>
          <p className="text-muted-foreground mt-0.5">
            Searchable metadata repository of all datasets
          </p>
        </div>
        <Button onClick={() => { setFormData({}); setDialogOpen(true); }} className="gap-2">
          <Plus className="w-4 h-4" />
          New Entry
        </Button>
      </div>

      {/* Search & Filters */}
      <div className="flex flex-wrap gap-3">
        <div className="flex-1 min-w-[300px]">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <Input
              placeholder="Search datasets, schemas, descriptions..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </div>

        {allTags.length > 0 && (
          <Select value={selectedTag || "all"} onValueChange={(v) => setSelectedTag(v === "all" ? null : v)}>
            <SelectTrigger className="w-40">
              <SelectValue placeholder="All Tags" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Tags</SelectItem>
              {allTags.map(tag => (
                <SelectItem key={tag} value={tag}>{tag}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}

        <Select value={selectedClassification || "all"} onValueChange={(v) => setSelectedClassification(v === "all" ? null : v)}>
          <SelectTrigger className="w-48">
            <SelectValue placeholder="All Classifications" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Classifications</SelectItem>
            <SelectItem value="public">Public</SelectItem>
            <SelectItem value="internal">Internal</SelectItem>
            <SelectItem value="confidential">Confidential</SelectItem>
            <SelectItem value="restricted">Restricted</SelectItem>
            <SelectItem value="pii">PII</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <Card className="dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <Database className="w-8 h-8 text-blue-600" />
              <div>
                <p className="text-2xl font-bold dark:text-white">{catalogEntries.length}</p>
                <p className="text-xs text-slate-500 dark:text-slate-400">Total Datasets</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card className="dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <Tag className="w-8 h-8 text-emerald-600" />
              <div>
                <p className="text-2xl font-bold dark:text-white">{allTags.length}</p>
                <p className="text-xs text-slate-500 dark:text-slate-400">Unique Tags</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card className="dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <Shield className="w-8 h-8 text-purple-600" />
              <div>
                <p className="text-2xl font-bold dark:text-white">
                  {catalogEntries.filter(e => e.data_classification === 'pii' || e.data_classification === 'restricted').length}
                </p>
                <p className="text-xs text-slate-500 dark:text-slate-400">Sensitive Datasets</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card className="dark:bg-slate-800 dark:border-slate-700">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <TrendingUp className="w-8 h-8 text-amber-600" />
              <div>
                <p className="text-2xl font-bold dark:text-white">
                  {catalogEntries.filter(e => e.is_active).length}
                </p>
                <p className="text-xs text-slate-500 dark:text-slate-400">Active Datasets</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Catalog Entries */}
      <div className="grid gap-4">
        {filteredEntries.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center">
              <div className="w-16 h-16 rounded-2xl bg-blue-50 dark:bg-blue-900/20 flex items-center justify-center mx-auto mb-5">
                <Database className="w-8 h-8 text-[#0060AF] dark:text-blue-400" />
              </div>
              <h3 className="text-lg font-semibold text-foreground mb-1">
                {searchQuery || selectedTag || selectedClassification ? "No datasets match your filters" : "No catalog entries yet"}
              </h3>
              <p className="text-muted-foreground">
                {searchQuery || selectedTag || selectedClassification
                  ? "Try adjusting your search or filters"
                  : "Create your first entry to get started."}
              </p>
            </CardContent>
          </Card>
        ) : (
          filteredEntries.map(entry => (
            <Card key={entry.id} className="dark:bg-slate-800 dark:border-slate-700 hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                      <PlatformIcon platform={entry.platform} size={20} />
                      <CardTitle className="dark:text-white">{entry.dataset_name}</CardTitle>
                      {!entry.is_active && (
                        <Badge variant="outline" className="text-slate-500">Inactive</Badge>
                      )}
                    </div>
                    {entry.description && (
                      <p className="text-sm text-slate-600 dark:text-slate-400 mt-1">{entry.description}</p>
                    )}
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setViewEntry(entry)}
                    className="gap-2"
                  >
                    <Eye className="w-4 h-4" />
                    View
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                  <div>
                    <p className="text-xs text-slate-500 dark:text-slate-400 mb-1">Schema.Table</p>
                    <p className="text-sm font-medium dark:text-white">
                      {entry.schema_name ? `${entry.schema_name}.${entry.table_name}` : entry.table_name || 'N/A'}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-500 dark:text-slate-400 mb-1">Row Count</p>
                    <p className="text-sm font-medium dark:text-white">{formatNumber(entry.row_count)}</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-500 dark:text-slate-400 mb-1">Size</p>
                    <p className="text-sm font-medium dark:text-white">{formatBytes(entry.size_bytes)}</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-500 dark:text-slate-400 mb-1">Update Frequency</p>
                    <p className="text-sm font-medium dark:text-white capitalize">{entry.update_frequency || 'N/A'}</p>
                  </div>
                </div>

                <div className="flex flex-wrap gap-2">
                  {entry.data_classification && (
                    <Badge className={classificationColors[entry.data_classification]}>
                      <Shield className="w-3 h-3 mr-1" />
                      {entry.data_classification}
                    </Badge>
                  )}
                  {entry.tags?.map(tag => (
                    <Badge key={tag} variant="outline" className="dark:border-slate-600">
                      <Tag className="w-3 h-3 mr-1" />
                      {tag}
                    </Badge>
                  ))}
                  {entry.owner && (
                    <Badge variant="secondary" className="dark:bg-slate-700">
                      Owner: {entry.owner}
                    </Badge>
                  )}
                  {entry.quality_score !== null && entry.quality_score !== undefined && (
                    <Badge variant="outline" className={cn(
                      entry.quality_score >= 80 ? "border-green-500 text-green-700" :
                      entry.quality_score >= 60 ? "border-yellow-500 text-yellow-700" :
                      "border-red-500 text-red-700"
                    )}>
                      <BarChart3 className="w-3 h-3 mr-1" />
                      Quality: {entry.quality_score}%
                    </Badge>
                  )}
                </div>
              </CardContent>
            </Card>
          ))
        )}
      </div>

      {/* Create/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>New Catalog Entry</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <Label>Dataset Name *</Label>
              <Input
                value={formData.dataset_name || ""}
                onChange={(e) => setFormData({ ...formData, dataset_name: e.target.value })}
                placeholder="Customer Data"
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label>Schema Name</Label>
                <Input
                  value={formData.schema_name || ""}
                  onChange={(e) => setFormData({ ...formData, schema_name: e.target.value })}
                  placeholder="dbo"
                />
              </div>
              <div>
                <Label>Table Name</Label>
                <Input
                  value={formData.table_name || ""}
                  onChange={(e) => setFormData({ ...formData, table_name: e.target.value })}
                  placeholder="customers"
                />
              </div>
            </div>
            <div>
              <Label>Connection *</Label>
              <Select
                value={formData.connection_id || ""}
                onValueChange={(v) => {
                  const conn = connections.find(c => c.id === v);
                  setFormData({
                    ...formData,
                    connection_id: v,
                    connection_name: conn?.name,
                    platform: conn?.platform
                  });
                }}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select connection" />
                </SelectTrigger>
                <SelectContent>
                  {connections.map(c => (
                    <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label>Description</Label>
              <Textarea
                value={formData.description || ""}
                onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                placeholder="Describe this dataset..."
                rows={3}
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label>Owner</Label>
                <Input
                  value={formData.owner || ""}
                  onChange={(e) => setFormData({ ...formData, owner: e.target.value })}
                  placeholder="Data Team"
                />
              </div>
              <div>
                <Label>Classification</Label>
                <Select
                  value={formData.data_classification || ""}
                  onValueChange={(v) => setFormData({ ...formData, data_classification: v })}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="public">Public</SelectItem>
                    <SelectItem value="internal">Internal</SelectItem>
                    <SelectItem value="confidential">Confidential</SelectItem>
                    <SelectItem value="restricted">Restricted</SelectItem>
                    <SelectItem value="pii">PII</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div>
              <Label>Tags (comma-separated)</Label>
              <Input
                value={formData.tags?.join(", ") || ""}
                onChange={(e) => setFormData({
                  ...formData,
                  tags: e.target.value.split(",").map(t => t.trim()).filter(Boolean)
                })}
                placeholder="finance, customer-data, production"
              />
            </div>
            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={() => setDialogOpen(false)}>Cancel</Button>
              <Button onClick={handleCreate}>Create</Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* View Entry Dialog */}
      {viewEntry && (
        <Dialog open={!!viewEntry} onOpenChange={() => setViewEntry(null)}>
          <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-3">
                <PlatformIcon platform={viewEntry.platform} size={24} />
                {viewEntry.dataset_name}
              </DialogTitle>
            </DialogHeader>
            <div className="space-y-4">
              {viewEntry.description && (
                <div>
                  <Label>Description</Label>
                  <p className="text-sm text-slate-600 dark:text-slate-400 mt-1">{viewEntry.description}</p>
                </div>
              )}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label>Schema.Table</Label>
                  <p className="text-sm mt-1 dark:text-white">
                    {viewEntry.schema_name ? `${viewEntry.schema_name}.${viewEntry.table_name}` : viewEntry.table_name}
                  </p>
                </div>
                <div>
                  <Label>Connection</Label>
                  <p className="text-sm mt-1 dark:text-white">{viewEntry.connection_name}</p>
                </div>
                <div>
                  <Label>Row Count</Label>
                  <p className="text-sm mt-1 dark:text-white">{formatNumber(viewEntry.row_count)}</p>
                </div>
                <div>
                  <Label>Size</Label>
                  <p className="text-sm mt-1 dark:text-white">{formatBytes(viewEntry.size_bytes)}</p>
                </div>
              </div>
              {viewEntry.schema_definition?.columns && (
                <div>
                  <Label>Schema</Label>
                  <div className="mt-2 border dark:border-slate-700 rounded-lg overflow-hidden">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-50 dark:bg-slate-800">
                        <tr>
                          <th className="px-3 py-2 text-left">Column</th>
                          <th className="px-3 py-2 text-left">Type</th>
                          <th className="px-3 py-2 text-left">Nullable</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y dark:divide-slate-700">
                        {viewEntry.schema_definition.columns.map((col, idx) => (
                          <tr key={idx}>
                            <td className="px-3 py-2 dark:text-white">{col.name}</td>
                            <td className="px-3 py-2 dark:text-slate-400">{col.type}</td>
                            <td className="px-3 py-2 dark:text-slate-400">{col.nullable ? 'Yes' : 'No'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          </DialogContent>
        </Dialog>
      )}
    </div>
  );
}