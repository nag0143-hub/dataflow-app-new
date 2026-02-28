import { useState, useEffect } from "react";
import { Workflow, Eye, EyeOff, LogIn, AlertCircle, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent } from "@/components/ui/card";
import { dataflow } from "@/api/client";
import { useAuth } from "@/lib/AuthContext";

export default function LDAPIntegration() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const { checkAppState } = useAuth();

  useEffect(() => {
    const darkPref = localStorage.getItem("dataflow-dark") === "true";
    if (darkPref) {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
  }, []);

  const handleLogin = async (e) => {
    e.preventDefault();
    if (!username.trim() || !password) {
      setError("Please enter your username and password");
      return;
    }
    setLoading(true);
    setError("");
    try {
      await dataflow.auth.login(username, password);
      await checkAppState();
    } catch (err) {
      setError(err.message || "Authentication failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <div className="w-full max-w-md space-y-6 px-4">
        <div className="text-center space-y-3">
          <div className="w-16 h-16 rounded-2xl bg-[#0060AF] flex items-center justify-center mx-auto shadow-lg shadow-[#0060AF]/20">
            <Workflow className="w-8 h-8 text-white" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-900 dark:text-white">DataFlow</h1>
            <p className="text-[#0060AF] dark:text-blue-400 text-sm font-medium">Data Connector Platform</p>
          </div>
          <p className="text-slate-500 dark:text-slate-400 text-sm leading-relaxed max-w-sm mx-auto">
            Build and manage data pipelines across databases, flat files, and cloud storage.
            Generate Airflow DAGs automatically and deploy to GitLab with one click.
          </p>
        </div>

        {import.meta.env.DEV && (
          <div className="rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 px-4 py-3 flex items-start gap-2 text-sm text-slate-600 dark:text-slate-400">
            <AlertCircle className="w-4 h-4 mt-0.5 shrink-0 text-slate-400" />
            <span>Demo credentials: <strong className="text-slate-700 dark:text-slate-300">admin / admin</strong></span>
          </div>
        )}

        <Card className="border-slate-200 dark:border-slate-700 dark:bg-slate-800 shadow-sm">
          <CardContent className="p-6">
            <form onSubmit={handleLogin} className="space-y-4">
              <div className="space-y-1.5">
                <Label className="dark:text-slate-300">Preferred ID</Label>
                <Input
                  autoFocus
                  value={username}
                  onChange={e => { setUsername(e.target.value); setError(""); }}
                  placeholder="Enter your preferred ID"
                  className="dark:bg-slate-700 dark:border-slate-600 dark:text-white"
                />
              </div>
              <div className="space-y-1.5">
                <Label className="dark:text-slate-300">Password</Label>
                <div className="relative">
                  <Input
                    type={showPassword ? "text" : "password"}
                    value={password}
                    onChange={e => { setPassword(e.target.value); setError(""); }}
                    placeholder="••••••••"
                    className="pr-10 dark:bg-slate-700 dark:border-slate-600 dark:text-white"
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword(v => !v)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
                  >
                    {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                  </button>
                </div>
              </div>

              {error && (
                <p className="text-sm text-red-600 dark:text-red-400 flex items-center gap-1.5">
                  <AlertCircle className="w-3.5 h-3.5" />
                  {error}
                </p>
              )}

              <Button type="submit" disabled={loading} className="w-full gap-2 bg-[#0060AF] hover:bg-[#004d8c] text-white">
                {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <LogIn className="w-4 h-4" />}
                {loading ? "Authenticating..." : "Sign In"}
              </Button>
            </form>
          </CardContent>
        </Card>

        <p className="text-center text-xs text-slate-400 dark:text-slate-500">
          Secured with US Bank LDAP directory authentication
        </p>
      </div>
    </div>
  );
}
