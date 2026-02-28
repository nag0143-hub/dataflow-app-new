import { Link, useLocation, useNavigate } from "react-router-dom";
import { createPageUrl } from "./utils";
import { useState, useEffect } from "react";
import { 
  Workflow,
  Home,
  Cable,
  Play,
  Sun,
  Moon,
  BookOpen,
  HelpCircle,
  PanelLeftClose,
  PanelLeft,
  Database,
  ScrollText,
  Activity,
  Wind,
  Code2,
  Shield,
  LogOut,
  MonitorDot
} from "lucide-react";
import { cn } from "@/lib/utils";
import featureFlags from "@/feature-flags";
import { useAuth } from "@/lib/AuthContext";

export default function Layout({ children }) {
  const location = useLocation();
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const [darkMode, setDarkMode] = useState(() => {
    return localStorage.getItem("dataflow-dark") === "true";
  });
  const [collapsed, setCollapsed] = useState(() => {
    return localStorage.getItem("dataflow-sidebar-collapsed") === "true";
  });
  const [adminMode, setAdminMode] = useState(() => {
    return localStorage.getItem("dataflow-admin") === "true";
  });
  
  useEffect(() => {
    if (darkMode) {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
    localStorage.setItem("dataflow-dark", darkMode);
  }, [darkMode]);

  useEffect(() => {
    localStorage.setItem("dataflow-sidebar-collapsed", collapsed);
  }, [collapsed]);

  useEffect(() => {
    localStorage.setItem("dataflow-admin", adminMode);
  }, [adminMode]);

  const handleSignOut = async () => {
    await logout();
    navigate("/", { replace: true });
  };

  const navItems = [
    { name: "Dashboard", icon: Home, page: "Dashboard", flag: "dashboard" },
    { name: "Connections", icon: Cable, page: "Connections", flag: "connections" },
    { name: "Pipelines", icon: Play, page: "Pipelines", flag: "pipelines" },
    { name: "Data Catalog", icon: BookOpen, page: "DataCatalog", flag: "dataCatalog" },
    { name: "User Guide", icon: HelpCircle, page: "UserGuide", flag: "userGuide" },
  ];

  const adminNavItems = [
    { name: "Data Model", icon: Database, page: "DataModel", flag: "dataModel" },
    { name: "Audit Trail", icon: ScrollText, page: "AuditTrail", flag: "auditTrail" },
    { name: "Activity Logs", icon: Activity, page: "ActivityLogs", flag: "activityLogs" },
    { name: "Airflow", icon: Wind, page: "Airflow", flag: "airflow" },
    { name: "Airflow Console", icon: MonitorDot, page: "AirflowConsole", flag: "airflowConsole" },
    { name: "Function Library", icon: Code2, page: "CustomFunctions", flag: "customFunctions" },
  ];

  const isActive = (page) => {
    const path = location.pathname.toLowerCase();
    return path.includes(page.toLowerCase()) || 
           (page === "Dashboard" && path === "/");
  };

  return (
    <div className="min-h-screen bg-background flex">
      <aside className={cn(
        "fixed top-0 left-0 bottom-0 z-50 flex flex-col border-r transition-all duration-300",
        "bg-[hsl(var(--sidebar-background))] border-[hsl(var(--sidebar-border))]",
        collapsed ? "w-[60px]" : "w-[240px]"
      )}>
        <div className={cn(
          "flex items-center h-16 border-b border-[hsl(var(--sidebar-border))]",
          collapsed ? "justify-center px-2" : "px-4 gap-3"
        )}>
          <div className="w-9 h-9 rounded-lg bg-[#0060AF] flex items-center justify-center flex-shrink-0">
            <Workflow className="w-5 h-5 text-white" />
          </div>
          {!collapsed && (
            <div className="min-w-0">
              <h1 className="text-sm font-semibold tracking-tight text-white truncate">DataFlow</h1>
              <p className="text-[11px] text-[hsl(var(--sidebar-foreground))] truncate">Data Connector Platform</p>
            </div>
          )}
        </div>

        <nav className="flex-1 py-3 px-2 space-y-1 overflow-y-auto">
          {navItems.filter(item => featureFlags[item.flag]?.enabled !== false).map((item) => {
            const isComingSoon = featureFlags[item.flag]?.comingSoon;
            if (isComingSoon) {
              return (
                <div
                  key={item.page}
                  title={collapsed ? `${item.name} — Coming Soon` : "Coming Soon"}
                  className={cn(
                    "flex items-center gap-3 rounded-lg text-sm font-medium cursor-not-allowed opacity-50",
                    collapsed ? "justify-center px-2 py-2.5" : "px-3 py-2.5",
                    "text-[hsl(var(--sidebar-foreground))]"
                  )}
                >
                  <item.icon className="w-[18px] h-[18px] flex-shrink-0" />
                  {!collapsed && (
                    <>
                      <span className="truncate">{item.name}</span>
                      <span className="ml-auto text-[9px] uppercase tracking-wider bg-white/10 text-white/50 px-1.5 py-0.5 rounded-full shrink-0">Soon</span>
                    </>
                  )}
                </div>
              );
            }
            return (
              <Link
                key={item.page}
                to={createPageUrl(item.page)}
                title={collapsed ? item.name : undefined}
                className={cn(
                  "flex items-center gap-3 rounded-lg text-sm font-medium transition-all duration-200",
                  collapsed ? "justify-center px-2 py-2.5" : "px-3 py-2.5",
                  isActive(item.page)
                    ? "bg-[#0060AF] text-white shadow-sm shadow-[#0060AF]/30"
                    : "text-[hsl(var(--sidebar-foreground))] hover:bg-[hsl(var(--sidebar-accent))] hover:text-white"
                )}
              >
                <item.icon className="w-[18px] h-[18px] flex-shrink-0" />
                {!collapsed && <span className="truncate">{item.name}</span>}
              </Link>
            );
          })}
          {adminMode && (
            <>
              <div className="h-px bg-[hsl(var(--sidebar-border))] mx-2 my-2" />
              {!collapsed && (
                <div className="text-[10px] uppercase tracking-widest text-[hsl(var(--sidebar-foreground))] opacity-60 px-3 mb-1">Admin</div>
              )}
              {adminNavItems.filter(item => featureFlags[item.flag]?.enabled !== false).map((item) => {
                const isComingSoon = featureFlags[item.flag]?.comingSoon;
                if (isComingSoon) {
                  return (
                    <div
                      key={item.page}
                      title={collapsed ? `${item.name} — Coming Soon` : "Coming Soon"}
                      className={cn(
                        "flex items-center gap-3 rounded-lg text-sm font-medium cursor-not-allowed opacity-50",
                        collapsed ? "justify-center px-2 py-2.5" : "px-3 py-2.5",
                        "text-[hsl(var(--sidebar-foreground))]"
                      )}
                    >
                      <item.icon className="w-[18px] h-[18px] flex-shrink-0" />
                      {!collapsed && (
                        <>
                          <span className="truncate">{item.name}</span>
                          <span className="ml-auto text-[9px] uppercase tracking-wider bg-white/10 text-white/50 px-1.5 py-0.5 rounded-full shrink-0">Soon</span>
                        </>
                      )}
                    </div>
                  );
                }
                return (
                  <Link
                    key={item.page}
                    to={createPageUrl(item.page)}
                    title={collapsed ? item.name : undefined}
                    className={cn(
                      "flex items-center gap-3 rounded-lg text-sm font-medium transition-all duration-200",
                      collapsed ? "justify-center px-2 py-2.5" : "px-3 py-2.5",
                      isActive(item.page)
                        ? "bg-[#0060AF] text-white shadow-sm shadow-[#0060AF]/30"
                        : "text-[hsl(var(--sidebar-foreground))] hover:bg-[hsl(var(--sidebar-accent))] hover:text-white"
                    )}
                  >
                    <item.icon className="w-[18px] h-[18px] flex-shrink-0" />
                    {!collapsed && <span className="truncate">{item.name}</span>}
                  </Link>
                );
              })}
            </>
          )}
        </nav>

        {user && !collapsed && (
          <div className="px-3 py-2 border-t border-[hsl(var(--sidebar-border))]">
            <div className="flex items-center gap-2.5">
              <div className="w-7 h-7 rounded-full bg-[#0060AF]/20 flex items-center justify-center text-xs font-bold text-[#0060AF] shrink-0">
                {(user.name || user.username || "U").charAt(0).toUpperCase()}
              </div>
              <div className="min-w-0">
                <p className="text-xs font-medium text-white truncate">{user.name || user.username}</p>
                <p className="text-[10px] text-[hsl(var(--sidebar-foreground))] opacity-60 truncate capitalize">{user.role || "user"}</p>
              </div>
            </div>
          </div>
        )}

        <div className={cn(
          "border-t border-[hsl(var(--sidebar-border))] py-3 px-2 space-y-1"
        )}>
          <button
            onClick={() => setDarkMode(!darkMode)}
            className={cn(
              "flex items-center gap-3 w-full rounded-lg text-sm font-medium transition-colors",
              "text-[hsl(var(--sidebar-foreground))] hover:bg-[hsl(var(--sidebar-accent))] hover:text-white",
              collapsed ? "justify-center px-2 py-2.5" : "px-3 py-2.5"
            )}
            title={darkMode ? "Switch to light mode" : "Switch to dark mode"}
          >
            {darkMode ? <Sun className="w-[18px] h-[18px] flex-shrink-0 text-amber-400" /> : <Moon className="w-[18px] h-[18px] flex-shrink-0" />}
            {!collapsed && <span>{darkMode ? "Light Mode" : "Dark Mode"}</span>}
          </button>
          <button
            onClick={() => setAdminMode(!adminMode)}
            className={cn(
              "flex items-center gap-3 w-full rounded-lg text-sm font-medium transition-colors",
              "text-[hsl(var(--sidebar-foreground))] hover:bg-[hsl(var(--sidebar-accent))] hover:text-white",
              collapsed ? "justify-center px-2 py-2.5" : "px-3 py-2.5"
            )}
            title={adminMode ? "Disable admin mode" : "Enable admin mode"}
          >
            <Shield className={cn("w-[18px] h-[18px] flex-shrink-0", adminMode && "text-amber-400")} />
            {!collapsed && <span>Admin Mode</span>}
          </button>
          <button
            onClick={() => setCollapsed(!collapsed)}
            className={cn(
              "flex items-center gap-3 w-full rounded-lg text-sm font-medium transition-colors",
              "text-[hsl(var(--sidebar-foreground))] hover:bg-[hsl(var(--sidebar-accent))] hover:text-white",
              collapsed ? "justify-center px-2 py-2.5" : "px-3 py-2.5"
            )}
            title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {collapsed ? <PanelLeft className="w-[18px] h-[18px] flex-shrink-0" /> : <PanelLeftClose className="w-[18px] h-[18px] flex-shrink-0" />}
            {!collapsed && <span>Collapse</span>}
          </button>
          <button
            onClick={handleSignOut}
            className={cn(
              "flex items-center gap-3 w-full rounded-lg text-sm font-medium transition-colors",
              "text-red-400 hover:bg-red-500/10 hover:text-red-300",
              collapsed ? "justify-center px-2 py-2.5" : "px-3 py-2.5"
            )}
            title="Sign Out"
          >
            <LogOut className="w-[18px] h-[18px] flex-shrink-0" />
            {!collapsed && <span>Sign Out</span>}
          </button>
        </div>
      </aside>
      
      <main className={cn(
        "flex-1 min-h-screen transition-all duration-300",
        collapsed ? "ml-[60px]" : "ml-[240px]"
      )}>
        <div className="p-6 lg:p-8">
          {children}
        </div>
      </main>
    </div>
  );
}
