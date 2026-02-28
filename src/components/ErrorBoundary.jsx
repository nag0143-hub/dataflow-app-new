import { Component } from "react";
import { AlertTriangle, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    console.error("[ErrorBoundary]", error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex flex-col items-center justify-center min-h-[40vh] gap-4 text-center p-8">
          <div className="w-16 h-16 rounded-full bg-red-50 dark:bg-red-900/20 flex items-center justify-center">
            <AlertTriangle className="w-8 h-8 text-red-500" />
          </div>
          <div>
            <h2 className="text-xl font-semibold text-slate-900 dark:text-white mb-1">Something went wrong</h2>
            <p className="text-sm text-slate-500 dark:text-slate-400 max-w-sm">
              {this.state.error?.message || "An unexpected error occurred."}
            </p>
          </div>
          <Button
            variant="outline"
            onClick={() => { this.setState({ hasError: false, error: null }); window.location.reload(); }}
            className="gap-2"
          >
            <RefreshCw className="w-4 h-4" />
            Reload Page
          </Button>
        </div>
      );
    }
    return this.props.children;
  }
}