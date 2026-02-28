import { Suspense, lazy } from 'react'
import { Toaster } from "@/components/ui/toaster"
import { QueryClientProvider } from '@tanstack/react-query'
import { queryClientInstance } from '@/lib/query-client'
import NavigationTracker from '@/lib/NavigationTracker'
import { pagesConfig } from './pages.config'
import { BrowserRouter as Router, Route, Routes, Navigate } from 'react-router-dom';
import PageNotFound from './lib/PageNotFound';
import { AuthProvider, useAuth } from '@/lib/AuthContext';
import UserNotRegisteredError from '@/components/UserNotRegisteredError';

const LoginPage = lazy(() => import('./pages/LDAPIntegration'));

const { Pages, Layout } = pagesConfig;

const LayoutWrapper = ({ children, currentPageName }) => Layout ?
  <Layout currentPageName={currentPageName}>{children}</Layout>
  : <>{children}</>;

const AuthenticatedApp = () => {
  const { isAuthenticated, isLoadingAuth, isLoadingPublicSettings, authError } = useAuth();

  if (isLoadingPublicSettings || isLoadingAuth) {
    return (
      <div className="fixed inset-0 flex items-center justify-center bg-background">
        <div className="w-8 h-8 border-4 border-slate-200 border-t-[#0060AF] rounded-full animate-spin"></div>
      </div>
    );
  }

  if (authError) {
    if (authError.type === 'user_not_registered') {
      return <UserNotRegisteredError />;
    }
  }

  if (!isAuthenticated) {
    return (
      <Suspense fallback={
        <div className="fixed inset-0 flex items-center justify-center bg-background">
          <div className="w-8 h-8 border-4 border-slate-200 border-t-[#0060AF] rounded-full animate-spin"></div>
        </div>
      }>
        <Routes>
          <Route path="*" element={<LoginPage />} />
        </Routes>
      </Suspense>
    );
  }

  const suspenseFallback = (
    <div className="flex items-center justify-center h-full min-h-[200px]">
      <div className="w-6 h-6 border-3 border-slate-200 border-t-primary rounded-full animate-spin"></div>
    </div>
  );

  return (
    <Suspense fallback={suspenseFallback}>
      <Routes>
        <Route path="/" element={
          <LayoutWrapper currentPageName="Dashboard">
            {Pages.Dashboard ? <Pages.Dashboard /> : <Navigate to="/Dashboard" />}
          </LayoutWrapper>
        } />
        {Object.entries(Pages).filter(([path]) => path !== 'LDAPIntegration').map(([path, Page]) => (
          <Route
            key={path}
            path={`/${path}`}
            element={
              <LayoutWrapper currentPageName={path}>
                <Page />
              </LayoutWrapper>
            }
          />
        ))}
        <Route path="/LDAPIntegration" element={<Navigate to="/" replace />} />
        <Route path="*" element={<PageNotFound />} />
      </Routes>
    </Suspense>
  );
};


function App() {

  return (
    <AuthProvider>
      <QueryClientProvider client={queryClientInstance}>
        <Router>
          <NavigationTracker />
          <AuthenticatedApp />
        </Router>
        <Toaster />
      </QueryClientProvider>
    </AuthProvider>
  )
}

export default App
