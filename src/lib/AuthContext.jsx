import React, { createContext, useState, useContext, useEffect } from 'react';
import { dataflow } from '@/api/client';

const AuthContext = createContext();

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [workspace, setWorkspace] = useState(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoadingAuth, setIsLoadingAuth] = useState(true);
  const [isLoadingPublicSettings, setIsLoadingPublicSettings] = useState(false);
  const [authError, setAuthError] = useState(null);
  const [appPublicSettings, setAppPublicSettings] = useState({ name: 'DataFlow' });

  useEffect(() => {
    checkAppState();
  }, []);

  const checkAppState = async () => {
    try {
      setIsLoadingAuth(true);
      setAuthError(null);
      const currentUser = await dataflow.auth.me();
      setUser(currentUser);
      setIsAuthenticated(true);
      if (currentUser.workspace) {
        setWorkspace(currentUser.workspace);
      } else {
        const stored = sessionStorage.getItem('dataflow-workspace');
        if (stored) {
          try { setWorkspace(JSON.parse(stored)); } catch {}
        }
      }
    } catch (error) {
      setUser(null);
      setIsAuthenticated(false);
      setWorkspace(null);
    } finally {
      setIsLoadingAuth(false);
    }
  };

  const logout = async () => {
    await dataflow.auth.logout();
    setUser(null);
    setIsAuthenticated(false);
    setWorkspace(null);
  };

  const navigateToLogin = () => {
    dataflow.auth.redirectToLogin();
  };

  const switchWorkspace = async (workspaceId) => {
    const result = await dataflow.auth.switchWorkspace(workspaceId);
    if (result.workspace) {
      setWorkspace(result.workspace);
    }
    return result;
  };

  return (
    <AuthContext.Provider value={{ 
      user, 
      workspace,
      isAuthenticated, 
      isLoadingAuth,
      isLoadingPublicSettings,
      authError,
      appPublicSettings,
      logout,
      navigateToLogin,
      checkAppState,
      switchWorkspace
    }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
