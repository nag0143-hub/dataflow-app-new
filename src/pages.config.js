import { lazy } from 'react';
import __Layout from './Layout.jsx';

const AuditTrail = lazy(() => import('./pages/AuditTrail'));
const CustomFunctions = lazy(() => import('./pages/CustomFunctions'));
const DataCatalog = lazy(() => import('./pages/DataCatalog'));
const DataModel = lazy(() => import('./pages/DataModel'));
const LDAPIntegration = lazy(() => import('./pages/LDAPIntegration'));
const Pipelines = lazy(() => import('./pages/Pipelines'));
const Requirements = lazy(() => import('./pages/Requirements'));
const UserGuide = lazy(() => import('./pages/UserGuide'));
const Dashboard = lazy(() => import('./pages/Dashboard'));
const ActivityLogs = lazy(() => import('./pages/ActivityLogs'));
const Connections = lazy(() => import('./pages/Connections'));
const Airflow = lazy(() => import('./pages/Airflow'));
const AirflowConsole = lazy(() => import('./pages/AirflowConsole'));


export const PAGES = {
    "AuditTrail": AuditTrail,
    "CustomFunctions": CustomFunctions,
    "DataCatalog": DataCatalog,
    "DataModel": DataModel,
    "LDAPIntegration": LDAPIntegration,
    "Pipelines": Pipelines,
    "Requirements": Requirements,
    "UserGuide": UserGuide,
    "Dashboard": Dashboard,
    "ActivityLogs": ActivityLogs,
    "Connections": Connections,
    "Airflow": Airflow,
    "AirflowConsole": AirflowConsole,
}

export const pagesConfig = {
    mainPage: "Dashboard",
    Pages: PAGES,
    Layout: __Layout,
};
