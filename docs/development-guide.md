# DataFlow Platform — Comprehensive Development Guide

This guide covers everything a developer needs to understand, maintain, manage, and enhance the DataFlow platform. It assumes no prior experience with this application or its technology stack.

---

## Table of Contents

1. [What Is DataFlow?](#1-what-is-dataflow)
2. [Technology Stack Overview](#2-technology-stack-overview)
3. [Getting Started](#3-getting-started)
4. [Project Structure](#4-project-structure)
5. [Configuration System](#5-configuration-system)
6. [Backend Architecture](#6-backend-architecture)
7. [Frontend Architecture](#7-frontend-architecture)
8. [Database Design](#8-database-design)
9. [Authentication & Sessions](#9-authentication--sessions)
10. [API Reference](#10-api-reference)
11. [Pipeline Wizard (End-to-End)](#11-pipeline-wizard-end-to-end)
12. [DAG Generation & Deployment](#12-dag-generation--deployment)
13. [External Integrations](#13-external-integrations)
14. [Feature Flags & Navigation](#14-feature-flags--navigation)
15. [Styling & Theming](#15-styling--theming)
16. [Environment Variables](#16-environment-variables)
17. [Docker & Deployment](#17-docker--deployment)
18. [Common Development Tasks](#18-common-development-tasks)
19. [Troubleshooting](#19-troubleshooting)

---

## 1. What Is DataFlow?

DataFlow is an internal data pipeline management platform (inspired by Airbyte and Fivetran). It allows data engineers to:

- **Create connections** to databases (PostgreSQL, SQL Server, MySQL, Oracle, MongoDB), cloud storage (Azure ADLS Gen2, AWS S3), flat files (delimited, fixed-width, COBOL/EBCDIC), and file transfer systems (SFTP, NAS).
- **Build pipelines** that move data between connections with scheduling, data quality rules, column mapping, masking, and SLA monitoring.
- **Generate Airflow DAGs** in dag-factory YAML format, automatically filled from the pipeline configuration.
- **Deploy to GitLab** using LDAP credentials (no stored tokens) — the generated DAG and pipeline spec files are committed to a Git repository for CI/CD.
- **Monitor orchestration** by proxying Airflow's REST API to display DAG runs, task status, and pipeline health.

The platform is designed for **1,000+ users** with workspace-based multi-tenancy, Redis session caching, and PostgreSQL JSONB storage.

---

## 2. Technology Stack Overview

### Frontend
| Technology | Purpose | Version |
|---|---|---|
| **React** | UI framework (component-based) | 18.2 |
| **Vite** | Build tool and dev server (fast HMR) | 6.1 |
| **React Router** | Client-side page routing | 6.26 |
| **TanStack React Query** | Server state management and caching | 5.84 |
| **Tailwind CSS** | Utility-first CSS framework | 3.4 |
| **Radix UI** | Headless accessible UI primitives (shadcn/ui) | Various |
| **Lucide React** | Icon library | 0.475 |
| **Recharts** | Chart/visualization library | 2.15 |
| **Framer Motion** | Animation library | 11.16 |

### Backend
| Technology | Purpose | Version |
|---|---|---|
| **Express.js** | HTTP API server | 5.2 |
| **PostgreSQL** | Primary database (via `pg` driver) | 16+ |
| **Redis** | Session store and cache (via `ioredis`) | 7+ |
| **Pino** | Structured JSON logging | 10.3 |
| **Zod** | Runtime input validation | 3.25 |
| **LDAP** | Authentication (via `ldap-authentication`) | 4.0 |
| **Helmet** | HTTP security headers | 8.1 |

### Database Drivers (for testing connections to external systems)
| Driver | Database |
|---|---|
| `pg` | PostgreSQL |
| `mysql2` | MySQL / MariaDB |
| `mssql` | Microsoft SQL Server |
| `mongodb` | MongoDB |
| `oracledb` | Oracle (requires Instant Client) |

### Build & Dev Tools
| Tool | Purpose |
|---|---|
| `vite` | Dev server with hot module replacement |
| `eslint` | JavaScript linting |
| `postcss` + `autoprefixer` | CSS processing for Tailwind |
| `Docker` + `docker-compose` | Containerized deployment |

### Key Concepts for New Developers

- **JSX**: React uses JSX — HTML-like syntax inside JavaScript. Files use `.jsx` extension.
- **ESM**: The project uses ES Modules (`import`/`export`) not CommonJS (`require`).
- **Tailwind CSS**: Styles are applied as CSS class names directly in JSX (e.g., `className="text-sm font-bold text-slate-900"`). No separate CSS files for most components.
- **JSONB**: PostgreSQL's JSON Binary column type. DataFlow stores all entity data as JSONB documents, combining the flexibility of a document database with PostgreSQL's relational guarantees.

---

## 3. Getting Started

### Prerequisites

- **Node.js 20+** (LTS recommended)
- **PostgreSQL 16+** (local or remote)
- **Redis 7+** (optional — the app falls back to in-memory storage without it)
- **npm** (comes with Node.js)

### Local Development Setup

```bash
# 1. Clone the repository
git clone <repo-url>
cd dataflow-app

# 2. Install dependencies
npm install

# 3. Set up environment variables
cp .env.example .env
# Edit .env — at minimum, set DATABASE_URL

# 4. Start development server
npm run dev
```

The app starts at `http://localhost:5000`. In development mode:
- The Vite dev server serves the frontend with hot module replacement (instant UI updates on file save)
- The Express API server runs as a Vite plugin (no separate process needed)
- LDAP auth runs in demo mode (credentials: `admin` / `admin`)
- Database tables are auto-created on first startup

### Available Scripts

| Command | Description |
|---|---|
| `npm run dev` | Start development server (Vite + Express plugin) |
| `npm run build` | Build the frontend for production (outputs to `dist/`) |
| `npm start` | Start production server (Express serves API + static files) |
| `npm run lint` | Run ESLint checks |
| `npm run lint:fix` | Auto-fix ESLint issues |
| `npm run preview` | Preview the production build locally |

---

## 4. Project Structure

```
dataflow-app/
├── dataflow.yaml                    # Master configuration file
├── package.json                     # Dependencies and scripts
├── vite.config.js                   # Vite build configuration
├── tailwind.config.js               # Tailwind CSS theme customization
├── jsconfig.json                    # Path aliases (@/ → src/)
├── Dockerfile                       # Multi-stage Docker build
├── docker-compose.yml               # Full stack orchestration
├── .env.example                     # Environment variable template
│
├── config/                          # Server-side configuration
│   ├── index.js                     # Config loader (switches by NODE_ENV)
│   ├── development.js               # Dev environment defaults
│   ├── production.js                # Prod environment defaults
│   ├── pipeline-wizard.yaml         # Wizard steps, transforms, DQ rules
│   └── platforms.yaml               # Connection platform definitions
│
├── server/                          # Backend (Express.js API)
│   ├── middleware.js                 # API routes + middleware (dev server)
│   ├── production.js                # Standalone production server
│   ├── db.js                        # PostgreSQL pool, table init, queries
│   ├── redis.js                     # Redis client with in-memory fallback
│   ├── ldap-auth.js                 # LDAP authentication + sessions
│   ├── logger.js                    # Pino structured logging
│   ├── validation.js                # Zod schemas + validation middleware
│   ├── gitlab.js                    # GitLab API integration
│   ├── vault.js                     # HashiCorp Vault client
│   ├── airflow-proxy.js             # Airflow REST API proxy
│   ├── test-connection.js           # Database connection tester
│   ├── introspect-schema.js         # Live schema discovery
│   ├── spec-validator.js            # Pipeline spec validation
│   ├── scripts/
│   │   ├── generate-pipeline-tasks.js  # Python sensor + ingestion script generator
│   │   ├── generate-spark-script.js    # PySpark extraction/transform script generator
│   │   └── generate-bootstrap.js       # dag-factory bootstrap + mapping JSON generator
│
├── src/                             # Frontend (React)
│   ├── main.jsx                     # Application entry point
│   ├── App.jsx                      # Root component with routing + auth gate
│   ├── Layout.jsx                   # Sidebar navigation shell
│   ├── pages.config.js              # Page registry (lazy-loaded)
│   ├── dataflow-config.js           # YAML config loader (frontend)
│   ├── feature-flags.js             # Feature toggle definitions
│   │
│   ├── api/
│   │   └── client.js                # SDK singleton export
│   │
│   ├── lib/
│   │   ├── local-sdk.js             # REST API client (entity CRUD, auth)
│   │   ├── AuthContext.jsx           # Authentication context provider
│   │   ├── NavigationTracker.jsx     # Page view logging
│   │   ├── query-client.js           # TanStack Query configuration
│   │   ├── app-params.js            # URL parameter management
│   │   └── utils.js                 # cn() utility for class merging
│   │
│   ├── pages/                       # Route-level page components
│   │   ├── Dashboard.jsx            # Overview with stats + recent runs
│   │   ├── Pipelines.jsx            # Pipeline list/card view + management
│   │   ├── Connections.jsx          # Connection management + testing
│   │   ├── LDAPIntegration.jsx      # Login page
│   │   ├── DataCatalog.jsx          # Data catalog (placeholder)
│   │   ├── UserGuide.jsx            # User guide (placeholder)
│   │   ├── DataModel.jsx            # Admin: live database schema viewer
│   │   ├── AuditTrail.jsx           # Admin: entity change history
│   │   ├── ActivityLogs.jsx         # Admin: system activity logs
│   │   ├── Airflow.jsx              # Admin: Airflow DAG management
│   │   └── CustomFunctions.jsx      # Admin: custom function registry
│   │
│   ├── components/                  # Reusable UI components
│   │   ├── ui/                      # shadcn/ui primitives (button, card, dialog, etc.)
│   │   ├── JobFormDialog.jsx        # Pipeline wizard dialog (multi-step)
│   │   ├── JobFormTabs/             # Wizard tab components
│   │   │   ├── JobBasicsTab.jsx     # Step 1: Name, connections, metadata
│   │   │   ├── JobDataTab.jsx       # Step 2: Dataset selection
│   │   │   ├── ScheduleSettings.jsx # Step 3: Scheduling
│   │   │   └── AdvancedTabContent.jsx # Step 4: DQ, mapping, masking, SLA
│   │   ├── DagTemplates.js          # Airflow DAG YAML generation
│   │   ├── JobSpecExport.jsx        # Pipeline spec builder
│   │   ├── JobSpecTabPreview.jsx    # Review tab with YAML preview
│   │   ├── DeployTabContent.jsx     # Deploy tab with GitLab integration
│   │   ├── GitCheckinDialog.jsx     # Quick deploy dialog (outside wizard)
│   │   ├── PipelineCard.jsx         # Pipeline card view component
│   │   ├── PipelineListRow.jsx      # Pipeline list view row
│   │   ├── ConnectionCard.jsx       # Connection card view component
│   │   ├── PlatformIcon.jsx         # Platform icons + color config
│   │   ├── StatusBadge.jsx          # Status indicator badges
│   │   ├── StatCard.jsx             # Dashboard stat cards
│   │   ├── OrchestrationPanel.jsx   # Airflow status display
│   │   ├── PipelineStepIndicator.jsx # Wizard step progress bar
│   │   ├── ObjectSelector.jsx       # Schema/table picker for databases
│   │   ├── SchemaImporter.jsx       # DDL import + validation
│   │   ├── SchemaDefinitionPanel.jsx # Flat file schema definition
│   │   ├── ColumnMapper/            # Column mapping components
│   │   ├── DataQualityRules.jsx     # DQ rule configuration
│   │   ├── DataCleansing.jsx        # Data cleansing rules
│   │   ├── DataMaskingConfig.jsx    # Data masking configuration
│   │   ├── SLAConfig.jsx            # SLA monitoring setup
│   │   ├── EmptyStateGuide.jsx      # Empty state with CTA
│   │   ├── OnboardingWizard.jsx     # First-time setup guide
│   │   └── ...                      # Other shared components
│   │
│   ├── hooks/                       # Custom React hooks
│   │   └── useAirflowStatusSync.js  # Airflow polling hook
│   │
│   └── utils/
│       └── toYaml.js                # YAML serialization utility
│
├── public/                          # Static assets
│   └── dataflow-icon.svg            # App icon
│
└── docs/                            # Documentation
    ├── development-guide.md          # This file
    └── production-readiness-review.md
```

---

## 5. Configuration System

DataFlow uses a three-layer configuration architecture:

### Layer 1: Master Config (`dataflow.yaml`)

This is the central configuration file. It defines:

- **Infrastructure**: Server, database, Redis, LDAP, Vault, GitLab, Airflow settings
- **Navigation**: Which sidebar items are visible or marked "Coming Soon"
- **Features**: Toggle platform capabilities on/off
- **Environment Overrides**: Per-environment (dev, IT, UAT, prod) overrides

```yaml
# Example: Disable a feature
features:
  vault_credentials: { enabled: false }

# Example: Mark a nav item as "Coming Soon"
navigation:
  main:
    data_catalog: { enabled: true, coming_soon: true }
```

### Layer 2: Specialized Configs (`config/`)

| File | Purpose |
|---|---|
| `pipeline-wizard.yaml` | Wizard step definitions, advanced feature toggles, transform functions, DQ rule types, masking types, schedule presets, load methods, delivery channels, event sensors |
| `platforms.yaml` | Connection platform definitions with categories (database, cloud, file, orchestration). Categories determine UI behavior (e.g., flat file platforms show file schema instead of table selector) |

### Layer 3: Environment Configs (`config/*.js`)

Server-side runtime configuration files that read environment variables:

| File | When Used | Key Differences |
|---|---|---|
| `config/development.js` | `NODE_ENV !== 'production'` | Debug logging, smaller DB pools, request logging on |
| `config/production.js` | `NODE_ENV === 'production'` | Warn-level logging, larger DB pools, request logging off |
| `config/index.js` | Always | Auto-selects the right config based on `NODE_ENV` |

### How Config Flows to the Frontend

```
dataflow.yaml  ──┐
platforms.yaml ──┤──→ src/dataflow-config.js ──→ src/feature-flags.js ──→ Layout.jsx
wizard.yaml    ──┘         (merges + applies            (derives flags)     (shows/hides
                            env overrides)                                   nav items)
```

`src/dataflow-config.js` loads all three YAML files, merges them, and applies environment-specific overrides based on `VITE_DATAFLOW_ENV` or `NODE_ENV`.

---

## 6. Backend Architecture

### How the Server Works

The Express server runs differently in development vs. production:

**Development** (`npm run dev`):
- Vite starts its dev server on port 5000
- The Express API middleware is loaded as a Vite plugin (see `vite.config.js`)
- Both frontend (with HMR) and API routes are served from the same port
- No need for CORS or separate processes

**Production** (`npm start`):
- `server/production.js` runs a standalone Express server
- It serves the built static files from `dist/` and the API routes
- Includes production hardening: `helmet`, `compression`, rate limiting

### Middleware Chain (Request Processing Order)

```
Request → cookie-parser → JSON body parser → logger (pino)
        → rate limiter (read: 1000/15m, write: 200/15m)
        → route matching → [requireWorkspace] → handler → response
```

The `requireWorkspace` middleware:
1. Extracts the session ID from the `dataflow_sid` cookie (or `x-session-id` header as fallback)
2. Looks up the session in Redis (or in-memory store)
3. Resolves the user's workspace
4. Injects `req.workspaceId` and `req.sessionUser` into the request
5. Returns 401 if no valid session is found

### Structured Logging (`server/logger.js`)

All server logs are JSON-formatted using Pino:

```json
{
  "level": "info",
  "time": "2026-02-28T14:48:07.488Z",
  "service": "dataflow",
  "env": "development",
  "pid": 10288,
  "reqId": "req-mm6frdsl-0",
  "method": "GET",
  "url": "/api/entities/Pipeline",
  "statusCode": 200,
  "duration_ms": 3,
  "msg": "request completed"
}
```

Log levels route by HTTP status:
- **5xx** → `logger.error` ("request server error")
- **4xx** → `logger.warn` ("request client error")
- **2xx/3xx** → `logger.info` ("request completed")

### Input Validation (`server/validation.js`)

All API inputs are validated using Zod schemas before reaching handlers:

```javascript
// Example: Login validation
const loginSchema = z.object({
  username: z.string().min(1).max(100),
  password: z.string().min(1).max(200),
});

// Usage on route:
router.post('/login', validateBody(loginSchema), loginHandler);
```

Invalid requests receive a structured 400 response:
```json
{
  "error": "Validation failed",
  "details": [
    { "field": "username", "message": "Required" }
  ]
}
```

### Key Server Files

| File | Responsibility |
|---|---|
| `middleware.js` | All API route definitions for development |
| `production.js` | Production server (same routes + static files + security) |
| `db.js` | PostgreSQL pool, table creation, CRUD queries, workspace management |
| `redis.js` | Redis wrapper with `getSession`/`setSession`/`deleteSession` + cache helpers. Falls back to `Map` if Redis is unavailable |
| `ldap-auth.js` | LDAP bind authentication, session creation (8h TTL), role mapping |
| `logger.js` | Pino logger factory + HTTP request logging middleware |
| `validation.js` | Zod schemas for all API endpoints + `validateBody()`/`validateParams()` middleware |
| `gitlab.js` | GitLab OAuth2 password grant auth + Commits API for file pushes |
| `vault.js` | HashiCorp Vault AppRole authentication + KV v2 secret retrieval |
| `airflow-proxy.js` | Proxies requests to Airflow REST API + DAG file management |
| `test-connection.js` | Tests real database connectivity using native drivers |
| `introspect-schema.js` | Queries `information_schema` for table/column metadata |
| `spec-validator.js` | Validates pipeline YAML specs (structure, cron syntax, connection references) |
| `scripts/generate-pipeline-tasks.js` | Generates Python scripts with sensor callables (ADLS2, S3, SFTP, local, NAS) and ingestion functions for flat file pipelines |
| `scripts/generate-spark-script.js` | Generates PySpark scripts with JDBC extraction, target writes, column mapping, data cleansing, DQ validation, and security masking |
| `scripts/generate-bootstrap.js` | Generates dag-factory bootstrap loader (generate_dags.py) and column mapping JSON files |

---

## 7. Frontend Architecture

### Application Bootstrap Flow

```
main.jsx → App.jsx → AuthProvider → QueryClientProvider → Router
                         ↓
                    AuthContext checks /api/auth/me
                         ↓
               ┌─── Not Authenticated ──→ Login Page (no sidebar)
               └─── Authenticated ──→ Layout (sidebar) + Routes
```

### Routing

Routes are defined in `src/pages.config.js` using lazy loading:

```javascript
const Dashboard = lazy(() => import('./pages/Dashboard'));
const Pipelines = lazy(() => import('./pages/Pipelines'));
// ... etc
```

`App.jsx` dynamically generates routes from `pages.config.js`. Each page is wrapped in the `Layout` component (sidebar + header). The root path `/` renders the Dashboard.

### Auth Gate

`App.jsx` checks `isAuthenticated` from `AuthContext`:
- **Not authenticated** → All routes show the login page (no sidebar visible)
- **Authenticated** → Normal routing with Layout
- **Sign Out** (in sidebar) → Clears session → Routes back to login

### State Management

| Layer | Tool | Purpose |
|---|---|---|
| **Server State** | TanStack React Query | Caching API responses, background refetching |
| **Auth State** | React Context (`AuthContext`) | User, workspace, authentication status |
| **UI State** | React `useState` | Form data, dialogs, filters, view modes |
| **Persistence** | `localStorage` | Dark mode, admin mode, sidebar collapsed state |
| **Session** | `sessionStorage` | Session ID, workspace info |

### The SDK Client (`src/lib/local-sdk.js`)

This is the single interface between the frontend and backend. It uses a **Proxy pattern** to provide CRUD methods for any entity:

```javascript
import { dataflow } from '@/api/client';

// Entity CRUD
const pipelines = await dataflow.entities.Pipeline.list();
const pipeline = await dataflow.entities.Pipeline.get(id);
await dataflow.entities.Pipeline.create({ name: "New Pipeline", ... });
await dataflow.entities.Pipeline.update(id, { status: "active" });
await dataflow.entities.Pipeline.delete(id);

// Filtered listing
const results = await dataflow.entities.Pipeline.filter({ status: "active" });

// Auth
await dataflow.auth.login(username, password);
await dataflow.auth.me();
await dataflow.auth.logout();

// Functions
await dataflow.functions.invoke('searchPipelines', { searchTerm: "finance" });
```

All SDK calls:
- Include `credentials: 'include'` for cookie-based auth
- Include `x-session-id` header as a fallback
- Target `/api/*` endpoints on the same origin

### Component Hierarchy

```
App.jsx
└── Layout.jsx (sidebar + main content area)
    ├── Dashboard.jsx
    │   ├── StatCard (4x: connections, pipelines, success, failed)
    │   ├── Recent Pipeline Runs table
    │   └── OrchestrationPanel (Airflow status)
    │
    ├── Pipelines.jsx
    │   ├── Status filter chips
    │   ├── Card/List view toggle
    │   ├── PipelineCard / PipelineListRow (per pipeline)
    │   ├── JobFormDialog (pipeline wizard)
    │   │   ├── PipelineStepIndicator
    │   │   ├── JobBasicsTab
    │   │   ├── JobDataTab
    │   │   ├── ScheduleSettings
    │   │   ├── AdvancedTabContent
    │   │   ├── JobSpecTabPreview (Review)
    │   │   └── DeployTabContent (Deploy)
    │   ├── JobDetailsDialog
    │   └── JobSpecExport
    │
    └── Connections.jsx
        ├── ConnectionCard (per connection)
        ├── Connection Form Dialog
        └── ConnectionProfilePicker
```

---

## 8. Database Design

### Schema Overview

DataFlow uses a **hybrid relational/document model**:

- **`workspace` table** (relational): Standard columns for multi-tenancy
- **13 entity tables** (JSONB document store): Each stores its data in a `data` JSONB column

### Table Structure

Every entity table has the same columns:

| Column | Type | Purpose |
|---|---|---|
| `id` | `TEXT` (UUID) | Primary key |
| `data` | `JSONB` | All entity-specific fields |
| `workspace_id` | `INTEGER` | Foreign key to `workspace.id` |
| `created_date` | `TIMESTAMPTZ` | Auto-set on insert |
| `updated_date` | `TIMESTAMPTZ` | Auto-set on update |
| `created_by` | `TEXT` | User email from session |

### Entity Tables

| Table Name | Entity Type | Key Fields in `data` |
|---|---|---|
| `pipeline` | Pipeline | name, status, source/target_connection_id, selected_datasets, schedule_type, cron_expression |
| `connection` | Connection | name, platform, host, database, port, auth_method, status |
| `pipeline_run` | PipelineRun | pipeline_id, run_number, status, started_at, completed_at, rows_processed |
| `activity_log` | ActivityLog | log_type (info/warning/error/success), category, message |
| `audit_log` | AuditLog | entity_type, entity_id, action, before/after data |
| `ingestion_job` | IngestionJob | Legacy job tracking |
| `airflow_dag` | AirflowDAG | dag_id, pipeline_id, yaml_content |
| `custom_function` | CustomFunction | name, language, code, description |
| `connection_profile` | ConnectionProfile | name, platform, template settings |
| `connection_prerequisite` | ConnectionPrerequisite | connection_id, requirement, status |
| `pipeline_version` | PipelineVersion | pipeline_id, version_number, snapshot |
| `data_catalog_entry` | DataCatalogEntry | name, description, schema |
| `dag_template` | DagTemplate | name, yaml_template, description |

### Indexes

The database automatically creates these indexes on startup:

1. **B-tree** on `created_date` and `updated_date` (sorting)
2. **GIN** on `data` JSONB column (JSON field lookups)
3. **Functional** on `data->>'status'`, `data->>'name'`, `data->>'platform'` (filtering)
4. **Full-text search** (`to_tsvector`) on name+description for pipelines/connections and message+category for activity logs
5. **Trigram** (`pg_trgm`) on name fields for fuzzy `ILIKE` matching

### How Queries Work

Since entity data lives in JSONB, queries use PostgreSQL's JSON operators:

```sql
-- List pipelines with status "active" in workspace 1
SELECT id, data, created_date, updated_date, created_by
FROM pipeline
WHERE workspace_id = 1 AND data->>'status' = 'active'
ORDER BY created_date DESC;

-- Search by name (case-insensitive, trigram-indexed)
SELECT * FROM pipeline
WHERE workspace_id = 1 AND data->>'name' ILIKE '%finance%';
```

### Multi-Tenancy

All entity queries are scoped by `workspace_id`. The middleware injects `req.workspaceId` from the authenticated session, and every database operation filters by it. There is no way for one workspace to see another's data.

---

## 9. Authentication & Sessions

### Login Flow

```
Browser                         Server
  │                               │
  │  POST /api/auth/login         │
  │  { username, password }       │
  │ ─────────────────────────────>│
  │                               │  1. Validate input (Zod)
  │                               │  2. Authenticate via LDAP
  │                               │     (or demo mode: admin/admin)
  │                               │  3. Create session in Redis
  │                               │  4. Set HttpOnly cookie
  │  200 OK + Set-Cookie          │
  │  { sessionId, user, workspace}│
  │ <─────────────────────────────│
  │                               │
  │  GET /api/auth/me             │
  │  (cookie sent automatically)  │
  │ ─────────────────────────────>│
  │                               │  5. Extract session from cookie
  │                               │  6. Lookup session in Redis
  │  200 OK { user, workspace }   │
  │ <─────────────────────────────│
```

### Session Storage

- **Primary**: Redis (key: `dataflow:session:<sessionId>`, TTL: 8 hours)
- **Fallback**: In-memory `Map` (used when Redis is unavailable)
- **Cookie**: `dataflow_sid` — HttpOnly, Secure (production), SameSite strict/lax

### LDAP Authentication

When `LDAP_DEMO_MODE=false`, the server authenticates against a real LDAP directory:

1. User provides their "Preferred ID" (username) and password
2. Server binds to LDAP using `ldap-authentication` library
3. LDAP returns user attributes (name, email, `memberOf` groups)
4. Groups are mapped to roles:
   - Member of admin LDAP group → `admin` role
   - Member of developer LDAP group → `developer` role
   - Everyone else → `viewer` role

### Demo Mode

When `LDAP_DEMO_MODE=true` (default in development):
- Accepts `admin`/`admin` credentials
- Returns a mock admin user with full access
- No LDAP server needed

### Sign Out Flow

1. User clicks "Sign Out" in the sidebar
2. Frontend calls `POST /api/auth/logout`
3. Server deletes the session from Redis and clears the cookie
4. Frontend resets auth state → `isAuthenticated=false`
5. App re-renders, showing the login page

---

## 10. API Reference

### Authentication Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/auth/login` | No | Authenticate with username/password |
| `GET` | `/api/auth/me` | Yes | Get current user + workspace |
| `POST` | `/api/auth/logout` | Yes | End session |
| `GET` | `/api/auth/workspaces` | Yes | List available workspaces |
| `POST` | `/api/auth/workspaces` | Yes | Create a new workspace |
| `POST` | `/api/auth/switch-workspace` | Yes | Switch active workspace |

### Entity CRUD Endpoints

All entity endpoints follow the pattern `/api/entities/:entityName`:

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/entities/:name` | List entities (supports `?sort=`, `?limit=`, `?cursor=`) |
| `GET` | `/api/entities/:name/:id` | Get entity by ID |
| `POST` | `/api/entities/:name` | Create entity |
| `PUT` | `/api/entities/:name/:id` | Update entity |
| `DELETE` | `/api/entities/:name/:id` | Delete entity |
| `POST` | `/api/entities/:name/filter` | Filter entities by JSONB fields |
| `POST` | `/api/entities/:name/batch` | Batch create (up to 100 items) |

Valid entity names: `Pipeline`, `Connection`, `PipelineRun`, `ActivityLog`, `AuditLog`, `IngestionJob`, `AirflowDAG`, `CustomFunction`, `ConnectionProfile`, `ConnectionPrerequisite`, `PipelineVersion`, `DataCatalogEntry`, `DagTemplate`

### Keyset Pagination

```
GET /api/entities/Pipeline?cursor=<lastId>&limit=50
→ { items: [...], nextCursor: "abc123", hasMore: true }
```

### Integration Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/test-connection` | Test database connectivity |
| `POST` | `/api/introspect-schema` | Discover tables/columns from a connection |
| `POST` | `/api/validate-spec` | Validate a pipeline spec (YAML/JSON) |
| `POST` | `/api/test-vault` | Test HashiCorp Vault AppRole access |
| `POST` | `/api/gitlab/commit` | Commit files to GitLab repository |
| `POST` | `/api/gitlab/status` | Check GitLab project access |
| `GET` | `/api/gitlab/config` | Get configured GitLab URL/project |
| `GET` | `/api/health` | Health check (status, DB connectivity, uptime) |

### Airflow Proxy Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/airflow/connections` | List Airflow-type connections |
| `GET` | `/api/airflow/:connId/dags` | List DAGs from an Airflow instance |
| `GET` | `/api/airflow/:connId/dags/:dagId/runs` | Get DAG run history |
| `POST` | `/api/airflow/:connId/dags/:dagId/trigger` | Trigger a DAG run |
| `PATCH` | `/api/airflow/:connId/dags/:dagId` | Pause/unpause a DAG |
| `POST` | `/api/airflow/sync-pipeline-status` | Sync pipeline statuses from Airflow |
| `POST` | `/api/airflow/:connId/dags/checkin` | Push DAG file to Airflow folder |
| `POST` | `/api/airflow/pipeline/:id/generate-deploy-package` | Generate full deploy artifact package |

### Admin Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/admin/purge-logs` | Purge old activity logs |
| `GET` | `/api/admin/data-model` | Live database schema introspection |

---

## 11. Pipeline Wizard (End-to-End)

The pipeline wizard is a 6-step process for creating data pipelines. It lives in `src/components/JobFormDialog.jsx`.

### Step 1: Basics (`JobBasicsTab.jsx`)

User configures:
- **Pipeline name** (used to generate the Airflow DAG ID: `dataflow__<sanitized_name>`)
- **Source connection** (where data comes from)
- **Target connection** (where data goes)
- **Metadata**: Assignment group (maps to DAG owner), cost center, notification email
- **Tags**: Freeform labels for grouping/filtering

The connection selector shows platform icons and environment tags (dev/prod/uat as colored pills).

### Step 2: Datasets (`JobDataTab.jsx`)

Behavior adapts based on the source platform:

**For database sources:**
- `ObjectSelector` component connects to the source database and shows available schemas/tables
- User selects tables, each with configurable load method (append, upsert, replace, SCD Type 2)
- Optional: filter query, incremental column

**For flat file sources:**
- Input modes: File List, Folder, Wildcard Pattern
- `SchemaDefinitionPanel` for defining the file structure:
  - CSV: Paste headers or column definitions
  - JSON: Paste JSON schema
  - COBOL: Paste copybook (PIC X/9/S9 COMP-3 parsing)

### Step 3: Schedule (`ScheduleSettings.jsx`)

- **Preset grid**: 8 options (None, Hourly, Daily, Weekly, Monthly, Quarterly, Cron, Sensor)
- **Custom cron**: Human-readable translation shown below the cron input
- **Event sensors**: File watcher, S3 event, database sensor, upstream DAG dependency
- **Calendar timetable**: Include/exclude business day calendars

### Step 4: Advanced (`AdvancedTabContent.jsx`) — Optional

Toggle-able advanced features with a left-sidebar navigation:

| Section | Purpose | Component |
|---|---|---|
| Column Mapping | Map source → target columns with transforms | `ColumnMapper/` |
| Data Cleansing | Auto-fix rules (trim whitespace, null handling, case) | `DataCleansing.jsx` |
| Data Quality | Validation rules (not null, unique, regex, range) | `DataQualityRules.jsx` |
| Security & Masking | PII classification, masking (redact, hash, partial) | `DataMaskingConfig.jsx` |
| SLA Configuration | Arrival time expectations, alerting thresholds | `SLAConfig.jsx` |

### Step 5: Review (`JobSpecTabPreview.jsx`)

Displays:
- **DAG Template selection** (from `DagTemplates.js`)
- **Generated YAML previews** of both artifacts:
  1. Airflow DAG (dag-factory format)
  2. Pipeline Spec (DataFlow internal format)
- **Spec validation results** (errors and warnings)

### Step 6: Deploy (`DeployTabContent.jsx`)

The deploy tab uses a 4-step progress flow: **Save → Generate → Deploy → Done**.

1. **Save**: The pipeline is saved via the `onSavePipeline` callback to ensure it has an ID in the database.
2. **Generate**: Calls `POST /api/airflow/pipeline/:id/generate-deploy-package` to build all deploy artifacts (DAG YAML, Python/PySpark scripts, mapping JSON files, bootstrap loader).
3. **Deploy**: For DAG folder deployment, loops through generated files calling `dags/checkin` per file. For GitLab deployment, bundles all files into a single atomic commit using LDAP credentials.
4. **Done**: Displays a success summary showing the deployed file count and individual file paths.

The full artifact package may include:
- `specs/<pipeline-name>/<name>-airflow-dag.yaml` (DAG definition)
- `specs/<pipeline-name>/<name>-pipelinespec.yaml` (pipeline spec)
- `{pipeline}_tasks.py` (Python sensor + ingestion for flat file sources)
- `{pipeline}_spark.py` (PySpark extraction/transform for RDBMS or advanced features)
- `mappings/{pipeline}_{dataset}_mapping.json` (column mapping per dataset)
- `generate_dags.py` (dag-factory bootstrap loader)

---

## 12. DAG Generation & Deployment

### How DAG YAML is Generated

DAG generation uses a **template-based approach** (`src/components/DagTemplates.js`):

1. **Templates** are YAML strings with `{{placeholders}}`
2. `fillTemplate()` replaces placeholders with values from the pipeline's `formData`
3. Dynamic sections (per-dataset tasks) are generated by functions like `buildDatasetIngestGroup()` and `buildDatasetExtractGroup()`

### Built-in Templates

| Template | Use Case | Operators Used |
|---|---|---|
| Flat File — Landing to Raw | File ingestion with optional sensor | PythonSensor → PythonOperator → (optional) PythonOperator |
| Flat File — Simple Ingest | Basic file copy without sensor | PythonOperator only |
| Database — Extract to DWH | Database-to-database ETL | SparkSubmitOperator per dataset |

### Generated DAG Structure (dag-factory format)

```yaml
<dag_id>:
  default_args:
    owner: <assignment_group>
    start_date: "2024-01-01"
    retries: 3
    retry_delay_sec: 60
  schedule: "@daily"
  description: <pipeline_description>
  tasks:
    sensor_check:
      operator: airflow.sensors.python.PythonSensor
      python_callable: <callable_path>
    ingest_<table_name>:
      operator: airflow.operators.python.PythonOperator
      python_callable: <callable_path>
      dependencies: [sensor_check]
```

### Deployment to GitLab

The deploy flow uses LDAP credentials for GitLab authentication (no stored tokens):

```
Frontend → POST /api/gitlab/commit
  { username, password, branch, message, files: [...] }
    ↓
Server → GitLab OAuth2 Password Grant → Access Token
    ↓
Server → GitLab Commits API → Atomic multi-file commit
    ↓
Response → { success, commit_sha, commit_url }
```

### Deploy Package Generation

The `POST /api/airflow/pipeline/:id/generate-deploy-package` endpoint generates a complete deploy artifact package. It loads the pipeline and its source/target connections from the database, determines the source and target platforms, and calls the appropriate generators to produce:

- `{pipeline}_tasks.py` — Python sensor + ingestion script (flat file sources only)
- `{pipeline}_spark.py` — PySpark extraction/transform script (RDBMS sources or advanced features)
- `mappings/{pipeline}_{dataset}_mapping.json` — Column mapping JSON per dataset
- `generate_dags.py` — dag-factory bootstrap loader deployed to the dags root

**Script selection logic**: RDBMS sources generate a PySpark script. Flat file sources generate a Python tasks script. If any advanced features are enabled (data_cleansing, data_quality, security, column_mappings), a PySpark script is generated regardless of source type.

### Python Tasks Script (`generate-pipeline-tasks.js`)

Generates `{pipeline}_tasks.py` containing:

- **Sensor callables**: `adls_file_exists()`, `s3_file_exists()`, `sftp_file_exists()`, `local_file_exists()`, `nas_file_exists()` — each using appropriate SDKs (Azure SDK, boto3, paramiko, os/pathlib)
- **`ingest_function()`**: Reads flat files (CSV, fixed-width, EBCDIC) and writes to the target platform (local filesystem, ADLS2, S3)
- **Connection configuration**: Retrieved via Airflow Variables or environment variables — credentials are never hardcoded in generated scripts

Export: `generatePythonTasksScript(pipeline, sourceConn, targetConn)` → string

### PySpark Script (`generate-spark-script.js`)

Generates `{pipeline}_spark.py` containing:

- **argparse CLI** for all runtime parameters (connection details, dataset names, paths)
- **JDBC reads** from Oracle, SQL Server, PostgreSQL, MySQL, and MongoDB
- **Target writes**: local parquet files, ADLS2 (`abfss://` protocol), S3 (`s3a://` protocol)
- **Column mapping** from JSON mapping files
- **Data cleansing transforms**: trim, lowercase, uppercase, date_iso, null handling
- **DQ validation**: not_null, unique, range, regex checks with quarantine support for failed records
- **Security masking**: hash, redact, and partial mask operations

Export: `generateSparkScript(pipeline, sourceConn, targetConn, advancedFeatures)` → string

### Bootstrap DAG Loader (`generate-bootstrap.js`)

Generates `generate_dags.py`:

- Uses `dagfactory.load_yaml_dags()` to discover and load all YAML DAG definitions in `dags/{subfolder}/`
- Includes an `ImportError` guard with a helpful install message if dag-factory is not installed
- Deployed to the Airflow dags root folder

Also generates **column mapping JSON files** per dataset, each containing column definitions, DQ rules, and masking configuration.

Exports:
- `generateBootstrapDagLoader(subdir)` → string
- `generateMappingFiles(pipeline, columnMappings)` → array

### Deploy Flow (4-Step Process)

The frontend `DeployTabContent.jsx` uses a 4-step progress indicator: **Save → Generate → Deploy → Done**.

1. **Save**: Pipeline is saved via the `onSavePipeline` callback to ensure it exists in the database with an ID.
2. **Generate**: Calls `POST /api/airflow/pipeline/:id/generate-deploy-package` to build all artifacts (DAG YAML, scripts, mapping files, bootstrap loader).
3. **Deploy**: For DAG folder deployment, loops through each generated file calling `dags/checkin` per file. For GitLab deployment, bundles all files into a single atomic commit.
4. **Done**: Displays a success summary with the deployed file count and individual file paths.

### dag-factory Prerequisite

The Airflow environment must have `dag-factory>=1.0.0` installed for the generated DAGs to load. The bootstrap `generate_dags.py` is deployed to the dags root folder where it discovers and loads all YAML DAG definitions.

```bash
pip install dag-factory>=1.0.0
```

### Dagbag Test Suite

The test suite at `tests/airflow_dagbag/test_dagbag_load.py` contains 10 tests (141 assertions) covering:

- YAML parsing and structure validation
- DAG building from dag-factory YAML
- Bootstrap loader (`generate_dags.py`) loading and execution
- Advanced features YAML generation (data cleansing, DQ, masking)
- Realistic Python tasks script stubs
- Realistic PySpark script stubs
- Column mapping JSON validation

---

## 13. External Integrations

### GitLab (LDAP Deploy Target)

**Purpose**: Commit generated DAG and spec files to a Git repository for CI/CD.

**How it works**:
1. User enters their LDAP credentials in the Deploy tab
2. Server authenticates via GitLab's OAuth2 password grant (LDAP passthrough)
3. No tokens are stored — authentication is per-commit
4. Files are pushed via GitLab's Commits API

**Configuration**:
```env
GITLAB_URL=https://gitlab.your-company.com
GITLAB_PROJECT=namespace/project-name
```

### Airflow

**Purpose**: Monitor and control Airflow DAGs from within DataFlow.

**How it works**:
- DataFlow proxies requests to Airflow's REST API through `/api/airflow/*` endpoints
- Credentials are stored in Connection entities (type: `airflow`) — never exposed to the frontend
- **Status sync**: Every 2 minutes, `useAirflowStatusSync` polls Airflow connections to update pipeline statuses

**Features**:
- View DAG list, run history, task instances, and logs
- Trigger DAG runs and pause/unpause DAGs
- Push DAG files directly to Airflow's dags folder (admin only)

### HashiCorp Vault

**Purpose**: Securely retrieve database credentials at connection test/use time.

**How it works**:
1. Connection is configured with `auth_method: "vault_credentials"`
2. Vault config stored per connection: URL, namespace, role_id, secret_id, mount_point, secret_path
3. At test time, server authenticates to Vault via AppRole → retrieves credentials → connects to database

---

## 14. Feature Flags & Navigation

### How Feature Flags Work

```
dataflow.yaml (navigation + features sections)
       ↓
src/dataflow-config.js (merges YAML + env overrides)
       ↓
src/feature-flags.js (derives { enabled, comingSoon } per flag)
       ↓
Layout.jsx (shows/hides/dims sidebar items)
Components (conditionally render features)
```

### Available Flags

| Flag | Controls |
|---|---|
| `dashboard` | Dashboard page visibility |
| `connections` | Connections page visibility |
| `pipelines` | Pipelines page visibility |
| `dataCatalog` | Data Catalog page (coming soon) |
| `userGuide` | User Guide page (coming soon) |
| `dataModel` | Admin: Data Model page |
| `auditTrail` | Admin: Audit Trail page |
| `activityLogs` | Admin: Activity Logs page |
| `airflow` | Admin: Airflow page |
| `customFunctions` | Admin: Custom Functions page |
| `vaultCredentials` | Vault credential support in connections |
| `gitlabDeploy` | GitLab deployment in pipeline wizard |
| `schemaIntrospection` | Live schema discovery from connections |
| `connectionTesting` | Real database connection testing |
| `dagCheckin` | Admin DAG file push to Airflow |
| `specValidation` | Pipeline spec validation |
| `airflowStatusSync` | Automatic Airflow status polling |

### Modifying Flags

To disable a feature, edit `dataflow.yaml`:
```yaml
features:
  vault_credentials: { enabled: false }
```

To mark something as "Coming Soon":
```yaml
navigation:
  main:
    data_catalog: { enabled: true, coming_soon: true }
```

---

## 15. Styling & Theming

### Design System

- **Primary Color**: US Bank blue `#0060AF` (hover: `#004d8c`)
- **Orange**: Reserved exclusively for the GitLab SVG brand icon
- **Dark Mode**: Full support via Tailwind's `dark:` prefix

### How Dark Mode Works

1. User toggles dark mode in the sidebar
2. `Layout.jsx` adds/removes `class="dark"` on `<html>`
3. Tailwind applies `dark:` variants for all styled elements
4. Preference is saved in `localStorage` ("dataflow-dark")

### Adding Styles to New Components

Every component uses Tailwind utility classes. Always include `dark:` variants:

```jsx
// Good — supports both modes
<div className="bg-white dark:bg-slate-800 text-slate-900 dark:text-white">

// Bad — breaks in dark mode
<div className="bg-white text-slate-900">
```

### CSS Variables (HSL-based)

The theme uses CSS variables for dynamic colors. These are defined in `src/index.css`:

```css
:root {
  --background: 0 0% 100%;        /* white */
  --foreground: 222 84% 5%;       /* near-black */
  --sidebar-background: 222 47% 15%;  /* dark blue-gray */
  /* ... */
}
.dark {
  --background: 222 84% 5%;       /* near-black */
  --foreground: 210 40% 98%;      /* near-white */
  /* ... */
}
```

### shadcn/ui Components

Low-level UI primitives (buttons, dialogs, dropdowns, etc.) are in `src/components/ui/`. These are generated by shadcn/ui and follow Radix UI patterns.

To use a component:
```jsx
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
```

---

## 16. Environment Variables

### Complete Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `DB_SSL` | No | `true` | Database SSL (`false` to disable) |
| `DB_SSL_REJECT_UNAUTHORIZED` | No | `true` | Reject self-signed certs |
| `DB_POOL_MIN` | No | `2` (dev) / `5` (prod) | Min DB connection pool size |
| `DB_POOL_MAX` | No | `10` (dev) / `20` (prod) | Max DB connection pool size |
| `DB_IDLE_TIMEOUT_MS` | No | `30000` | Idle connection timeout |
| `DB_CONNECTION_TIMEOUT_MS` | No | `5000` | Connection attempt timeout |
| `REDIS_URL` | No | `redis://localhost:6379` | Redis connection URL |
| `REDIS_PREFIX` | No | `dataflow:` | Redis key prefix |
| `PORT` | No | `5000` | Server port |
| `HOST` | No | `0.0.0.0` | Server bind address |
| `NODE_ENV` | No | `development` | Environment (`development` or `production`) |
| `CORS_ORIGIN` | No | `*` | Allowed CORS origins |
| `LOG_LEVEL` | No | `debug` (dev) / `warn` (prod) | Pino log level |
| `LOG_REQUESTS` | No | `true` (dev) / `false` (prod) | Log HTTP requests |
| `LDAP_DEMO_MODE` | No | `true` (dev) | Enable demo auth (admin/admin) |
| `LDAP_URI` | If LDAP | — | LDAP server URI (e.g., `ldaps://...`) |
| `LDAP_BASEDN` | If LDAP | — | LDAP base DN |
| `LDAP_DOMAIN` | If LDAP | — | LDAP domain |
| `LDAP_ROLES_MAPPING_ADMIN` | No | — | LDAP group DN for admin role |
| `LDAP_ROLES_MAPPING_DEV` | No | — | LDAP group DN for developer role |
| `LDAP_TLS_REJECT_UNAUTHORIZED` | No | `false` | Reject self-signed LDAP certs |
| `LDAP_SESSION_TTL_HOURS` | No | `8` | Session duration in hours |
| `GITLAB_URL` | If GitLab | — | GitLab server URL |
| `GITLAB_PROJECT` | If GitLab | — | GitLab project path |
| `VAULT_ADDR` | If Vault | — | HashiCorp Vault address |
| `VAULT_NAMESPACE` | No | — | Vault namespace |
| `VAULT_DEFAULT_MOUNT` | No | `secret` | Vault KV mount |
| `AIRFLOW_DAGS_FOLDER` | No | `/opt/airflow/dags` | Airflow DAGs directory |
| `AIRFLOW_API_TIMEOUT_MS` | No | `15000` | Airflow API request timeout |
| `RATE_LIMIT_MAX` | No | `1000` | API read requests per 15 min |
| `RATE_LIMIT_WRITE_MAX` | No | `200` | API write requests per 15 min |
| `API_BODY_LIMIT` | No | `10mb` | Max request body size |
| `DAG_CALLABLE_BASE_PATH` | No | `/data/dags/` | Base path for DAG callables |
| `VITE_DATAFLOW_ENV` | No | — | Frontend env override (dev/it/uat/production) |

---

## 17. Docker & Deployment

### Docker Build

The `Dockerfile` uses a multi-stage build:

```
Stage 1 (builder):
  node:20-alpine → npm ci → npm run build → dist/

Stage 2 (production):
  node:20-alpine → npm ci --omit=dev → copy dist/ + server/ + config/
  → EXPOSE 5000 → node server/production.js
```

### Docker Compose (Full Stack)

```bash
# Start everything (app + PostgreSQL + Redis)
cp .env.example .env    # Edit values first
docker compose up -d

# View logs
docker compose logs -f app

# Stop
docker compose down
```

The compose file runs:
- **PostgreSQL 16** on port 5432 with persistent volume
- **Redis 7** on port 6379 with persistent volume (256MB max, LRU eviction)
- **DataFlow app** on port 5000, waits for DB+Redis health checks

### AKS / Kubernetes Deployment

For Azure Kubernetes Service:
1. Build and push the Docker image to your container registry
2. Create a Kubernetes deployment + service using the image
3. Set all env vars via ConfigMap/Secrets
4. Point `DATABASE_URL` to your managed PostgreSQL instance
5. Point `REDIS_URL` to your managed Redis instance
6. Configure an Ingress for external access

### Local PC Deployment

```bash
# Option 1: Docker Compose (recommended)
docker compose up -d

# Option 2: Native Node.js
npm install
npm run build
export DATABASE_URL=postgresql://user:pass@localhost:5432/dataflow
export NODE_ENV=production
npm start
```

### Health Check

The production server exposes `GET /api/health`:

```json
{
  "status": "ok",
  "uptime": 3600,
  "database": "connected",
  "redis": { "status": "connected" },
  "dbReady": true,
  "timestamp": "2026-02-28T14:48:21.428Z"
}
```

Returns **503** if the database is down.

---

## 18. Common Development Tasks

### Adding a New Page

1. **Create the page component** in `src/pages/NewPage.jsx`:
   ```jsx
   export default function NewPage() {
     return <div>New Page Content</div>;
   }
   ```

2. **Register it** in `src/pages.config.js`:
   ```javascript
   const NewPage = lazy(() => import('./pages/NewPage'));
   // Add to PAGES object:
   "NewPage": NewPage,
   ```

3. **Add to sidebar** in `dataflow.yaml`:
   ```yaml
   navigation:
     main:
       new_page: { enabled: true, coming_soon: false }
   ```

4. **Add the nav item** in `src/Layout.jsx`:
   ```javascript
   const navItems = [
     // ...existing items
     { name: "New Page", icon: SomeIcon, page: "NewPage", flag: "newPage" },
   ];
   ```

5. **Add the feature flag** in `src/feature-flags.js`:
   ```javascript
   newPage: toFlag(nav.main?.new_page),
   ```

### Adding a New Entity Type

1. **Add the table name** to the entity allowlist in `server/db.js` (look for `entityNameToTable`):
   ```javascript
   case 'NewEntity': return 'new_entity';
   ```

2. **Add table creation** in the `initializeDatabase()` function in `server/db.js` (follow existing pattern)

3. **Use from frontend**:
   ```javascript
   const items = await dataflow.entities.NewEntity.list();
   ```

### Adding a New Connection Platform

1. **Define it** in `config/platforms.yaml`:
   ```yaml
   snowflake:
     label: Snowflake
     category: database
     port: 443
     ssl: true
   ```

2. **Add icon + colors** in `src/components/PlatformIcon.jsx`:
   ```javascript
   snowflake: { icon: Database, color: "bg-sky-100 text-sky-600", borderColor: "border-l-sky-500", label: "Snowflake" },
   ```

3. **Add connection testing** in `server/test-connection.js` (follow the `case` pattern)

### Adding a New API Endpoint

1. **Add the route** in `server/middleware.js` (inside `createApiMiddleware`):
   ```javascript
   router.post('/api/my-endpoint', requireWorkspace, async (req, res) => {
     // handler
   });
   ```

2. **Add validation** in `server/validation.js`:
   ```javascript
   export const myEndpointSchema = z.object({ ... });
   ```

3. **Apply to route**:
   ```javascript
   router.post('/api/my-endpoint', validateBody(myEndpointSchema), requireWorkspace, handler);
   ```

4. **Add the same route** in `server/production.js` (production server has its own route definitions)

### Modifying the Pipeline Wizard

- **Add a new step**: Edit the `steps` array in `JobFormDialog.jsx` and create a new tab component in `src/components/JobFormTabs/`
- **Add a new advanced feature**: Edit `config/pipeline-wizard.yaml` under `features.advanced`, then add the component to `AdvancedTabContent.jsx`
- **Add a new DAG template**: Add the template string to `src/components/DagTemplates.js`

### Running the Linter

```bash
npm run lint          # Show errors
npm run lint:fix      # Auto-fix what's possible
```

---

## 19. Troubleshooting

### Common Issues

**"Auth failed" / 401 errors on every request**
- Check if the session cookie is being sent. The `dataflow_sid` cookie must be HttpOnly and sent with `credentials: 'include'`.
- In development, verify LDAP demo mode is active: `LDAP_DEMO_MODE=true`
- If Redis is down, sessions fall back to in-memory — a server restart will clear all sessions.

**Database tables not created**
- Tables are auto-created on server startup by `initializeDatabase()` in `server/db.js`.
- Check the server logs for `"database ready"` or any PostgreSQL connection errors.
- Verify `DATABASE_URL` is set and the database exists.

**"ECONNREFUSED" on database connection**
- Verify PostgreSQL is running: `pg_isready -h localhost -p 5432`
- Check the connection string format: `postgresql://user:password@host:port/dbname`

**Vite HMR not working / "server connection lost"**
- This usually means the Vite server restarted (e.g., config change). It reconnects automatically.
- If persistent, check for syntax errors in any recently edited file.

**Redis connection failed (non-blocking)**
- Redis is optional. The server logs `"redis connected"` or falls back silently.
- To run without Redis: simply don't set `REDIS_URL`.
- In-memory sessions are lost on server restart.

**Blank page after login**
- Open browser console (F12) and check for JavaScript errors.
- Verify the API is responding: `curl http://localhost:5000/api/health`
- Check if `AuthContext.checkAppState()` successfully calls `/api/auth/me`.

**GitLab deploy fails**
- Verify `GITLAB_URL` and `GITLAB_PROJECT` environment variables
- Ensure the user has write access to the GitLab project
- LDAP credentials must be valid for GitLab's OAuth password grant

**"Deploy package generation fails"**
- Verify the pipeline has been saved (has an ID). Pipeline must exist in the database before generating deploy package.
- Check that source and target connections exist and are referenced correctly.
- For flat file pipelines, verify source connection platform is one of: `flat_file_delimited`, `flat_file_fixed_width`, `cobol_ebcdic`, `sftp`, `nas`, `local_fs`.

**"Entity not found" / unknown entity name**
- Entity names are case-sensitive in the API: `Pipeline` not `pipeline`
- Check the allowlist in `server/db.js` (`entityNameToTable` function)

### Viewing Logs

**Development**: Logs appear in the terminal running `npm run dev`. They are JSON-formatted (Pino).

**Production**: Use `docker compose logs -f app` or check stdout from the Node.js process.

**Key log fields to look for**:
- `"msg": "request server error"` — 5xx errors (bugs)
- `"msg": "request client error"` — 4xx errors (bad input, auth failures)
- `"msg": "user authenticated"` — successful logins
- `"msg": "database ready"` — DB initialization complete

### Performance Debugging

- Check DB pool utilization: Look for connection timeout errors in logs
- Check query performance: Enable `LOG_LEVEL=debug` to see all request durations
- Check Redis: Use `redis-cli monitor` to see session lookups
- Check rate limiting: 429 responses indicate rate limit hits (default: 1000 reads / 200 writes per 15 min)

---

## Appendix: Key Design Decisions

| Decision | Rationale |
|---|---|
| JSONB document storage | Flexibility to evolve entity schemas without migrations |
| Workspace-based multi-tenancy | Airbyte-style isolation for teams/departments |
| HttpOnly cookies for sessions | Prevents XSS-based session theft |
| LDAP credentials per-commit (no stored tokens) | Security — no long-lived tokens to rotate or leak |
| dag-factory YAML format | Industry-standard format compatible with Airflow's dag-factory plugin |
| Config-driven UI (YAML) | Non-developers can toggle features without code changes |
| Redis with in-memory fallback | Graceful degradation — works without Redis for development |
| No connection_type field | Any connection can serve as source or target |
| Case-insensitive duplicate prevention | `LOWER(data->>'name')` prevents "Finance ETL" and "finance etl" from coexisting |
