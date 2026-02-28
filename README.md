# DataFlow - Data Connector Platform

A full-stack application for managing data pipelines, connections, and ETL workflows. Built with React, Express.js, and PostgreSQL.

## Features

- **Pipeline Management** — Create, configure, run, and monitor data pipelines
- **Connection Management** — Manage data sources and targets with connection profiles
- **Activity Logging** — Track all operations with detailed activity and audit logs
- **Airflow Integration** — View and manage Airflow DAGs
- **Data Catalog** — Browse and search your data assets
- **Visual Pipeline Builder** — Drag-and-drop pipeline construction
- **Version History** — Track pipeline configuration changes over time

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, Vite 6, Tailwind CSS, Radix UI (shadcn/ui) |
| Routing | React Router DOM v6 |
| State | TanStack React Query |
| Backend | Express.js |
| Database | PostgreSQL with JSONB storage |
| Language | JavaScript (ESM) |

## Project Structure

```
dataflow/
├── config/
│   ├── index.js              # Config loader (auto-selects by NODE_ENV)
│   ├── development.js        # Development configuration
│   └── production.js         # Production configuration
├── server/
│   ├── db.js                 # Database connection pool and initialization
│   ├── middleware.js          # Express API middleware (for Vite dev server)
│   └── production.js         # Standalone production server
├── src/
│   ├── api/client.js         # SDK client instance
│   ├── components/           # Reusable UI components
│   ├── hooks/                # Custom React hooks
│   ├── lib/
│   │   ├── local-sdk.js      # Local SDK adapter (entity CRUD, auth, functions)
│   │   ├── AuthContext.jsx   # Authentication context provider
│   │   └── NavigationTracker.jsx
│   ├── pages/                # Route-level page components
│   └── utils/                # Shared utilities
├── public/                   # Static assets
├── .env.development.example  # Dev environment template
├── .env.production.example   # Prod environment template
└── package.json
```

---

## Local Development Setup

### Prerequisites

- **Node.js** 18+ (20 recommended)
- **PostgreSQL** 14+ (local install, Docker, or managed service)
- **npm** 9+

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/dataflow.git
cd dataflow
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Configure Environment

Copy the development environment template and fill in your database credentials:

```bash
cp .env.development.example .env
```

Edit `.env`:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/dataflow
DB_SSL=false
PORT=5000
LOG_LEVEL=debug
```

**Using Docker for PostgreSQL (optional):**

```bash
docker run -d \
  --name dataflow-postgres \
  -e POSTGRES_USER=dataflow \
  -e POSTGRES_PASSWORD=dataflow \
  -e POSTGRES_DB=dataflow \
  -p 5432:5432 \
  postgres:16

# Then set in .env:
# DATABASE_URL=postgresql://dataflow:dataflow@localhost:5432/dataflow
```

### 4. Start the Development Server

```bash
npm run dev
```

The app starts at `http://localhost:5000` with:
- Vite dev server serving the React frontend with hot module replacement
- Express API middleware handling all `/api/*` routes
- Database tables auto-created on first startup

### 5. Verify

Open `http://localhost:5000` in your browser. You should see the DataFlow dashboard.

Test the API directly:

```bash
curl http://localhost:5000/api/health
# {"status":"ok","dbReady":true}

curl http://localhost:5000/api/auth/me
# {"id":"1","email":"user@local","name":"Local User","role":"admin","is_authenticated":true}
```

---

## Production Deployment

### Option A: Quick Deploy

1. Set the `DATABASE_URL` environment variable to your PostgreSQL connection string
2. Build and deploy:

```bash
npm run build
npm start
```

### Option B: Self-Hosted / VPS

#### 1. Provision Infrastructure

- **Server**: Any Linux VPS (Ubuntu 22.04+ recommended) with Node.js 18+
- **Database**: Managed PostgreSQL (Neon, Supabase, AWS RDS, Render) or self-hosted

#### 2. Clone and Install

```bash
git clone https://github.com/your-org/dataflow.git
cd dataflow
npm ci --production
```

#### 3. Configure Production Environment

```bash
cp .env.production.example .env
```

Edit `.env` with your production values:

```env
DATABASE_URL=postgresql://user:password@your-db-host:5432/dataflow?sslmode=require
PORT=5000
CORS_ORIGIN=https://your-domain.com
LOG_LEVEL=warn
NODE_ENV=production
```

#### 4. Build the Frontend

```bash
npm run build
```

This creates an optimized production build in the `dist/` directory.

#### 5. Start the Production Server

```bash
npm start
```

The production server:
- Serves the built React frontend as static files
- Handles all API routes
- Auto-initializes database tables on first start

#### 6. Process Manager (Recommended)

Use PM2 or systemd to keep the server running:

**PM2:**

```bash
npm install -g pm2
pm2 start npm --name "dataflow" -- start
pm2 save
pm2 startup
```

**Systemd:**

```ini
# /etc/systemd/system/dataflow.service
[Unit]
Description=DataFlow Data Connector Platform
After=network.target

[Service]
Type=simple
User=dataflow
WorkingDirectory=/opt/dataflow
ExecStart=/usr/bin/npm start
Restart=on-failure
RestartSec=10
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable dataflow
sudo systemctl start dataflow
```

### Option C: Docker

```dockerfile
# Dockerfile
FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --production=false
COPY . .
RUN npm run build

FROM node:20-alpine
WORKDIR /app
RUN apk add --no-cache dumb-init
COPY package.json package-lock.json ./
RUN npm ci --production
COPY --from=builder /app/dist ./dist
COPY server/ ./server/
COPY config/ ./config/
ENV NODE_ENV=production
USER node
EXPOSE 5000
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "server/production.js"]
```

```bash
docker build -t dataflow .
docker run -d \
  -p 5000:5000 \
  -e DATABASE_URL="postgresql://user:pass@host:5432/dataflow" \
  -e NODE_ENV=production \
  --name dataflow \
  dataflow
```

### Option D: Docker Compose (with PostgreSQL)

```yaml
# docker-compose.yml
services:
  app:
    build: .
    ports:
      - "5000:5000"
    environment:
      DATABASE_URL: postgresql://dataflow:dataflow@db:5432/dataflow
      NODE_ENV: production
      DB_SSL: "false"
    depends_on:
      db:
        condition: service_healthy

  db:
    image: postgres:16
    environment:
      POSTGRES_USER: dataflow
      POSTGRES_PASSWORD: dataflow
      POSTGRES_DB: dataflow
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dataflow"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
```

```bash
docker compose up -d
```

### Option E: Azure Kubernetes Service (AKS)

This section covers deploying DataFlow to Azure Kubernetes Service with Azure Database for PostgreSQL.

#### Prerequisites

- **Azure CLI** (`az`) installed and authenticated
- **kubectl** configured
- **Docker** for building container images
- **Azure Container Registry (ACR)** or another container registry
- **Helm** (optional, for ingress controller)

#### 1. Provision Azure Resources

**Create a Resource Group:**

```bash
az group create --name dataflow-rg --location eastus
```

**Create an Azure Container Registry:**

```bash
az acr create --resource-group dataflow-rg --name dataflowacr --sku Basic
az acr login --name dataflowacr
```

**Create Azure Database for PostgreSQL Flexible Server:**

```bash
az postgres flexible-server create \
  --resource-group dataflow-rg \
  --name dataflow-pgdb \
  --location eastus \
  --admin-user dataflowadmin \
  --admin-password '<strong-password>' \
  --sku-name Standard_B1ms \
  --tier Burstable \
  --storage-size 32 \
  --version 16 \
  --yes

az postgres flexible-server db create \
  --resource-group dataflow-rg \
  --server-name dataflow-pgdb \
  --database-name dataflow
```

**Allow AKS to reach the database** (choose one approach):

```bash
# Option A: Allow Azure services (simpler, less restrictive)
az postgres flexible-server firewall-rule create \
  --resource-group dataflow-rg \
  --name dataflow-pgdb \
  --rule-name allow-azure \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0

# Option B: Private Endpoint (recommended for production)
# Place the PostgreSQL server and AKS cluster in the same VNet
# or use VNet peering with a Private Endpoint for the database.
# See: https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-networking-private
```

**Create an AKS Cluster:**

```bash
az aks create \
  --resource-group dataflow-rg \
  --name dataflow-aks \
  --node-count 2 \
  --node-vm-size Standard_B2s \
  --generate-ssh-keys \
  --attach-acr dataflowacr

az aks get-credentials --resource-group dataflow-rg --name dataflow-aks
```

> If the AKS cluster already exists, attach ACR separately:
> `az aks update --resource-group dataflow-rg --name dataflow-aks --attach-acr dataflowacr`

#### 2. Build and Push the Container Image

```bash
# Use a specific tag (commit SHA or version) for reproducibility
docker build -t dataflowacr.azurecr.io/dataflow:v1.0.0 .
docker push dataflowacr.azurecr.io/dataflow:v1.0.0
```

#### 3. Create Kubernetes Secret for Database Credentials

```bash
kubectl create namespace dataflow

kubectl create secret generic dataflow-db \
  --namespace dataflow \
  --from-literal=DATABASE_URL="postgresql://dataflowadmin:<password>@dataflow-pgdb.postgres.database.azure.com:5432/dataflow?sslmode=require"
```

#### 4. Kubernetes Deployment Manifest

Create a file `k8s/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dataflow
  namespace: dataflow
  labels:
    app: dataflow
spec:
  replicas: 2
  selector:
    matchLabels:
      app: dataflow
  template:
    metadata:
      labels:
        app: dataflow
    spec:
      containers:
        - name: dataflow
          image: dataflowacr.azurecr.io/dataflow:v1.0.0
          ports:
            - containerPort: 5000
          env:
            - name: NODE_ENV
              value: "production"
            - name: PORT
              value: "5000"
            - name: LOG_LEVEL
              value: "warn"
            - name: CORS_ORIGIN
              value: "https://dataflow.yourdomain.com"
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: dataflow-db
                  key: DATABASE_URL
          resources:
            requests:
              cpu: "250m"
              memory: "256Mi"
            limits:
              cpu: "500m"
              memory: "512Mi"
          readinessProbe:
            httpGet:
              path: /api/health
              port: 5000
            initialDelaySeconds: 10
            periodSeconds: 15
          livenessProbe:
            httpGet:
              path: /api/health
              port: 5000
            initialDelaySeconds: 20
            periodSeconds: 30
---
apiVersion: v1
kind: Service
metadata:
  name: dataflow
  namespace: dataflow
spec:
  type: ClusterIP
  selector:
    app: dataflow
  ports:
    - port: 80
      targetPort: 5000
      protocol: TCP
```

#### 5. Ingress Configuration (NGINX Ingress Controller)

Install the NGINX ingress controller and cert-manager for TLS:

```bash
# NGINX Ingress Controller
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx --create-namespace \
  --set controller.service.annotations."service\.beta\.kubernetes\.io/azure-load-balancer-health-probe-request-path"=/healthz

# cert-manager for automatic TLS certificates
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true

# Create a ClusterIssuer for Let's Encrypt
kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: your-email@example.com
    privateKeySecretRef:
      name: letsencrypt-prod
    solvers:
      - http01:
          ingress:
            class: nginx
EOF
```

Create `k8s/ingress.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: dataflow-ingress
  namespace: dataflow
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "50m"
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - dataflow.yourdomain.com
      secretName: dataflow-tls
  rules:
    - host: dataflow.yourdomain.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: dataflow
                port:
                  number: 80
```

#### 6. Deploy to AKS

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/ingress.yaml

kubectl get pods -n dataflow
kubectl get svc -n dataflow
```

#### 7. Horizontal Pod Autoscaler (Optional)

Scale based on CPU usage:

```yaml
# k8s/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: dataflow-hpa
  namespace: dataflow
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: dataflow
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

```bash
kubectl apply -f k8s/hpa.yaml
```

#### 8. CI/CD with Azure DevOps or GitHub Actions

**GitHub Actions example** (`.github/workflows/deploy.yml`):

```yaml
name: Deploy to AKS
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: azure/login@v2
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Build and push to ACR
        run: |
          az acr login --name dataflowacr
          docker build -t dataflowacr.azurecr.io/dataflow:${{ github.sha }} .
          docker push dataflowacr.azurecr.io/dataflow:${{ github.sha }}

      - uses: azure/aks-set-context@v4
        with:
          resource-group: dataflow-rg
          cluster-name: dataflow-aks

      - name: Deploy
        run: |
          kubectl set image deployment/dataflow \
            dataflow=dataflowacr.azurecr.io/dataflow:${{ github.sha }} \
            -n dataflow
          kubectl rollout status deployment/dataflow -n dataflow
```

#### AKS Deployment Tips

- **Database networking**: Allow AKS subnet access to PostgreSQL via Azure networking rules or Private Endpoints for production workloads
- **Managed Identity**: Use Azure Workload Identity instead of connection string secrets for PostgreSQL authentication in production
- **TLS certificates**: Use cert-manager with Let's Encrypt for automatic HTTPS certificate provisioning
- **Monitoring**: Enable Azure Monitor Container Insights on the AKS cluster for log aggregation and metrics
- **Node pools**: Use a dedicated user node pool (separate from system) for application workloads
- **Resource quotas**: Set namespace resource quotas to prevent runaway resource consumption
- **Pod Disruption Budgets**: Add a PDB with `minAvailable: 1` to maintain availability during node upgrades
- **Image tagging**: Always use specific image tags (commit SHA or semver) instead of `latest` in production

### Option F: Generic Kubernetes (Any Cloud / On-Prem)

This section covers deploying DataFlow to any Kubernetes cluster (EKS, GKE, AKS, on-prem, k3s, etc.) using vendor-neutral manifests.

#### Architecture Overview

```
                    +---------------------+
                    |   Ingress (nginx)   |
                    |   TLS termination   |
                    +----------+----------+
                               |
              +----------------+----------------+
              |                                 |
     +--------v--------+          +-------------v-----------+
     |  Frontend (CDN)  |          |   API Deployment        |
     |  Static assets   |          |   2-10 replicas (HPA)   |
     |  from dist/      |          |   Express.js + Node 20  |
     +-----------------+          +-------------+-----------+
                                                |
                                   +------------v-----------+
                                   |   PostgreSQL           |
                                   |   (managed: RDS /      |
                                   |    Cloud SQL /          |
                                   |    Azure DB)            |
                                   +------------+-----------+
                                                |
                                   +------------v-----------+
                                   |   PgBouncer (optional)  |
                                   |   Connection pooling    |
                                   +------------------------+
```

#### 1. Deployment Manifest

Create `k8s/generic/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dataflow-api
  labels:
    app: dataflow
    component: api
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 1
  selector:
    matchLabels:
      app: dataflow
  template:
    metadata:
      labels:
        app: dataflow
        component: api
    spec:
      terminationGracePeriodSeconds: 30
      containers:
      - name: dataflow
        image: your-registry/dataflow-platform:latest
        ports:
        - containerPort: 5000
          name: http
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: dataflow-secrets
              key: database-url
        - name: NODE_ENV
          value: "production"
        - name: PORT
          value: "5000"
        - name: RATE_LIMIT_MAX
          value: "1000"
        - name: RATE_LIMIT_WRITE_MAX
          value: "200"
        resources:
          requests:
            cpu: 250m
            memory: 256Mi
          limits:
            cpu: 1000m
            memory: 512Mi
        readinessProbe:
          httpGet:
            path: /api/health
            port: 5000
          initialDelaySeconds: 10
          periodSeconds: 15
          successThreshold: 1
          failureThreshold: 3
        livenessProbe:
          httpGet:
            path: /api/health
            port: 5000
          initialDelaySeconds: 30
          periodSeconds: 30
          failureThreshold: 3
        startupProbe:
          httpGet:
            path: /api/health
            port: 5000
          failureThreshold: 10
          periodSeconds: 5
```

#### 2. Service + Ingress

```yaml
apiVersion: v1
kind: Service
metadata:
  name: dataflow-service
spec:
  selector:
    app: dataflow
  ports:
  - port: 80
    targetPort: 5000
    protocol: TCP
  type: ClusterIP
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: dataflow-ingress
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "10m"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  ingressClassName: nginx
  tls:
  - hosts:
    - dataflow.yourcompany.com
    secretName: dataflow-tls
  rules:
  - host: dataflow.yourcompany.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: dataflow-service
            port:
              number: 80
```

#### 3. Horizontal Pod Autoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: dataflow-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: dataflow-api
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 25
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
```

#### 4. Pod Disruption Budget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: dataflow-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: dataflow
```

#### 5. PgBouncer Sidecar (Recommended for Multi-Replica)

When running multiple API replicas, each with a pool of up to 20 database connections, you can exhaust PostgreSQL's `max_connections`. PgBouncer acts as a connection multiplexer.

```yaml
# Add as a sidecar container in the Deployment spec
containers:
- name: pgbouncer
  image: edoburu/pgbouncer:1.22.0
  ports:
  - containerPort: 6432
  env:
  - name: DATABASE_URL
    valueFrom:
      secretKeyRef:
        name: dataflow-secrets
        key: database-url
  - name: POOL_MODE
    value: "transaction"
  - name: DEFAULT_POOL_SIZE
    value: "20"
  - name: MAX_CLIENT_CONN
    value: "100"
  - name: MAX_DB_CONNECTIONS
    value: "10"
  resources:
    requests:
      cpu: 50m
      memory: 64Mi
    limits:
      cpu: 200m
      memory: 128Mi
  livenessProbe:
    tcpSocket:
      port: 6432
    periodSeconds: 30
```

When using PgBouncer sidecar, update the app container's `DATABASE_URL` to point to `localhost:6432` instead of the remote database host.

#### 6. Database Connection Sizing Guide

| Pod Replicas | Pool Max (per pod) | Total DB Connections | PgBouncer Recommended? |
|:---:|:---:|:---:|:---:|
| 1-2 | 20 | 20-40 | No |
| 3-5 | 20 | 60-100 | Yes |
| 5-10 | 20 | 100-200 | Required |
| 10+ | 10 | 100+ | Required + dedicated PgBouncer deployment |

Ensure your managed PostgreSQL instance's `max_connections` exceeds the total connection count above by at least 20% for admin/monitoring connections.

#### Kubernetes Deployment Tips

- **Database**: Use a managed PostgreSQL service (AWS RDS, GCP Cloud SQL, Azure Database, Neon, Supabase) rather than running Postgres inside K8s
- **Autovacuum**: Tune `autovacuum_vacuum_cost_delay` and `autovacuum_vacuum_scale_factor` for high-update tables (`pipeline_run`, `activity_log`)
- **Image tags**: Always use specific tags (commit SHA or semver) instead of `latest` in production
- **Resource quotas**: Set namespace resource quotas to prevent runaway resource consumption
- **Monitoring**: Deploy Prometheus + Grafana or use cloud-native monitoring (CloudWatch, Cloud Monitoring, Azure Monitor)
- **Node pools**: Use a dedicated application node pool separate from system components
- **Network policies**: Restrict pod-to-pod traffic to only necessary paths

---

## Performance Characteristics

### Server Middleware Stack

The production server includes the following performance and security middleware:

| Middleware | Purpose |
|-----------|---------|
| `compression` | Gzip/Brotli response compression (60-80% size reduction) |
| `helmet` | Security headers (X-Content-Type-Options, X-Frame-Options, HSTS, etc.) |
| `express-rate-limit` | API rate limiting (1000 reads / 200 writes per 15 min per IP) |

### Database Indexing

On startup, the server automatically creates these indexes:

| Index Type | Tables | Purpose |
|-----------|--------|---------|
| B-tree on `created_date DESC` | All entity tables | Fast default sorting |
| B-tree on `updated_date DESC` | All entity tables | Fast update-time sorting |
| GIN on `data` (JSONB) | All entity tables | Fast key-existence and containment queries |
| Expression on `data->>'status'` | pipeline, connection, pipeline_run, ingestion_job | Fast status filtering |
| Expression on `data->>'name'` | pipeline, connection, pipeline_run, ingestion_job | Fast name lookups |
| GIN on `to_tsvector(data::text)` | pipeline, connection, activity_log | Full-text search |

### Search Performance

Full-text search uses PostgreSQL's `to_tsvector` / `plainto_tsquery` instead of regex-based `data::text ~*` matching. This leverages GIN indexes for sub-millisecond search at scale.

### Frontend Code Splitting

All route-level pages are lazy-loaded via `React.lazy()` and wrapped in `Suspense`. This means the initial JavaScript bundle only includes the shell/layout, and each page is loaded on-demand when navigated to.

### Rate Limiting

| Endpoint Pattern | Limit | Window |
|-----------------|-------|--------|
| All `/api/*` routes | 1000 requests | 15 minutes |
| Write operations (POST/PUT/DELETE) on entities | 200 requests | 15 minutes |

Override via environment variables: `RATE_LIMIT_MAX`, `RATE_LIMIT_WRITE_MAX`.

### Health Check

`GET /api/health` returns:

```json
{
  "status": "ok",
  "uptime": 3600,
  "database": "connected",
  "timestamp": "2025-01-01T00:00:00.000Z"
}
```

Returns `503` with `"status": "degraded"` if the database connection is down. Use this endpoint for Kubernetes readiness/liveness probes and load balancer health checks.

---

## Configuration Reference

Configuration files are in the `config/` directory. The loader in `config/index.js` selects the file based on `NODE_ENV`.

| Variable | Description | Dev Default | Prod Default |
|----------|-------------|-------------|--------------|
| `DATABASE_URL` | PostgreSQL connection string | *(required)* | *(required)* |
| `DB_SSL` | SSL for database; set `false` to disable (default: enabled with `rejectUnauthorized: false`) | enabled | enabled |
| `PORT` | Server port | `5000` | `5000` |
| `LOG_LEVEL` | Logging level (`debug`, `info`, `warn`, `error`) | `debug` | `warn` |
| `LOG_REQUESTS` | Log HTTP requests to stdout | `true` | `false` |
| `CORS_ORIGIN` | Allowed CORS origin | `*` | `*` |
| `AUTH_USER_EMAIL` | Mock user email | `user@local` | `admin@dataflow.app` |
| `AUTH_USER_NAME` | Mock user name | `Local User` | `Admin` |
| `AUTH_USER_ROLE` | Mock user role | `admin` | `admin` |
| `NODE_ENV` | Environment selector | `development` | `production` |

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/auth/me` | Current user |
| `GET` | `/api/entities/:name` | List entities (query: `sort`, `limit`, `skip`) |
| `POST` | `/api/entities/:name/filter` | Filter entities (body: `query`, `sort`, `limit`, `skip`) |
| `GET` | `/api/entities/:name/:id` | Get entity by ID |
| `POST` | `/api/entities/:name` | Create entity |
| `PUT` | `/api/entities/:name/:id` | Update entity |
| `DELETE` | `/api/entities/:name/:id` | Delete entity |
| `POST` | `/api/functions/:name` | Invoke server function |

### Entity Names

`Pipeline`, `Connection`, `PipelineRun`, `ActivityLog`, `AuditLog`, `IngestionJob`, `AirflowDAG`, `CustomFunction`, `ConnectionProfile`, `ConnectionPrerequisite`, `PipelineVersion`, `DataCatalogEntry`

---

## Database

All entities use a generic JSONB schema:

```sql
CREATE TABLE "table_name" (
  id SERIAL PRIMARY KEY,
  data JSONB NOT NULL DEFAULT '{}',
  created_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  created_by TEXT DEFAULT 'system'
);
```

Entity-specific fields are stored in the `data` JSONB column, allowing flexible schemas without migrations.

Tables are auto-created on server startup if they don't exist.

---

## Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start development server (Vite + API) |
| `npm run build` | Build production frontend |
| `npm start` | Start production server |
| `npm run lint` | Run ESLint |
| `npm run lint:fix` | Auto-fix lint issues |
