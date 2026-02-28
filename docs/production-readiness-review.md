# DataFlow Platform — Production Readiness Review

**Date**: February 28, 2026
**Target**: 1,000+ users, multi-tenant, AKS deployment

---

## Current State: Not ready for 1000+ multi-tenant production

The platform has a strong UI and solid config architecture, but the backend needs significant hardening before it can serve 1000+ users across multiple tenants.

---

## CRITICAL Priority

### 1. No Multi-Tenancy

The database has no concept of tenants. All 13 tables store data globally — if two organizations share the same instance, they'd see each other's pipelines, connections, and credentials. Every table needs a `tenant_id` column, and every API query needs to scope by tenant. Consider PostgreSQL Row-Level Security (RLS) as an enforcement layer.

**Affected**: All 13 DB tables, all API endpoints, all frontend data fetching

### 2. Session Management is In-Memory

LDAP sessions are stored in a JavaScript `Map` in server memory. This means:
- Sessions are lost on every restart or deploy
- Cannot run multiple server instances (AKS pods) — each has its own session map
- No session expiry enforcement
- Session ID is passed via a custom header, not a secure HTTP-only cookie

**Recommendation**: Store sessions in Redis or PostgreSQL, use secure HTTP-only cookies, enforce TTL, support horizontal scaling.

**Affected**: `server/ldap-auth.js`, `src/lib/AuthContext.jsx`

### 3. CORS is Wide Open

`cors_origin: "*"` allows any website to call the API. For production, this must be locked down to actual domain(s).

**Affected**: `dataflow.yaml` → `server.cors_origin`, `server/middleware.js`, `server/production.js`

---

## HIGH Priority

### 4. Vault Integration Needs Caching

Currently, every Vault request does a fresh login — no token caching, no renewal strategy, no circuit breaker if Vault is down. At scale, this becomes a bottleneck and a reliability risk. Vault tokens should be cached and renewed automatically.

**Affected**: `server/vault.js`

### 5. Database Pool is Undersized

Pool max is 10 connections (dev). With 1000+ users making concurrent requests, connection exhaustion will occur. Production config has 30 max, which is better but may still need tuning. No migration framework — the schema is built on startup with `CREATE TABLE IF NOT EXISTS`, which won't handle future schema changes gracefully.

**Affected**: `server/db.js`, `dataflow.yaml` → `database`

### 6. No Input Validation Layer

API endpoints don't validate request bodies against schemas. Any malformed data gets passed through to the database. A validation middleware (like Zod or Joi) would catch bad input before it reaches the DB.

**Affected**: `server/middleware.js`, all API route handlers

### 7. RBAC is Shallow

Only three roles exist: admin, developer, viewer. There's no permission model (who can edit which pipelines, which connections). For 1000+ users across teams, resource-level permissions are needed — at minimum, team/project scoping.

**Affected**: `server/ldap-auth.js`, `dataflow.yaml` → `ldap.roles_mapping`

---

## MEDIUM Priority

### 8. No Structured Logging or Observability

Logging is `console.log` only. For production on AKS, structured JSON logs are needed (for Azure Monitor / ELK / Splunk), along with request IDs for tracing, and health/readiness endpoints beyond a basic healthcheck.

**Affected**: All server files, `Dockerfile`

### 9. Config System is Global

The YAML config works well for a single-tenant deployment but doesn't support per-tenant configuration (different Vault paths, different GitLab projects, different Airflow instances per team). If multi-tenancy requires config isolation, a tenant-config layer is needed.

**Affected**: `dataflow.yaml`, `config/*.yaml`, `src/dataflow-config.js`

### 10. No Background Job Queue

Pipeline runs, Airflow syncs, and GitLab deploys happen synchronously in the request cycle. At scale, these should be offloaded to a job queue (Bull/BullMQ with Redis, or similar) so the API stays responsive.

**Affected**: `server/middleware.js`, `server/airflow-proxy.js`, `server/gitlab.js`

### 11. Demo Mode is a Risk

`LDAP_DEMO_MODE=true` bypasses authentication entirely with hardcoded credentials. If this accidentally gets enabled in production, anyone can log in. Consider removing demo mode from the production build entirely, or adding a startup check that refuses to start with demo mode on in production.

**Affected**: `server/ldap-auth.js`, `dataflow.yaml` → `ldap.demo_mode`

---

## LOW Priority (recommended before launch)

### 12. No Database Migrations

Schema changes will be painful without a migration tool (like `node-pg-migrate` or Knex migrations).

### 13. No CI/CD Pipeline Hardening

Automated tests, security scanning, dependency auditing are absent.

### 14. Frontend Hooks

Creating `useAuth`, `useVault`, `useTenant` hooks would clean up the frontend code but isn't blocking.

### 15. Secrets Exposure

Some flows return Vault secrets to the browser; secrets should stay server-side and only pass through opaque references.

---

## Recommended Implementation Sequence

| Order | Item | Priority | Effort |
|-------|------|----------|--------|
| 1 | Tenant model — add `tenant_id` everywhere, scope all queries | Critical | Large |
| 2 | Auth hardening — Redis sessions, secure cookies, TTL, kill demo mode in prod | Critical | Medium |
| 3 | CORS lockdown — restrict to actual domains | Critical | Small |
| 4 | Vault token caching — cache + renew, add circuit breaker | High | Medium |
| 5 | DB pool + migrations — right-size pools, add migration framework | High | Medium |
| 6 | Input validation — schema validation on all API endpoints | High | Medium |
| 7 | RBAC expansion — team/project-level permissions | High | Large |
| 8 | Structured logging — JSON logs with request IDs | Medium | Medium |
| 9 | Job queue — async processing for heavy operations | Medium | Medium |
| 10 | Observability — metrics, health endpoints, alerting | Medium | Medium |
| 11 | Per-tenant config — tenant-aware config layer | Medium | Medium |
| 12 | Demo mode safety — block demo mode in production env | Medium | Small |
| 13 | DB migrations — migration framework | Low | Small |
| 14 | Frontend hooks — useAuth, useVault, useTenant | Low | Small |
| 15 | Secrets isolation — keep secrets server-side only | Low | Small |

---

## Architecture Gaps Summary

| Area | Current | Production Target |
|------|---------|-------------------|
| Tenancy | Single-tenant, no isolation | Multi-tenant with RLS |
| Sessions | In-memory Map | Redis/PostgreSQL + secure cookies |
| Auth | 3 roles, no resource perms | RBAC with team/project scoping |
| Vault | Login per request | Cached tokens, auto-renewal |
| DB Pool | 10 max (dev) / 30 (prod) | Right-sized + read replicas |
| Schema Mgmt | CREATE IF NOT EXISTS | Migration framework |
| Validation | None | Zod/Joi on all endpoints |
| Logging | console.log | Structured JSON + request IDs |
| Jobs | Synchronous | Async queue (Bull/Redis) |
| CORS | Wildcard (*) | Domain-restricted |
| Scaling | Stateful (in-memory) | Stateless, horizontally scalable |

---

## Industry Benchmark: Airflow vs Airbyte vs DataFlow

Research into how Apache Airflow and open-source Airbyte architect their production systems, and how DataFlow compares.

### Component Comparison

| Component | Airflow | Airbyte | DataFlow (Current) |
|-----------|---------|---------|-------------------|
| Web Server | Flask + Gunicorn | Java API Server + React UI | Express + React/Vite |
| Job Execution | Celery Workers via Redis/RabbitMQ | Temporal Workers + Workload API | Synchronous in Express request |
| Database | PostgreSQL (metadata store) | PostgreSQL (config + metadata) | PostgreSQL (JSONB entities) |
| Message Broker | Redis or RabbitMQ | Temporal (backed by PostgreSQL) | None |
| Session/Auth | Flask-AppBuilder + DB-backed sessions | Enterprise SSO/SCIM, workspace tokens | In-memory Map (not persistent) |
| RBAC | DAG-level permissions (can_read/can_edit per DAG) | Workspace + role-based (Enterprise) | 3 flat roles, no resource perms |
| Multi-Tenancy | Not built-in — Lyft extended with team-based DAG repos + separate worker pools | Workspaces (logical isolation in shared DB) | None |
| Secret Mgmt | Connections store in DB + external backends (Vault, AWS SM) | Pluggable: none / GCP SM / AWS SM | Vault integration (no caching) |
| Config | `airflow.cfg` (INI file) + env vars | `.env` + `values.yaml` (Helm) | `dataflow.yaml` + config/*.yaml |
| Scaling | Add Celery workers, stateless webserver | Add worker pods, workload parallelism config | Single process, stateful |

### Key Insights

**1. Neither Airflow nor Airbyte has true multi-tenancy out of the box.**
Airflow explicitly states it lacks multi-tenant isolation. Airbyte uses "workspaces" as logical tenants within a shared database. Both require enterprise extensions or custom work for real tenant isolation. DataFlow adding `tenant_id` + workspace scoping would match how Airbyte does it.

**2. Both use Redis (or equivalent) as critical infrastructure.**
- Airflow: Redis as Celery broker + result backend. Every production Airflow deployment uses Redis.
- Airbyte: Uses Temporal (which itself uses PostgreSQL) instead of Redis, but serves the same purpose — durable job queue + state management.
- Conclusion: Redis is the right addition for DataFlow — simpler than Temporal, proven at scale.

**3. RBAC pattern is the same across both.**
Both use role → permission → resource. Airflow's `can_read`/`can_edit` per DAG is the model DataFlow should follow per pipeline/connection. Implementable without Flask-AppBuilder complexity.

**4. Job execution is always async and separated from the API.**
Airflow has workers pulling from Redis queues. Airbyte has Temporal workers polling task queues. Neither runs long operations inside the API request. DataFlow needs the same pattern — Redis + BullMQ is the simplest path (much lighter than standing up Temporal or Celery).

**5. Airbyte's "workloads" model is worth studying.**
They decoupled "scheduling a job" from "running a job" — the API enqueues workloads, workers pick them up when resources are available. This gives back-pressure and self-healing. DataFlow could adopt this pattern with BullMQ queues.

### Feasibility for DataFlow

| Pattern | Source | Feasible? | Effort |
|---------|--------|-----------|--------|
| Workspace-based multi-tenancy | Airbyte | Yes — add `tenant_id` to all tables + API scoping | Medium |
| DAG-level RBAC | Airflow | Yes — permission table: role x resource x action | Medium |
| Redis for sessions + caching | Both (Airflow uses Redis directly) | Yes — `ioredis` + `connect-redis` | Small |
| Async job queue | Airflow (Celery), Airbyte (Temporal) | Yes — BullMQ on Redis (simpler than Temporal) | Medium |
| Pluggable secret backends | Both | Already have Vault — add caching layer | Small |
| DB migrations | Both use Alembic (Python) | Use `node-pg-migrate` | Small |
| Structured logging | Both | Use `pino` (JSON logger) | Small |
| Stateless API servers | Both | Move sessions to Redis | Small |

### Target Production Stack

DataFlow can match the production patterns of both Airflow and Airbyte without their complexity. Both are large Java/Python distributed systems. DataFlow is a focused Node.js app — it does not need Celery or Temporal.

```
PostgreSQL  (already have)  — data + migrations
Redis       (add)           — sessions, caching, job queue, rate limiting
BullMQ      (add)           — async workers for pipeline runs, deploys, syncs
```

This matches Airflow's Celery pattern with a fraction of the operational overhead.

### Why Redis Specifically

Redis is one dependency that addresses 6 out of the 15 review items:

| Problem | How Redis Solves It |
|---------|-------------------|
| In-memory sessions (Critical) | Sessions survive restarts, shared across AKS pods, built-in TTL |
| Horizontal scaling (Critical) | All pods stateless with shared session store |
| Vault token caching (High) | Cache tokens with TTL matching lease duration |
| API rate limiting (High) | Distributed rate limiting across pods |
| Background job queue (Medium) | BullMQ runs on Redis for async pipeline/deploy/sync |
| Response caching (Medium) | Cache schema introspection, connection tests, Airflow status |

Azure Cache for Redis is the managed option for AKS deployments. The Node.js ecosystem has mature libraries: `ioredis`, `connect-redis`, `bullmq`.
