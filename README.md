# NotifyX — Notification Infrastructure Platform

> **One API. Every notification channel.**

NotifyX is a developer-facing, multi-tenant notification infrastructure platform designed to unify Email, Push, SMS, and In-App messaging through a single resilient, event-driven API.

---

## 📌 Current Project Status

```text
CURRENT PHASE: PHASE 11 — MULTI-TENANT QUOTAS, RATE LIMITING & BACKPRESSURE (COMPLETE)
```

> **Phase 11 Features**:
> - **Distributed Token Bucket Rate Limiting**: Atomic Redis Lua script (`ratelimit:tenant:{tenantId}:api`) enforcing requests/second and burst capacity with standard `X-RateLimit-*` and `Retry-After` headers.
> - **Notification Ingestion Quota**: Minute and Daily volume ceilings evaluated *before* database transactions, guaranteeing zero orphaned records on HTTP 429 rejection.
> - **Ephemeral vs. Durable State**: PostgreSQL is the authoritative source for tenant rate-limit configuration; Redis manages ephemeral tokens/counters for distributed coordination.
> - **Concurrency-Safe PostgreSQL Fallback**: Transactional row-lock fallback with automatic rollback on limit exhaustion during Redis degradation.
> - **Worker Bounded Concurrency & Backpressure**: Async `Semaphore` bounding concurrent in-flight deliveries across Email, Push, SMS, and In-App workers with post-delivery Kafka offset commits.
> - **Downstream Provider Throttling**: Provider HTTP 429 responses classified as `RATE_LIMIT` and seamlessly routed through the Phase 6 exponential backoff retry engine.
> - **Multi-Tenant Fairness**: Tenant rate-limit isolation preventing high-traffic floods from starving neighboring tenants.
> - **Bounded Metric Cardinality**: Aggregate/system-level Prometheus gauges (`rate_limit_remaining`) strictly excluding tenant IDs to prevent label explosion.
> - **Platform Console Dashboard**: Interactive Next.js console tab for "Usage & Limits".

---

## 🔄 Channel Worker Provider Architecture (Phase 5B)

```text
                         Channel Worker
                              │
                    Provider Factory
                       /          \
                    mock          real
                                  /   \
                               Push    SMS
                             provider provider
                              (FCM)  (Twilio)
                                │        │
                             External External
                             Service  Service
                              (FCM)   (Twilio)
```

---

## 🛡️ Distributed Systems Concepts & Delivery Guarantees

### 1. Consumer Group Separation & Channel Routing
All four channel workers consume from the single high-throughput Kafka topic: `notification.delivery.requested`. Each channel runs under an independent consumer group:
- `notifyx-inapp-workers`
- `notifyx-email-workers`
- `notifyx-push-workers`
- `notifyx-sms-workers`

Because Kafka tracks offsets **per consumer group**, when an email event is published, all groups receive the event. Non-matching workers (Push, SMS, In-App) filter it out and acknowledge the offset for their own group, while the Email worker dispatches the delivery to the provider.

### 2. At-Least-Once Delivery & Explicit Offset Acknowledgement
- Consumers run with `autoCommit: false`.
- The worker executes the delivery side effect and database update first.
- The Kafka offset is committed **only after** the PostgreSQL transaction successfully commits.
- If a transient provider error or database outage occurs, the offset is **not** acknowledged, ensuring Kafka redelivers the message upon recovery.

### 3. The External Side-Effect Problem & Provider Idempotency Boundary
A classic distributed-systems interview problem arises with external notification providers:
```text
1. Worker calls Provider
2. Provider sends email/SMS successfully
3. Worker crashes before committing Kafka offset
4. Kafka redelivers event to another worker replica
5. Second worker replica receives event
```
**Important Invariant**: AWS SES `SendEmailCommand` does not have native exactly-once idempotency deduplication. NotifyX distinguishes its **database-level idempotency state** from **external provider side-effects**:
1. NotifyX persists `deliveryId` in PostgreSQL.
2. When an event is consumed, the worker inspects the database record: if `delivery.status === 'DELIVERED'`, the event is acknowledged immediately without re-invoking SES.
3. `deliveryId` is logged and mapped as the logical idempotency key.
4. SES tags and configuration set names are **not** treated as an idempotency deduplication mechanism.

---

## 🏗️ Monorepo Architecture

```text
NotifyX/
├── apps/
│   ├── api/                      # Fastify REST API (:3001)
│   ├── outbox-publisher/         # Transactional Outbox Publisher (:3002)
│   ├── inapp-worker/             # In-App Worker (:3003, group: notifyx-inapp-workers)
│   ├── email-worker/             # Email Worker (:3004, group: notifyx-email-workers, SES / Mock)
│   ├── push-worker/              # Push Worker (:3005, group: notifyx-push-workers)
│   ├── sms-worker/               # SMS Worker (:3006, group: notifyx-sms-workers)
│   └── dashboard/                # Next.js App Router Console (:3000)
│
├── packages/
│   ├── config/                   # Centralized Zod-validated environment config
│   ├── database/                 # Prisma schema & PostgreSQL client
│   ├── kafka/                    # Reusable BaseChannelWorker, KafkaProducer, KafkaConsumer, Mock Providers
│   ├── logger/                   # Pino structured JSON logger
│   └── shared/                   # Event contracts, enums, constants
│
├── scripts/
│   ├── verify-phase2.js          # Phase 2 live verification
│   ├── verify-phase3.js          # Phase 3 live E2E & failure/recovery test
│   ├── verify-phase4.js          # Phase 4A live E2E, inbox API & idempotency test
│   ├── verify-phase4b.js         # Phase 4B live multi-channel E2E test
│   └── verify-phase5a.js         # Phase 5A dual-mode live E2E & SES/Mock verification
│
├── docker-compose.yml            # Local orchestration (10 services)
├── pnpm-workspace.yaml
├── turbo.json
└── README.md
```

---

## 🐳 Docker Stack & Services (10 Services)

| Service | Container Name | Port | Description |
|---|---|:---:|---|
| `postgres` | `notifyx-postgres` | `5432` | PostgreSQL 16 (Durable source of truth) |
| `redis` | `notifyx-redis` | `6379` | Redis 7 |
| `kafka` | `notifyx-kafka` | `9092`, `9094` | Apache Kafka 3.7.0 in KRaft mode |
| `api` | `notifyx-api` | `3001` | Fastify REST API & In-App Inbox endpoints |
| `outbox-publisher` | `notifyx-outbox-publisher` | `3002` | Transactional Outbox Publisher service |
| `inapp-worker` | `notifyx-inapp-worker` | `3003` | In-App Notification Worker (group: `notifyx-inapp-workers`) |
| `email-worker` | `notifyx-email-worker` | `3004` | Email Channel Worker (group: `notifyx-email-workers`) |
| `push-worker` | `notifyx-push-worker` | `3005` | Push Channel Worker (group: `notifyx-push-workers`) |
| `sms-worker` | `notifyx-sms-worker` | `3006` | SMS Channel Worker (group: `notifyx-sms-workers`) |
| `dashboard` | `notifyx-dashboard` | `3000` | Next.js Developer Dashboard |

---

## 🚀 Running the System

### 1. Start all 10 services via Docker Compose

```bash
docker compose up -d
```

### 2. Verify all containers are healthy

```bash
docker compose ps
```

### 3. Check Service Health & Readiness Endpoints

```bash
curl http://localhost:3001/health       # API
curl http://localhost:3002/health       # Outbox Publisher
curl http://localhost:3003/health       # In-App Worker
curl http://localhost:3004/health       # Email Worker
curl http://localhost:3005/health       # Push Worker
curl http://localhost:3006/health       # SMS Worker
```

---

## 🧪 Testing & Verification

```bash
# Run unit & integration tests across all packages (Vitest)
pnpm test

# Run TypeScript strict typecheck across all 12 packages
pnpm typecheck

# Build all packages & apps via Turborepo
pnpm build

# Run Phase 5A Live Dual-Mode Verification Test (Default: Mock Mode)
pnpm exec tsx scripts/verify-phase5a.js

# Run Phase 5B Live Multi-Mode Verification Test (Default: Mock Mode)
pnpm exec tsx scripts/verify-phase5b.js

# Optional Phase 5B: Run with real FCM (requires real FCM token & Firebase credentials)
MODE=fcm PUSH_PROVIDER=fcm FIREBASE_PROJECT_ID=my-project FIREBASE_CLIENT_EMAIL=sa@my-project.iam.gserviceaccount.com FIREBASE_PRIVATE_KEY="..." VERIFY_PUSH_TOKEN="fcm_token_..." pnpm exec tsx scripts/verify-phase5b.js

# Optional Phase 5B: Run with real Twilio (requires real Twilio credentials & recipient phone)
# Run Phase 7 Observability & Production Operations Verification
pnpm exec tsx scripts/verify-phase7.js

# Phase 8: Load Testing & Resilience Drills
pnpm test:load -- --scenario=all
pnpm test:failure
```

---

## 🗺️ Implementation Roadmap

- [x] **Phase 1: Foundation** — Monorepo, Docker, Fastify scaffolding, Next.js dashboard shell, Prisma schema.
- [x] **Phase 2: Core Domain** — Tenant, User, API Key models, cryptographic auth, strict multi-tenant isolation, atomic notification creation (`PENDING`), and notification retrieval.
- [x] **Phase 3: Event-Driven Backbone** — Apache Kafka (KRaft), OutboxEvent schema & migration, shared event contract, `@notifyx/kafka` producer, dedicated `apps/outbox-publisher` with safe row claiming (`SKIP LOCKED`), Docker stack integration, and failure recovery.
- [x] **Phase 4A: Kafka Consumers + In-App Worker** — Reusable `KafkaConsumer` infrastructure, dedicated consumer group (`notifyx-inapp-workers`), `apps/inapp-worker`, channel routing, durable DB-backed idempotency, atomic delivery state transition to `DELIVERED`, In-App Inbox REST APIs, health/readiness endpoints, Dockerization, and live duplicate event verification.
- [x] **Phase 4B: Multi-Channel Workers + Provider Adapters** — Reusable `BaseChannelWorker` framework, dedicated Kafka consumer groups (`notifyx-email-workers`, `notifyx-push-workers`, `notifyx-sms-workers`), channel routing, `MockEmailProvider`, `MockPushProvider`, `MockSmsProvider`, side-effect idempotency boundaries, full 10-service Docker stack, unit tests, and live multi-channel end-to-end verification.
- [x] **Phase 5A: Real Email Delivery using Amazon SES** — AWS SES v3 SDK integration, `SesEmailProvider`, factory pattern (`createEmailProvider`), UTF-8 mapping with HTML/text fallback, structured SES error classification (throttling, outages, permanent rejections), durable database idempotency protection against duplicate side effects, optional AWS env vars, unit tests (24/24 passing), and dual-mode live verification script.
- [x] **Phase 5B: Real Push (FCM) & SMS (Twilio) Delivery** — Official `firebase-admin` and `twilio` SDK integrations, `FcmPushProvider`, `TwilioSmsProvider`, factory patterns (`createPushProvider`, `createSmsProvider`), multiline private key handling, E.164 validation, structured error classification, sensitive token/phone redaction, offline mock defaults, and multi-mode verification script (`verify-phase5b.js`).
- [x] **Phase 6: Retry Engine, Exponential Backoff, Retry Topics, and DLQ** — Durable retries with exponential backoff & full jitter, atomic PostgreSQL transactions, dedicated Kafka retry topics (`notification.delivery.retry`), terminal Dead-Letter Queue routing (`notification.delivery.dlq`), and unified outbox publishing with zero dual-writes.
- [x] **Phase 7: Observability & Production Operations** — Standardized Pino logging, strict redaction, correlationId propagation (`requestId`, `correlationId`, `eventId`), bounded Prometheus metrics (`@notifyx/metrics`), decoupled liveness/readiness, and automated Prometheus & Grafana dashboard provisioning.
- [x] **Phase 8: Reliability, Load Testing & Failure Engineering** — Standalone load-testing harness (`tools/load-test`), baseline, sustained, and burst tests, automated failure drills (Kafka broker outage, PostgreSQL outage, worker crash, provider retry storms, duplicate Kafka message injection), and empirical benchmark measurement.
- [x] **Phase 9: AWS Deployment & Production Infrastructure** — Production-oriented AWS architecture documentation (VPC, ALB, RDS, ElastiCache, KRaft Kafka, ECR, IAM, Secrets Manager), Docker image hardening (non-root users, lean multi-stage builds), production config validation (reject mock providers + localhost URLs at startup), `docker-compose.prod.yml`, ECR build/push pipeline, staging deploy script, environment-aware smoke test, and GitHub Actions CI/CD.

---

## ☁️ AWS Deployment

See **[docs/aws_deployment.md](docs/aws_deployment.md)** for the complete production deployment guide covering:

- Dual-tier VPC networking (ALB → Private EC2 → Private Data Subnets)
- Amazon RDS PostgreSQL, ElastiCache Redis, KRaft Kafka
- ECR image registry with immutable Git SHA tags
- IAM least-privilege roles and GitHub OIDC authentication
- AWS Secrets Manager integration
- Prisma migration procedure
- Staging deployment script and smoke tests

```bash
# Build all images with current Git SHA tag
npm run ecr:build

# Build + push to ECR (requires AWS credentials)
ECR_REGISTRY=123456789012.dkr.ecr.us-east-1.amazonaws.com npm run ecr:push

# Run production smoke test (non-destructive health checks only)
SMOKE_ENV=production API_BASE=https://api.notifyx.example.com npm run smoke-test

# Run staging full lifecycle smoke test
SMOKE_ENV=staging npm run smoke-test

# Deploy to staging (requires DEPLOY_ENV=staging CONFIRM_DEPLOY=true)
DEPLOY_ENV=staging CONFIRM_DEPLOY=true npm run deploy:staging
```
