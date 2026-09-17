# NotifyX — System Overview

## 1. What Is NotifyX?

NotifyX is a notification infrastructure platform.

A client application integrates once with NotifyX and can then send:

```text
Email
Push
SMS
In-App
```

through one API.

The central design idea is:

> **Applications generate notification events; NotifyX is responsible for reliably processing and delivering them.**

---

## 2. Why Does It Exist?

Without NotifyX:

```text
Application
 ├── Email Provider
 ├── SMS Provider
 ├── FCM
 ├── Notification DB
 ├── Retry Logic
 ├── Rate Limiting
 ├── Scheduling
 └── Analytics
```

With NotifyX:

```text
Application
      ↓
 NotifyX API
      ↓
Notification Infrastructure
      ↓
Email / Push / SMS / In-App
```

The application only needs to understand NotifyX's API.

---

## 3. Mental Model

Think of NotifyX as a **delivery pipeline**.

```text
REQUEST
   ↓
VALIDATE
   ↓
AUTHORIZE
   ↓
PERSIST
   ↓
QUEUE
   ↓
PROCESS
   ↓
DELIVER
   ↓
TRACK
```

If something fails:

```text
PROCESS
   ↓
FAIL
   ↓
RETRY
   ↓
RETRY
   ↓
DLQ
```

---

## 4. Core Architecture

```text
                    Client Application
                           │
                           ▼
                     ┌───────────┐
                     │ API Server│
                     └─────┬─────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         PostgreSQL      Redis        Kafka
                                        │
                  ┌─────────────┬───────┼──────────────┐
                  ▼             ▼       ▼              ▼
              Email Worker  Push Worker SMS Worker  In-App Worker
                  │             │       │              │
                  ▼             ▼       ▼              ▼
               Provider        FCM    Twilio        PostgreSQL
```

---

## 5. Synchronous vs Asynchronous Work

### Bad approach

```text
Client
 ↓
API
 ↓
Email Provider
 ↓
SMS Provider
 ↓
Push Provider
 ↓
Response
```

Problems:

- slow requests
- provider outages affect API
- poor scalability
- request timeout risk

### NotifyX approach

```text
Client
 ↓
API
 ↓
Kafka
 ↓
202 Accepted
```

Then:

```text
Kafka
 ↓
Workers
 ↓
Providers
```

This separates **request acceptance** from **notification delivery**.

---

## 6. Main Components

### API Server

The front door.

Handles:

- authentication
- authorization
- validation
- rate limiting
- notification creation
- event publishing

### PostgreSQL

The source of truth.

Stores durable state.

### Redis

Fast temporary/coordination state.

Handles:

- rate limiting
- cache
- idempotency keys
- distributed locks

### Kafka

The event backbone.

Handles:

- buffering
- asynchronous processing
- consumer groups
- replay
- horizontal worker scaling

### Workers

Perform channel-specific delivery.

```text
Email Worker
Push Worker
SMS Worker
In-App Worker
```

### Scheduler

Finds notifications that are due and sends them into Kafka.

### Dashboard

Allows developers/admins to operate NotifyX.

### Observability

```text
OpenTelemetry
Prometheus
Grafana
```

---

## 7. Example End-to-End Flow

Suppose an e-commerce application wants to notify a user:

> Payment successful.

It sends:

```http
POST /v1/notifications
```

with:

```json
{
  "userId": "usr_123",
  "templateId": "payment-success",
  "channels": ["email", "in_app"],
  "data": {
    "orderId": "ORD123",
    "amount": 999
  }
}
```

### Step 1 — Authentication

API validates the API key.

### Step 2 — Tenant resolution

The API identifies which tenant owns the request.

### Step 3 — Validation

Zod validates the payload.

### Step 4 — Rate limit

Redis checks whether the tenant is within its allowed rate.

### Step 5 — Idempotency

The API checks the idempotency key.

### Step 6 — Database

Notification state is persisted.

### Step 7 — Kafka

Events are published:

```text
notification.email
notification.inapp
```

### Step 8 — Workers

Email Worker:

```text
Kafka
 ↓
Template
 ↓
Email Provider
```

In-App Worker:

```text
Kafka
 ↓
PostgreSQL
```

### Step 9 — Status

Delivery records are updated.

---

## 8. Why Kafka?

Kafka decouples producers from consumers.

Without Kafka:

```text
API → Email Worker
API → Push Worker
API → SMS Worker
```

The API becomes coupled to every worker.

With Kafka:

```text
API → Kafka → Workers
```

Kafka also provides:

- buffering
- consumer groups
- partitions
- replay
- asynchronous processing

This makes it suitable for high-volume event processing.

---

## 9. Why Redis?

Redis is not our primary database.

It is used because some operations need very fast temporary state.

Examples:

```text
Rate limiting
Cache
Idempotency
Distributed locks
```

PostgreSQL answers:

> What is the authoritative state?

Redis answers:

> Can I answer this quickly or coordinate this operation?

---

## 10. Why PostgreSQL?

Notifications have durable state:

```text
Who?
What?
Which tenant?
Which template?
Which channel?
When?
What status?
How many retries?
```

This requires durable relational storage.

PostgreSQL also gives us:

- transactions
- constraints
- indexes
- reliable persistence
- relational querying

---

## 11. Why Docker?

NotifyX contains multiple processes:

```text
API
Dashboard
Workers
Scheduler
PostgreSQL
Redis
Kafka
Prometheus
Grafana
```

Installing everything manually would be painful.

Docker lets us package each component consistently.

Docker Compose provides local orchestration.

```bash
docker compose up
```

starts the development environment.

---

## 12. Reliability Model

NotifyX should initially be described as providing:

> **At-least-once processing with idempotent consumers.**

Do not describe it as exactly-once external delivery.

Why?

Because this can happen:

```text
Worker
 ↓
Provider accepts request
 ↓
Network response lost
 ↓
Worker crashes
 ↓
Kafka redelivers
```

The worker cannot always know whether the provider already delivered the message.

This is a fundamental distributed-systems ambiguity.

---

## 13. Retry Model

Transient failure:

```text
Provider
 ↓
500
```

Worker:

```text
retry 1
 ↓
retry 2
 ↓
retry 3
 ↓
retry 4
 ↓
DLQ
```

Backoff:

```text
5 sec
30 sec
2 min
...
```

with jitter.

---

## 14. DLQ Model

The Dead Letter Queue is the final destination for messages that cannot be successfully processed after the retry policy is exhausted.

```text
Kafka
 ↓
Worker
 ↓
Failure
 ↓
Retry
 ↓
Retry
 ↓
DLQ
```

The dashboard allows authorized operators to inspect and replay messages.

---

## 15. In-App Notification Model

In-app notifications are persisted by NotifyX.

Example:

```text
┌──────────────────────────────────┐
│ Notifications                    │
├──────────────────────────────────┤
│ 🔵 Payment successful             │
│    Your payment was received.     │
│                                  │
│ 🔵 Order shipped                  │
│    Order ORD123 is on the way.    │
│                                  │
│ ⚪ Welcome to the platform        │
└──────────────────────────────────┘
```

Core fields:

```text
id
tenant_id
user_id
title
body
data
read_at
created_at
```

Real-time delivery can later be added using WebSockets or Server-Sent Events.

---

## 16. Multi-Tenant Model

Every request belongs to a tenant.

```text
API Key
   ↓
Tenant
   ↓
User
   ↓
Notification
```

Example:

```text
Tenant A
  └── Notification 1

Tenant B
  └── Notification 2
```

Tenant A must never be able to access Tenant B's data.

This is one of the most important security invariants.

---

## 17. Scaling

The API is stateless.

Therefore:

```text
Load Balancer
     │
 ┌───┼────┐
 ▼   ▼    ▼
API  API  API
```

Workers scale independently:

```text
Kafka
 │
 ├── Email Worker 1
 ├── Email Worker 2
 ├── Email Worker 3
 └── Email Worker 4
```

Kafka consumer groups distribute work among workers.

---

## 18. Performance Model

The API should optimize for:

```text
fast acceptance
```

rather than:

```text
fast external delivery inside the request
```

The important metrics are:

```text
API latency
Kafka publish latency
Consumer lag
Worker processing latency
Provider latency
End-to-end delivery latency
```

---

## 19. Observability Model

Every important request should be traceable.

Conceptually:

```text
Request
  │
  ├── API span
  │
  ├── DB span
  │
  ├── Kafka publish span
  │
  └── Worker span
          │
          └── Provider span
```

Metrics are exposed to Prometheus and visualized in Grafana.

---

## 20. Key System Invariants

These rules must remain true:

### Invariant 1

A tenant cannot access another tenant's data.

### Invariant 2

External notification delivery is never performed directly inside the API request.

### Invariant 3

Workers must tolerate duplicate Kafka messages.

### Invariant 4

Redis is not the only durable source of notification state.

### Invariant 5

Failed transient deliveries are retried according to policy.

### Invariant 6

Messages exceeding retry limits go to DLQ.

### Invariant 7

Secrets are never committed to Git.

### Invariant 8

Every important state transition is observable.

---

## 21. Repository Mental Model

```text
notifyx/
│
├── apps/
│   ├── api/
│   ├── dashboard/
│   ├── email-worker/
│   ├── push-worker/
│   ├── sms-worker/
│   ├── inapp-worker/
│   └── scheduler/
│
├── packages/
│   ├── database/
│   ├── kafka/
│   ├── redis/
│   ├── config/
│   ├── logger/
│   ├── telemetry/
│   └── shared/
│
├── infrastructure/
│   ├── docker/
│   ├── prometheus/
│   └── grafana/
│
├── docs/
│   ├── architecture.md
│   ├── prd.md
│   └── system_overview.md
│
├── docker-compose.yml
└── README.md
```

The exact repository layout may be changed if implementation experience demonstrates a better structure.

---

## 22. Coding Assistant Context

Before modifying NotifyX, an AI coding assistant should understand:

```text
Product:
Notification infrastructure

Architecture:
Event-driven

Primary DB:
PostgreSQL

Cache/coordination:
Redis

Event backbone:
Kafka

Deployment:
Docker / Docker Compose

Channels:
Email / Push / SMS / In-App

Observability:
OpenTelemetry / Prometheus / Grafana
```

Priority order:

```text
Correctness
   ↓
Security
   ↓
Reliability
   ↓
Observability
   ↓
Performance
   ↓
Developer convenience
```

When uncertain, preserve existing architectural invariants and ask for clarification before making a breaking architectural decision.

---

## 23. What This Project Should Demonstrate

By completion, NotifyX should allow the developer to explain:

```text
REST API design
        ↓
Authentication
        ↓
PostgreSQL transactions
        ↓
Redis caching
        ↓
Rate limiting
        ↓
Kafka
        ↓
Consumer groups
        ↓
Async workers
        ↓
Idempotency
        ↓
Retries
        ↓
Dead Letter Queues
        ↓
Scheduling
        ↓
Multi-tenancy
        ↓
Docker
        ↓
AWS
        ↓
Observability
        ↓
Load testing
        ↓
Failure handling
```

The goal is not to maximize the number of technologies.

The goal is to make every technology solve a real architectural problem.

---

## 24. Phase 8 Reliability and Resilience

Empirical load testing, backpressure verification, and failure drills are implemented in `tools/load-test` and `scripts/run-failure-drills.js`. Full results and operating procedures are documented in `docs/load_testing_and_resilience.md`.

---

## 25. Phase 9 AWS Deployment & Production Infrastructure

Phase 9 establishes the production-oriented AWS deployment topology and container hardening for NotifyX:

```text
Internet
   ↓
Route 53 (DNS) + ACM (TLS)
   ↓
Internet-Facing ALB (Public Subnet)
   ↓ Private HTTP
Private EC2 (Docker Compose in Private Subnet)
   ├── Fastify API (3001)
   ├── Next.js Dashboard (3000)
   ├── Outbox Publisher (3002)
   ├── Channel Workers (In-App: 3003, Email: 3004, Push: 3005, SMS: 3006)
   └── Apache Kafka (KRaft Broker on Private EC2 + EBS in Staging / Amazon MSK in Prod)
         ↓
   Amazon RDS PostgreSQL 16 (Private Data Subnet)
   Amazon ElastiCache Redis 7 (Private Data Subnet)
```

Key features:
- **Zero Direct Public Ingress**: Only the ALB is public; all API, Dashboard, Workers, Kafka, DB, and Cache instances reside in private subnets.
- **Hardened Containers**: All service Dockerfiles run as non-root users (`notifyx` / `nextjs`, UID 1001) using lean multi-stage builds.
- **Fail-Fast Environment Validation**: `packages/config` strictly rejects localhost/127.0.0.1 URLs and mock providers when `NODE_ENV=production`.
- **Automated Staging Tooling**: `scripts/build-and-push-ecr.js`, `scripts/deploy-staging.js`, and `scripts/smoke-test.js` provide complete CI/CD and deployment verification.
- **Documentation**: Comprehensive 20-section deployment guide at `docs/aws_deployment.md`.

---

## 26. Phase 10 Notification Product Layer

Phase 10 adds the developer product layer on top of the notification engine:

```text
Client Application
       │ [Idempotency-Key Header]
       ▼
Fastify API Server
       ├── Template Service (Mustache Interpolation, Immutable Versioning)
       ├── Preference Service (Channel & Category Matrix with Opt-In Defaults)
       └── Idempotency Service (Header-Only SHA-256 Hashing, 24h TTL, 409 Conflict)
              │
       PostgreSQL (ACID Source of Truth)
       ├── Notification [Status: SCHEDULED / PENDING / CANCELLED]
       ├── NotificationTemplate & NotificationTemplateVersion
       ├── NotificationPreference
       ├── IdempotencyRecord
       └── ScheduledNotification [Status: SCHEDULED / DISPATCHED / CANCELLED]
              │
              │ [FOR UPDATE SKIP LOCKED Polling]
              ▼
       Scheduler Service (Port 3007)
              │
              ▼
       OutboxEvent (status: PENDING)
              │
              ▼
       Outbox Publisher ──► Kafka ──► Workers ──► Providers
```

Key features:
- **Template System**: Immutable versioning (v1, v2) with explicit row-level locking (`SELECT ... FOR UPDATE`) and tenant ownership checks during promotion. Mustache rendering with size limits and missing variable validation.
- **Recipient Preferences**: Channel and category matrix (`TRANSACTIONAL`, `SECURITY`, `SYSTEM` enabled by default; `MARKETING` disabled by default).
- **Header-Only Idempotency**: Strict `Idempotency-Key` header with canonical SHA-256 hashing. Invariant: *"exactly one NotifyX Notification record for the same tenant + idempotency key."*
- **Scheduled Notifications**: Dedicated scheduler service (`apps/scheduler`, port 3007) with `FOR UPDATE SKIP LOCKED` claim polling. Concurrency-safe conversion to `NotificationDelivery` and `OutboxEvent` records. `ScheduledNotification.DISPATCHED` guarantees the job was converted into outbox events.
- **Non-Authoritative Cache**: Redis caching with silent fallback to PostgreSQL on any failure.
- **Next.js Console**: Phase 10 interactive dashboard with Templates, Preferences, Scheduled Queue, and Delivery Audit consoles.
- **Documentation**: Comprehensive guide at `docs/notification_product_features.md`.

---

## 27. Phase 11 Multi-Tenant Quotas, Rate Limiting & Backpressure

Phase 11 introduces comprehensive rate-limiting, quota enforcement, and backpressure controls:

```text
Client Request
      │
      ▼
Rate Limit Hook (Fastify preHandler)
      ├── Redis Token Bucket (`ratelimit:tenant:{id}:api`)
      └── [Fail-Closed / Concurrency-Safe PostgreSQL Fallback]
      │
      ▼ [Passed Token Bucket]
Notification Ingestion Service
      ├── Redis Quota Counter (`quota:tenant:{id}:minute/day`)
      └── [Rejection: HTTP 429 => ZERO DB writes / NO orphaned rows]
      │
      ▼ [Passed Quotas]
PostgreSQL ACID Transaction
      ├── Notification & NotificationDelivery
      └── OutboxEvent
            │
            ▼
Outbox Publisher ──► Kafka ──► Worker Semaphore ──► Provider (Retry on 429)
```

Key features:
- **Distributed Token Bucket Rate Limiting**: Atomic Redis Lua script managing requests/second and burst capacity per tenant (`ratelimit:tenant:{tenantId}:api`) with standard `X-RateLimit-*` and `Retry-After` headers.
- **Pre-DB Quota Enforcement**: Notification volume limits (`notificationsPerMinute` and `notificationsPerDay`) are evaluated prior to database writes, ensuring 0 orphaned records on HTTP 429 rejection.
- **Ephemeral vs. Durable Separation**: PostgreSQL stores durable tenant quota configurations; Redis manages ephemeral rate-limit tokens and sliding usage counters.
- **Concurrency-Safe Fallback**: Atomic transactional upsert and rollback pattern on `usage_quota_counters` during Redis degradation.
- **Worker Bounded Concurrency & Backpressure**: Asynchronous `Semaphore` controlling active in-flight worker executions across all channels (`EMAIL_WORKER_CONCURRENCY`, `PUSH_WORKER_CONCURRENCY`, etc.) with Kafka offset commits strictly postponed until post-execution.
- **Provider Throttling**: Downstream provider HTTP 429 responses are captured as `RATE_LIMIT` errors and routed through exponential backoff retry machinery.
- **Bounded Metric Cardinality**: Aggregate Prometheus gauges (`rate_limit_remaining`) strictly excluding tenant IDs to preserve metric infrastructure scalability.
- **Next.js Console**: Phase 11 interactive dashboard tab for "Usage & Limits".
- **Documentation**: Comprehensive guide at `docs/rate_limiting_and_backpressure.md`.


