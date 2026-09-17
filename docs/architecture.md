# NotifyX — Detailed Architecture

## 1. Purpose

NotifyX is a production-style, multi-tenant notification infrastructure platform. It exposes a unified API through which client applications can create notifications and deliver them through four channels:

- Email
- Push
- SMS
- In-app

The system is event-driven. API requests are accepted quickly, notification work is published to Kafka, and channel-specific workers perform delivery asynchronously.

The architecture is intentionally designed to demonstrate practical distributed-systems concepts: asynchronous processing, queues, retries, idempotency, rate limiting, caching, scheduling, priority handling, deduplication, dead-letter queues, multi-tenancy, observability, Docker-based deployment, and cloud deployment.

---

## 2. Architectural Principles

1. **Asynchronous by default** — API requests must not wait for external notification providers.
2. **At-least-once processing** — Kafka consumers may process a message more than once; handlers must therefore be idempotent.
3. **PostgreSQL is the source of truth** for durable application state, tenant configurations, and rate-limit definitions.
4. **Redis is an acceleration and distributed coordination layer** storing ephemeral rate-limit tokens, sliding window counters, and non-authoritative caches; it is not the durable source of truth.
5. **Kafka is the event backbone** between producers and workers.
6. **External providers are isolated behind provider adapters** so providers can be replaced or mocked.
7. **Tenant isolation is mandatory** at the application and database query layers.
8. **Every important operation is observable** through logs, metrics, and traces.
9. **Failure is expected** — transient provider errors are retried; permanently failing messages eventually enter a DLQ.
10. **Docker Compose is the local orchestration mechanism**. Kubernetes is explicitly out of scope for the initial project.

---

## 3. High-Level Architecture

```text
                         ┌───────────────────────┐
                         │   Client Applications │
                         │ Web / Mobile / Server │
                         └───────────┬───────────┘
                                     │ HTTPS
                                     ▼
                         ┌───────────────────────┐
                         │      API Server       │
                         │ Node.js + TypeScript  │
                         │ Fastify               │
                         └───────┬───────┬───────┘
                                 │       │
                         ┌───────┘       └──────────┐
                         ▼                          ▼
                  ┌─────────────┐             ┌─────────────┐
                  │ PostgreSQL  │             │    Redis    │
                  │ Source Data │             │ Cache / RL  │
                  └─────────────┘             └─────────────┘

                                 │ publish
                                 ▼
                         ┌───────────────────────┐
                         │         Kafka         │
                         │   Event Backbone      │
                         └───────┬───────┬───────┘
                                 │       │
                   ┌─────────────┘       └──────────────┐
                   ▼                                    ▼
          ┌─────────────────┐                  ┌─────────────────┐
          │  Email Worker   │                  │   Push Worker   │
          └────────┬────────┘                  └────────┬────────┘
                   │                                    │
                   ▼                                    ▼
             Email Provider                         FCM / APNs

                   ┌─────────────────┐
                   │   SMS Worker    │
                   └────────┬────────┘
                            ▼
                         Twilio

                   ┌─────────────────┐
                   │ In-App Worker   │
                   └────────┬────────┘
                            ▼
                       PostgreSQL
                            │
                            ▼
                    Client Dashboard

                         ┌────────────────────────┐
                         │     Scheduler          │
                         │ scheduled notifications│
                         └───────────┬────────────┘
                                     │
                                     ▼
                                    Kafka

                 ┌─────────────────────────────────┐
                 │ Observability                   │
                 │ OpenTelemetry → Prometheus      │
                 │                 → Grafana       │
                 └─────────────────────────────────┘
```

---

## 4. Components

### 4.1 API Server

Technology:

- Node.js
- TypeScript
- Fastify
- Zod for request validation
- Prisma ORM

Responsibilities:

- Authentication
- API-key validation
- Tenant resolution
- Authorization
- Request validation
- Rate limiting
- Notification creation
- Template selection
- Preference resolution
- Idempotency checks
- Publishing notification jobs to Kafka
- API responses

The API server must not call external email/SMS/push providers directly.

For accepted asynchronous notification requests, the API should normally return `202 Accepted` with a notification/job identifier.

---

### 4.2 Dashboard

Technology:

- Next.js
- TypeScript
- Tailwind CSS
- shadcn/ui

Responsibilities:

- Tenant dashboard
- Notification history
- Delivery status
- Templates
- User preferences
- API-key management
- Analytics
- Queue/DLQ visibility
- In-app notification inbox

The dashboard communicates with the API server rather than directly accessing PostgreSQL, Redis, or Kafka.

---

### 4.3 PostgreSQL

PostgreSQL is the durable source of truth.

Core entities:

```text
Tenant
User
ApiKey
Notification
NotificationDelivery
NotificationTemplate
NotificationPreference
Device
ScheduledNotification
RetryRecord
UsageRecord
InAppNotification
```

Important principles:

- Every tenant-owned table must contain `tenant_id` either directly or through a clearly defined relationship.
- Database queries must always enforce tenant boundaries.
- Notification status transitions should be transactionally safe.
- Durable state must not depend exclusively on Redis.

---

### 4.4 Redis

Redis responsibilities:

#### Rate limiting

Examples:

```text
tenant:{tenantId}:rate
api:{apiKey}:rate
```

#### Idempotency

```text
idempotency:{tenantId}:{key}
```

#### Caching

Potential cache targets:

- Templates
- Notification preferences
- Tenant configuration

#### Distributed locks

Used where multiple scheduler instances could otherwise process the same scheduled item.

Redis data should have appropriate TTLs.

---

### 4.5 Kafka

Kafka is the asynchronous event backbone.

Initial logical topics:

```text
notification.email
notification.push
notification.sms
notification.inapp

notification.retry
notification.dlq
```

Kafka consumer groups:

```text
email-workers
push-workers
sms-workers
inapp-workers
scheduler-workers
```

The exact topic/partition strategy can evolve after load testing.

Kafka message payloads should include a stable event/notification identifier.

Example:

```json
{
  "eventId": "evt_123",
  "notificationId": "ntf_123",
  "tenantId": "tenant_123",
  "userId": "usr_123",
  "channel": "EMAIL",
  "templateId": "order-shipped",
  "payload": {
    "orderId": "ORD-123"
  },
  "attempt": 1,
  "createdAt": "2026-09-16T10:00:00Z"
}
```

---

## 5. Worker Architecture

Workers are stateless processes.

Each worker:

1. Consumes a Kafka message.
2. Validates the event.
3. Checks idempotency.
4. Loads required data.
5. Renders the template if needed.
6. Applies channel-specific logic.
7. Calls the provider or persists in-app delivery.
8. Updates delivery state.
9. Commits/acknowledges the Kafka message only after the processing decision is durable.

Workers must safely tolerate duplicate messages.

---

## 6. Email Worker

Flow:

```text
Kafka
  ↓
Email Worker
  ↓
Idempotency check
  ↓
Load template/preferences
  ↓
Render email
  ↓
Email Provider Adapter
  ↓
Provider
  ↓
Update delivery state
```

Provider abstraction:

```text
EmailProvider
├── send(message)
└── getStatus(messageId)
```

Initial provider may be mocked in development and replaced by a real provider in production.

---

## 7. Push Worker

Flow:

```text
Kafka
  ↓
Push Worker
  ↓
Device token lookup
  ↓
Push Provider Adapter
  ↓
FCM/APNs
  ↓
Delivery result
```

The system should support invalid/expired device tokens.

---

## 8. SMS Worker

Flow:

```text
Kafka
  ↓
SMS Worker
  ↓
SMS Provider Adapter
  ↓
Twilio
  ↓
Delivery result
```

SMS should respect user preferences and tenant-level limits.

---

## 9. In-App Notifications

In-app notifications differ from external channels because NotifyX owns the final storage.

Flow:

```text
Kafka
  ↓
In-App Worker
  ↓
PostgreSQL
  ↓
In-App Notification Inbox
  ↓
Dashboard / Client
```

Core operations:

```text
Create notification
List unread notifications
Mark one as read
Mark all as read
Delete/archive notification
```

Future enhancement:

```text
WebSocket / SSE
     ↓
Real-time in-app notification
```

This should be treated as an enhancement after the durable notification flow works.

---

## 10. Notification Lifecycle

A notification can move through states such as:

```text
PENDING
  ↓
QUEUED
  ↓
PROCESSING
  ├──────────────→ SENT / DELIVERED
  │
  └→ RETRY_SCHEDULED
          ↓
       PROCESSING
          │
          └→ FAILED / DLQ
```

For in-app notifications:

```text
QUEUED
  ↓
DELIVERED
  ↓
READ
```

The exact enum should be finalized during schema design.

---

## 11. Idempotency

NotifyX uses at-least-once message processing.

Therefore:

```text
same Kafka message
       ↓
worker
       ↓
must not produce duplicate side effects
```

Every notification delivery should have a stable identifier.

Example:

```text
notificationId = ntf_123
deliveryId     = del_123_email
```

A worker should check whether the delivery has already reached a terminal state before performing the provider side effect.

Important interview problem:

```text
Provider accepts request
       ↓
Worker crashes
       ↓
Kafka message is redelivered
```

This creates an ambiguity: the provider may already have sent the notification.

The system must document its delivery guarantee honestly. It should not claim "exactly once delivery" merely because a database row is unique.

---

## 12. Retry Strategy

Retry only transient failures.

Examples:

Retryable:

- HTTP 429
- Provider 5xx
- Temporary network failure
- Timeout

Usually non-retryable:

- Invalid recipient
- Invalid API key
- Invalid template
- Permanent provider rejection

Example backoff:

```text
attempt 1 → 0s
attempt 2 → 5s
attempt 3 → 30s
attempt 4 → 2m
attempt 5 → DLQ
```

Use jitter to reduce synchronized retry spikes.

---

## 13. Dead Letter Queue

After maximum retry attempts:

```text
notification
     ↓
retry exhausted
     ↓
Kafka DLQ
     ↓
dashboard
```

The dashboard should allow authorized users to:

- inspect the event
- inspect failure reason
- retry/replay
- discard the event

Replay must create safe behavior under idempotency rules.

---

## 14. Priority

Notifications can have priorities:

```text
CRITICAL
HIGH
NORMAL
LOW
```

Priority should affect processing order without destroying fairness.

A possible implementation is separate Kafka topics:

```text
notification.high
notification.normal
notification.low
```

This decision should be validated through load testing before finalizing.

---

## 15. Rate Limiting

Rate limiting should operate at multiple levels:

```text
API request rate
Tenant notification rate
Channel rate
```

Redis can implement a token-bucket or sliding-window strategy.

Example:

```text
Tenant A
1000 notification requests / second

Tenant B
100 notification requests / second
```

A tenant exceeding its quota receives a controlled response rather than overwhelming the system.

---

## 16. Scheduled Notifications

A scheduled notification is stored durably:

```text
scheduled_at
status = SCHEDULED
```

The scheduler periodically finds due records and publishes them to Kafka.

Multiple scheduler instances must not enqueue the same notification.

Options:

- PostgreSQL row locking
- Redis distributed lock
- claim-and-update transaction

Start with PostgreSQL transactional claiming for correctness; introduce Redis locking only where it solves a demonstrated problem.

---

## 17. Multi-Tenancy

NotifyX is designed as a SaaS infrastructure platform.

Example:

```text
Tenant A
 ├── users
 ├── templates
 ├── API keys
 └── notifications

Tenant B
 ├── users
 ├── templates
 ├── API keys
 └── notifications
```

Requirements:

- Tenant-scoped API keys
- Tenant-scoped data
- Tenant-level rate limits
- Tenant usage metrics
- No cross-tenant reads/writes
- Tenant-aware logging and tracing

---

## 18. API Authentication

Use API keys for client-to-NotifyX communication.

Example:

```http
Authorization: Bearer nx_live_xxxxxxxxx
```

Store only a secure hash of the secret where practical.

Support:

- key creation
- key listing
- key revocation
- key rotation
- environment distinction

Example:

```text
nx_test_...
nx_live_...
```

---

## 19. Notification Templates

Templates are reusable.

Example:

```text
Template: ORDER_SHIPPED

Subject:
Your order {{orderId}} has shipped

Body:
Hi {{name}},
Your order {{orderId}} is on the way.
```

The rendering system must validate required variables.

Templates should support channel-specific content.

---

## 20. User Preferences

Users can control channels and notification categories.

Example:

```text
Security       Email ✓ Push ✓ SMS ✓
Marketing      Email ✗ Push ✗ SMS ✗
Order Updates  Email ✓ Push ✓ SMS ✗
```

Critical/security notifications may have different policy rules, which should be explicitly configured rather than hardcoded.

---

## 21. Deduplication and Aggregation

The system may eventually support:

```text
10 separate "liked your post" events
             ↓
      aggregation window
             ↓
"Monish and 9 others liked your post."
```

This is an advanced feature and should be implemented after the core delivery pipeline is stable.

---

## 22. Observability

Use:

- OpenTelemetry
- Prometheus
- Grafana

Metrics:

```text
notifications_created_total
notifications_processed_total
notifications_delivered_total
notifications_failed_total
notification_retry_total
notification_dlq_total

kafka_consumer_lag
worker_processing_duration
provider_latency
api_request_duration
redis_latency
database_query_duration
```

Every request/event should carry a correlation/trace identifier where possible.

---

## 23. Docker Architecture

All application and infrastructure components should run locally through Docker Compose.

Conceptual services:

```text
api
dashboard
email-worker
push-worker
sms-worker
inapp-worker
scheduler

postgres
redis
kafka

prometheus
grafana
```

Development command:

```bash
docker compose up
```

Each application service should have its own Dockerfile.

Production images should:

- use a small runtime image
- run as a non-root user where practical
- avoid development dependencies
- expose only required ports
- use environment variables/secrets
- provide health checks

---

## 24. Docker Networking

Containers communicate using Docker Compose service names.

Example:

```text
api → postgres:5432
api → redis:6379
api → kafka:9092
email-worker → kafka:9092
```

The host does not need every internal service port exposed.

Only externally required ports should be published.

---

## 25. Cloud Deployment

Initial cloud target:

```text
AWS
```

Possible mapping:

```text
Application containers → EC2
PostgreSQL             → RDS
Redis                  → ElastiCache
Object/static assets   → S3
CDN                    → CloudFront
IAM                    → access control
```

For the first deployment, Docker Compose on an EC2 instance is acceptable.

The architecture should remain portable enough that Kubernetes/ECS could be introduced later without redesigning the application domain.

---

## 26. Scaling Strategy

Scale stateless workers independently.

Example:

```text
Email traffic increases
        ↓
Run more email-worker containers
        ↓
Kafka consumer group distributes partitions
```

Similarly:

```text
Push traffic increases
        ↓
Scale push workers
```

API scaling:

```text
Load Balancer
     ↓
API container 1
API container 2
API container 3
```

Because API servers are stateless, requests can be distributed across instances.

---

## 27. Failure Scenarios to Design For

The system must explicitly handle:

### Kafka unavailable

API should fail safely or use an appropriate durable outbox strategy rather than falsely reporting that a notification was queued.

### Redis unavailable

Critical durable operations should continue where possible; rate limiting/cache features need defined fallback behavior.

### PostgreSQL unavailable

Do not report durable notification creation as successful if the authoritative transaction cannot be committed.

### Provider unavailable

Retry transient errors with backoff.

### Worker crashes

Kafka redelivery should be safe through idempotency.

### Duplicate events

Deduplicate based on stable identifiers.

### Provider timeout after accepting request

Treat delivery state as potentially ambiguous; never claim exactly-once external delivery.

### Traffic spike

Kafka buffers work; workers scale horizontally.

### Poison message

After bounded retries, move to DLQ.

---

## 28. Outbox Pattern — Planned Advanced Enhancement

A key consistency problem exists here:

```text
PostgreSQL transaction
        +
Kafka publish
```

If PostgreSQL commits but Kafka publishing fails, the notification may exist in the database but never enter the queue.

A robust enhancement is the **Transactional Outbox Pattern**:

```text
API
 ↓
PostgreSQL Transaction
 ├── notification row
 └── outbox event row
        ↓
Outbox Publisher
        ↓
Kafka
```

The outbox publisher retries until the event is published.

This should be part of the advanced version because it creates an excellent system-design discussion around database/Kafka consistency.

---

## 29. Security

Requirements:

- API authentication
- Tenant authorization
- Input validation
- Rate limiting
- Secure API-key storage
- HTTPS in production
- Security headers
- Secrets outside source control
- Structured audit logs
- No sensitive provider credentials in logs
- Payload size limits

---

## 30. Testing Strategy

### Unit tests

- template rendering
- retry calculation
- preference resolution
- rate limiting
- idempotency logic

### Integration tests

- API + PostgreSQL
- API + Redis
- producer + Kafka
- workers + provider mocks

### End-to-end tests

```text
API request
 ↓
Kafka
 ↓
Worker
 ↓
Provider mock
 ↓
PostgreSQL
```

### Failure tests

- provider timeout
- provider 500
- provider 429
- worker crash
- duplicate event
- Kafka restart
- Redis restart

### Load tests

Use a tool such as k6 to measure:

- API throughput
- queue throughput
- worker throughput
- p95/p99 latency
- behavior under spikes

---

## 31. Architecture Evolution

### V1

```text
API + PostgreSQL
```

### V2

```text
+ Redis
```

### V3

```text
+ Kafka
+ Email Worker
```

### V4

```text
+ Push Worker
+ SMS Worker
+ In-App Worker
```

### V5

```text
+ Retry
+ DLQ
+ Idempotency
+ Rate limiting
```

### V6

```text
+ Scheduling
+ Multi-tenancy
+ API keys
+ Templates
+ Preferences
```

### V7

```text
+ Docker Compose
+ AWS
```

### V8

```text
+ OpenTelemetry
+ Prometheus
+ Grafana
```

### V9

```text
+ Outbox
+ Load testing
+ Failure testing
+ Advanced scaling
```

---

## 32. Non-Goals for Initial Version

Do not add these before the core system works:

- Kubernetes
- Complex service mesh
- Multi-region deployment
- Global active-active architecture
- Custom Kafka cluster management
- ML-based notification optimization
- Complex workflow engine

The objective is a technically deep but understandable system.

---

## 33. Coding Assistant Rules

Any coding assistant working on NotifyX should:

1. Read `prd.md`, `system_overview.md`, and this file before making architectural changes.
2. Do not introduce a new infrastructure technology without a documented reason.
3. Do not replace Kafka, PostgreSQL, Redis, or Docker without explicit approval.
4. Preserve tenant isolation.
5. Preserve idempotency.
6. Do not perform provider calls from the API server.
7. Keep workers independently scalable.
8. Keep secrets out of source control.
9. Add tests for non-trivial business logic.
10. Update documentation when an architectural decision changes.
11. Prefer incremental changes over large rewrites.
12. Never claim exactly-once external notification delivery unless it is actually proven.
13. Load testing and failure drills are executed via `tools/load-test` and `scripts/run-failure-drills.js`. See `docs/load_testing_and_resilience.md`.

---

## 34. Phase 8 Reliability and Failure Engineering

Phase 8 introduces empirical load testing and failure engineering:
- **Load Test Tooling**: `tools/load-test` providing baseline, sustained, burst, retry-storm, and duplicate event scenarios with HDR/reservoir percentile tracking.
- **Transactional Outbox Resilience**: Kafka outage drills verify that PostgreSQL continues to accept notifications with atomic outbox records; outbox publishers resume publishing upon Kafka reconnection.
- **Fail-Fast Semantics**: PostgreSQL outage drills verify that API fails fast and does not return false 202 status codes.
- **At-Least-Once Delivery**: Worker crash drills verify that uncommitted Kafka messages are safely redelivered and deduplicated internally via PostgreSQL unique constraints.
- **Reference**: Detailed experimental procedures, empirical numbers, and commands are documented in `docs/load_testing_and_resilience.md`.

---

## 35. Phase 9 AWS Deployment Architecture

Phase 9 introduces a production-oriented AWS deployment architecture:
- **Deployment Target**: Private EC2 instance running Docker Compose (`docker-compose.prod.yml`) behind an Internet-Facing ALB.
- **Public Services**: Only ALB is internet-facing. Dashboard and API receive traffic via ALB TLS termination; ports bound to `127.0.0.1` on the EC2 host.
- **Private Services**: PostgreSQL (Amazon RDS), Redis (ElastiCache), Kafka (Private KRaft EC2 + EBS), Outbox Publisher, and all 4 Channel Workers — all strictly in private subnets.
- **Production Config Validation**: `packages/config` rejects mock providers, localhost URLs, and missing credentials when `NODE_ENV=production`.
- **Docker Hardening**: All images run as non-root users (`notifyx` UID 1001) with lean multi-stage builds.
- **ECR Registry**: Immutable Git SHA tags for all images via `scripts/build-and-push-ecr.js`.
- **CI/CD**: GitHub Actions CI on all PRs; manual `workflow_dispatch` deploy to staging (GitHub OIDC, no static AWS keys).
- **Reference**: Full architecture, VPC, networking, IAM, secrets, and deployment procedures documented in `docs/aws_deployment.md`.

---

## 36. Phase 10 Notification Product Layer

Phase 10 introduces the core developer and product abstractions on top of the notification engine:
- **Template System & Immutable Versioning**: Separation of `NotificationTemplate` from `NotificationTemplateVersion`. Promotion uses row-level locking (`SELECT ... FOR UPDATE`) with explicit `version.templateId === lockedTemplate.id && lockedTemplate.tenantId === tenantId` validation.
- **Safe Template Rendering**: Safe mustache interpolation (`{{user.name}}`) and missing-variable rejection without `eval()` or arbitrary code execution.
- **User Notification Preferences**: Granular channel and category matrix (`TRANSACTIONAL`, `SECURITY`, `SYSTEM`, `MARKETING`). Transactional/Security/System enabled by default; Marketing disabled by default.
- **Header-Only Idempotency**: Strict `Idempotency-Key` header with SHA-256 canonical hashing and 24h TTL. Invariant: *"exactly one NotifyX Notification record for the same tenant + idempotency key."*
- **Scheduled Notifications & Scheduler Service**: Standalone service (`apps/scheduler`, port 3007) polling due notifications via `FOR UPDATE SKIP LOCKED`. Converts jobs into `NotificationDelivery` and `OutboxEvent` records with status `ScheduledNotification.DISPATCHED`. Zero direct DB-to-Kafka bypass.
- **Non-Authoritative Redis Cache Tier**: Non-blocking cache-aside pattern with automatic fallback to PostgreSQL on cache miss or Redis outage.
- **Reference**: Full specifications, API endpoints, schema, and concurrency guarantees documented in `docs/notification_product_features.md`.

---

## 37. Phase 11 Multi-Tenant Quotas, Rate Limiting & Backpressure

Phase 11 introduces comprehensive rate-limiting, quota enforcement, and backpressure controls:
- **Distributed Token Bucket API Limiting**: Atomic Redis Lua script managing requests/second and burst capacity per tenant (`ratelimit:tenant:{tenantId}:api`) with standard `X-RateLimit-*` and `Retry-After` headers.
- **Pre-DB Quota Enforcement**: Notification volume limits (`notificationsPerMinute` and `notificationsPerDay`) are evaluated prior to database writes, ensuring 0 orphaned records on HTTP 429 rejection.
- **Ephemeral vs. Durable Separation**: PostgreSQL stores durable tenant quota configurations; Redis manages ephemeral rate-limit tokens and sliding usage counters.
- **Concurrency-Safe Fallback**: Atomic transactional upsert and rollback pattern on `usage_quota_counters` during Redis degradation (no naive count-then-insert).
- **Worker Bounded Concurrency & Backpressure**: Asynchronous `Semaphore` controlling active in-flight worker executions across all channels (`EMAIL_WORKER_CONCURRENCY`, `PUSH_WORKER_CONCURRENCY`, etc.) with Kafka offset commits strictly postponed until post-execution.
- **Provider Throttling**: Downstream provider HTTP 429 responses are captured as `RATE_LIMIT` errors and routed through exponential backoff retry machinery.
- **Bounded Metric Cardinality**: Aggregate Prometheus gauges (`rate_limit_remaining`) strictly excluding tenant IDs to preserve metric infrastructure scalability.
- **Reference**: Full specifications and operational guidelines documented in `docs/rate_limiting_and_backpressure.md`.

