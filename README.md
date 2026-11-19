# Distributed Job Queue

A background job processing system built on **Redis Streams** and **PostgreSQL**. An HTTP API accepts jobs, and a pool of worker processes runs them with automatic retries, exponential backoff, a dead-letter queue, crash recovery, and idempotent processing.

**Stack:** TypeScript · Node.js · Express · Redis 7 (Streams, consumer groups, Lua) · PostgreSQL 17 · Docker Compose · Vitest · GitHub Actions

```
                    POST /jobs
  client ──────────────► API ──── 1. INSERT job row ────────────► PostgreSQL
                          │                                       (job state + event history)
                          └────── 2. XADD / ZADD ──┐                   ▲
                                                   ▼                   │ state transitions
                 ┌──────────── Redis ───────────────────────┐          │
                 │  jq:stream   ready jobs (consumer group) │◄──┐      │
                 │  jq:delayed  retries + scheduled (ZSET)  │───┘ promote (Lua, Redis clock)
                 │  jq:dlq      dead-lettered jobs          │          │
                 │  jq:done:*   idempotency markers (TTL)   │          │
                 │  jq:lock:*   execution leases (TTL)      │          │
                 └──────────────────────────────────────────┘          │
                        │ XREADGROUP / XAUTOCLAIM                      │
                        ▼                                              │
              worker 1 · worker 2 · worker N  ─────────────────────────┘
```

## Quick start

```bash
docker compose up --build --scale worker=3
```

That brings up Redis, Postgres, the API on `:3000`, three workers, and a deliberately flaky webhook receiver on `:4000` that fails 30% of requests.

```bash
# A synthetic job that fails its first two attempts, then succeeds
curl -s -XPOST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"type":"sleep","payload":{"ms":50,"failTimes":2}}'

# A real job: deliver a webhook notification, 10 seconds from now
curl -s -XPOST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"type":"webhook.deliver","delayMs":10000,
       "payload":{"url":"http://sink:4000/hook","event":"order.shipped","data":{"orderId":42}}}'

curl -s localhost:3000/jobs/<id>     # status + full event history
curl -s localhost:3000/stats         # queue depth, in-flight, delayed, DLQ, counters
curl -s localhost:3000/dlq           # inspect dead-lettered jobs
curl -s -XPOST localhost:3000/dlq/<id>/replay
```

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Enqueue `{ type, payload, maxAttempts?, delayMs? }`. Optional `Idempotency-Key` header. Returns `202` with the job ID. |
| `GET` | `/jobs/:id` | Job row plus every state transition (which worker, which attempt, what error). |
| `GET` | `/jobs?status=&type=&limit=` | Query job history. |
| `GET` | `/stats` | Ready / in-flight / delayed / dead-lettered counts and processing counters. |
| `GET` | `/dlq` | List dead-lettered jobs with their last error. |
| `POST` | `/dlq/:id/replay` | Move a job out of the DLQ and back onto the queue with a fresh attempt budget. |
| `GET` | `/health` | Checks Redis and Postgres. |

Job types live in [`src/jobs/handlers.ts`](src/jobs/handlers.ts). Each one is a zod schema plus a `run` function. The API validates payloads against the schema before accepting a job, so bad input returns a `400` instead of burning through retries.

- **`sleep`**: synthetic job for exercising the plumbing (`ms`, `failRate`, `failTimes`).
- **`webhook.deliver`**: POSTs a notification to a URL. 5xx, 408, 429 and network errors are retried. Other 4xx responses are permanent and go straight to the DLQ. Combined with `delayMs`, this works as a scheduled notification service.

## How it works

### Job lifecycle

```
scheduled ─┐
           ▼
pending ──► processing ──► succeeded
              │    ▲
              ▼    │ backoff elapsed
           retrying ┘
              │ attempts exhausted, or non-retryable error
              ▼
            failed (in DLQ) ── replay ──► pending
```

Every transition is written to `job_events` in the same SQL statement that updates `jobs`, so `GET /jobs/:id` shows the full history.

### Why Redis Streams

A plain list (`LPUSH`/`BRPOP`) removes a message the moment a worker takes it. If that worker dies mid-job, the job is gone. Streams with a consumer group keep every delivered message in a **pending entries list** until the worker explicitly `XACK`s it. That gives at-least-once delivery, and makes crash recovery possible: `XAUTOCLAIM` lets a healthy worker take over messages that have sat unacknowledged for too long.

Acked messages are also `XDEL`ed, so `XLEN` of the stream is the real backlog.

### Retries with exponential backoff

A retryable failure doesn't requeue immediately. The next attempt goes into a sorted set (`jq:delayed`) scored by the time it should run. Each worker promotes due entries back onto the stream every 250 ms.

- **Delay:** `base × 2^(attempt−1)`, capped (1s, 2s, 4s, 8s ... up to 60s by default).
- **Jitter:** "equal jitter": half the delay is fixed and half is random. When a downstream service has an outage, every job that failed in the same second would otherwise retry in the same second and hit it again together.
- **Atomic hand-off:** scheduling the retry and acking the failed delivery happen in one Lua script. A crash can't leave the job both pending and scheduled, or lose it in between.
- **Atomic promotion:** the promote step is also a Lua script, so two workers promoting at the same moment can't both enqueue the same job.
- **Single clock:** both scheduling and promotion read time from Redis `TIME` inside the script, never from the worker's `Date.now()`. With multiple hosts, clocks drift, and if the process that schedules a retry and the one that promotes it disagree by minutes, the retry fires minutes late. This happened during development: the Docker VM clock drifted 15 minutes from the host and retries stalled for exactly 15 minutes. A test now skews the process clock by an hour and checks that retries still fire on time.
- **Permanent vs. transient errors:** handlers throw `NonRetryableError` for failures that will never succeed (invalid payload, HTTP 422). Those skip retries entirely.

### Dead-letter queue

After `maxAttempts` (default 5), or on a non-retryable error, the job moves to the `jq:dlq` stream with its last error and is marked `failed` in Postgres. `GET /dlq` lists them. `POST /dlq/:id/replay` removes the entry and re-enqueues it with `attempt = 1`, so after fixing whatever broke you can push the jobs through again.

A **poison pill** is a message that crashes the worker every time, so it never gets acked and never reaches the retry logic. It would bounce between workers forever. Reclaim checks the delivery count from `XPENDING`, and anything delivered more than `MAX_DELIVERIES` times goes to the DLQ.

### Idempotency: why it's needed

At-least-once delivery means **some jobs will be delivered more than once**. That's a consequence of the design, not a bug. The common causes:

1. A worker finishes a job, then crashes before `XACK`. The message is still pending, gets reclaimed, and is delivered again.
2. A worker is slow (GC pause, network stall) and exceeds the claim timeout. Another worker reclaims the message while the first one is **still running it**.
3. A client times out on `POST /jobs` and retries, creating a second copy of the same job.
4. An operator replays a job from the DLQ that actually succeeded on a side channel.

The defenses, in layers:

| Layer | Mechanism | Covers |
|---|---|---|
| Producer | `Idempotency-Key` header, enforced by a unique index in Postgres. A repeated POST returns the original job. | Client retries (3) |
| Completion marker | After success the worker sets `jq:done:<id>` **before** acking. Every delivery checks it first, and acks and skips if present. Postgres `status = 'succeeded'` is the durable fallback once the marker's TTL expires. | Crash between finish and ack (1), replays (4) |
| Execution lease | `SET jq:lock:<id> <owner> NX PX <ttl>` before running. If another live worker holds it, leave the message alone. Released with a compare-and-delete Lua script so a worker can't delete a lease it already lost. | Concurrent execution by a slow worker (2) |
| Check, lock, check | The done-marker is checked again **after** taking the lease. Without that second check, worker B could check, then A finishes and releases, then B takes the lease and runs the job again. | Race between the first two layers |
| Downstream | `webhook.deliver` sends the job ID as an `Idempotency-Key` header, and the sink dedupes on it. | Crash *after* the side effect but *before* the done-marker is written |

That last row is the honest limit: a worker can't atomically "do the side effect" and "record that it did it" unless both live in the same transactional system. Exactly-once *effects* across a network boundary need the receiver's cooperation. Inside this system the guarantee is at-least-once delivery with effectively-once processing.

### Crash recovery and heartbeats

Every worker runs `XAUTOCLAIM` on startup and every 5 seconds, claiming messages that have been pending longer than `CLAIM_IDLE_MS`. Those messages go through the same idempotency checks, so a job that actually finished is just acked.

"Pending for a long time" has to mean "the worker is dead", not "the job is slow". So while a job runs, its worker sends a **heartbeat** every `LOCK_TTL_MS / 3`. One Lua script extends the lease and resets the message's idle time with `XCLAIM … JUSTID`, which doesn't bump the delivery counter. As a result:

- A slow job on a live worker is never reclaimed, however long it runs (up to `JOB_TIMEOUT_MS`).
- A dead worker stops heartbeating. Its lease expires within `LOCK_TTL_MS` (15s) and its messages become claimable after `CLAIM_IDLE_MS` (30s). Config validation requires `CLAIM_IDLE_MS > LOCK_TTL_MS`, so by the time a message is reclaimed, the dead worker's lease is already gone.

An earlier version used a fixed 60-second lease with no heartbeat. The chaos benchmark caught the problem: when a worker crashed, its *other* in-flight jobs were reclaimed every few seconds, found the dead worker's lease still held, and each attempt counted as another delivery. After 10 of those the poison-pill check dead-lettered perfectly healthy jobs.

On `SIGTERM` a worker stops reading new messages, finishes in-flight jobs, and then exits (graceful shutdown).

### Persistence

Redis holds the queue. Postgres holds the system of record: `jobs` (current state, attempts, result, last error, timings) and `job_events` (append-only history). Migrations run on boot behind a Postgres advisory lock, so the API and N workers starting together don't race.

**Known gap: the dual write.** `POST /jobs` inserts the row and then `XADD`s. If Redis is down between the two, the API marks the job `failed` and returns `503`, so it doesn't silently sit in `pending`. The complete fix is a transactional outbox: write the job and an outbox row in one Postgres transaction, and have a relay publish to Redis.

## Benchmarks

Local run on a laptop (Docker Desktop, 3 workers × 4 consumers, `sleep` jobs of 20 ms):

| Scenario | Result |
|---|---|
| Enqueue rate (API accepting jobs) | ~2,800 jobs/s |
| 2,000 jobs, 20% random transient failure rate | **116 jobs/s** end to end, 17.2 s wall time |
| Jobs that needed at least one retry | 412 of 2,000 |
| Retry success rate (eventually succeeded after failing) | **99.5%**, remaining 2 dead-lettered after 5 attempts |
| p95 handler duration | 23 ms |
| p95 queue-to-done latency, first-try jobs | 3.5 s (the backlog drains at worker capacity) |

**Chaos run:** 2,000 jobs, with each job having a 0.5% chance of killing its worker right after the job completes but before the ACK. Docker restarts the dead workers.

| Metric | Result |
|---|---|
| Worker crashes during the run | 10 |
| Messages reclaimed from dead workers | 29 |
| Redeliveries of already-completed jobs caught by idempotency | **22** |
| Jobs completed more than once | **0** |
| Jobs re-run because their worker died mid-execution | 3 (expected under at-least-once) |
| Succeeded / dead-lettered | 2,000 / 0 |
| Throughput | 192 jobs/s, 10.4 s wall time |

Throughput is measured with the benchmark's monotonic clock. Latency percentiles come from Postgres timestamps. With 2,000 jobs enqueued at once, queue-to-done latency mostly reflects time spent waiting in the backlog.

Reproduce:

```bash
docker compose up -d --build --scale worker=3
npm run bench -- --jobs 2000 --ms 20 --fail-rate 0.2

# Chaos: each job has a 0.5% chance of killing its worker after completion but before ACK
CHAOS_CRASH_RATE=0.005 LOCK_TTL_MS=2000 CLAIM_IDLE_MS=3000 docker compose up -d --scale worker=3 worker
npm run bench -- --jobs 2000 --ms 20 --fail-rate 0
```

## Tests

```bash
docker compose up -d redis postgres
npm test
```

Integration tests run against real Redis and Postgres (each test uses its own key prefix). They cover:

- end-to-end success with a full event history
- producer idempotency (`Idempotency-Key`)
- delayed and scheduled jobs
- retry-then-succeed with the exact transition sequence
- **retries firing on time even when the worker's clock is skewed by an hour**
- DLQ after `maxAttempts`, then replay after a fix
- non-retryable errors skipping retries
- **duplicate delivery of a completed job is skipped** (handler runs once)
- **crash after completion but before ACK** is reclaimed by another worker and not re-run
- **a slow job on a live, heartbeating worker is never reclaimed**
- orphaned messages from a dead consumer get reclaimed and processed
- poison-pill detection via delivery count
- **3 workers × 3 consumers on 150 jobs: every job runs exactly once**

CI runs typecheck, tests (with Redis and Postgres service containers), and a Docker build on every push.

## Configuration

| Env var | Default | |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | |
| `DATABASE_URL` | `postgres://jobs:jobs@localhost:5432/jobs` | |
| `WORKER_CONCURRENCY` | `4` | Consumer loops per worker process |
| `DEFAULT_MAX_ATTEMPTS` | `5` | |
| `BACKOFF_BASE_MS` / `BACKOFF_MAX_MS` | `1000` / `60000` | |
| `JOB_TIMEOUT_MS` | `30000` | Handler is aborted after this |
| `LOCK_TTL_MS` | `15000` | Execution lease, renewed by heartbeat every TTL/3 |
| `CLAIM_IDLE_MS` | `30000` | No heartbeat for this long means orphaned. Must exceed `LOCK_TTL_MS` |
| `MAX_DELIVERIES` | `10` | Poison-pill threshold |
| `DONE_TTL_SEC` | `86400` | Redis idempotency marker lifetime |
| `CHAOS_CRASH_RATE` | `0` | Demo only: crash after done, before ACK |

## Project layout

```
src/
  api.ts          Express routes
  worker.ts       consume loop, idempotency, retries, DLQ, reclaim
  queue.ts        Redis Streams / ZSET / Lua operations
  db.ts           Postgres schema, state transitions, history
  backoff.ts      exponential backoff with jitter
  jobs/handlers.ts  job type definitions
  sink.ts         flaky webhook receiver for demos
scripts/bench.ts  load test + metrics
test/             integration tests
```

## Roadmap

Tracked in [milestones](https://github.com/colby-okazaki/distributed-job-queue/milestones):

- **Week 1:** per-client rate limiting on `POST /jobs` with a Redis token bucket ([#1](https://github.com/colby-okazaki/distributed-job-queue/issues/1))
- **Week 2:** Prometheus metrics ([#2](https://github.com/colby-okazaki/distributed-job-queue/issues/2)) and a Grafana dashboard ([#3](https://github.com/colby-okazaki/distributed-job-queue/issues/3))
- **Week 3:** transactional outbox to close the enqueue dual-write gap ([#4](https://github.com/colby-okazaki/distributed-job-queue/issues/4)), job cancellation ([#5](https://github.com/colby-okazaki/distributed-job-queue/issues/5))
- **Week 4:** AWS deploy on ECS, ElastiCache and RDS ([#6](https://github.com/colby-okazaki/distributed-job-queue/issues/6)), refreshed benchmarks ([#7](https://github.com/colby-okazaki/distributed-job-queue/issues/7))
