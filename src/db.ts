import pg from 'pg';
import type { JobEvent, JobRow, JobStatus } from './types.js';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  id              uuid PRIMARY KEY,
  type            text NOT NULL,
  payload         jsonb NOT NULL,
  status          text NOT NULL CHECK (status IN ('scheduled','pending','processing','retrying','succeeded','failed')),
  attempts        int  NOT NULL DEFAULT 0,
  max_attempts    int  NOT NULL,
  idempotency_key text UNIQUE,
  result          jsonb,
  last_error      text,
  run_at          timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  started_at      timestamptz,
  finished_at     timestamptz,
  duration_ms     int
);
CREATE INDEX IF NOT EXISTS jobs_status_created_idx ON jobs (status, created_at DESC);

CREATE TABLE IF NOT EXISTS job_events (
  id      bigserial PRIMARY KEY,
  job_id  uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  status  text NOT NULL,
  attempt int  NOT NULL,
  worker  text,
  error   text,
  at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, id);
`;

export interface NewJob {
  id: string;
  type: string;
  payload: unknown;
  maxAttempts: number;
  idempotencyKey?: string;
  runAt?: Date;
}

export interface TransitionExtras {
  worker?: string;
  error?: string;
  result?: unknown;
  durationMs?: number;
  runAt?: Date;
}

export class JobRepo {
  constructor(readonly pool: pg.Pool) {}

  static connect(databaseUrl: string): JobRepo {
    return new JobRepo(new pg.Pool({ connectionString: databaseUrl, max: 10 }));
  }

  /** API and workers all migrate on boot; an advisory lock stops them racing each other. */
  async migrate(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('SELECT pg_advisory_lock(727274)');
      await client.query(SCHEMA);
    } finally {
      await client.query('SELECT pg_advisory_unlock(727274)').catch(() => {});
      client.release();
    }
  }

  /**
   * Insert a job. With an Idempotency-Key, a repeated POST returns the
   * original job instead of creating (and running) a second one.
   */
  async create(job: NewJob): Promise<{ job: JobRow; created: boolean }> {
    const status: JobStatus = job.runAt ? 'scheduled' : 'pending';
    const { rows } = await this.pool.query<JobRow>(
      `WITH ins AS (
         INSERT INTO jobs (id, type, payload, status, max_attempts, idempotency_key, run_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING *
       ), ev AS (
         INSERT INTO job_events (job_id, status, attempt) SELECT id, status, 0 FROM ins
       )
       SELECT * FROM ins`,
      [job.id, job.type, JSON.stringify(job.payload), status, job.maxAttempts, job.idempotencyKey ?? null, job.runAt ?? null],
    );
    if (rows[0]) return { job: rows[0], created: true };
    const existing = await this.pool.query<JobRow>('SELECT * FROM jobs WHERE idempotency_key = $1', [job.idempotencyKey]);
    return { job: existing.rows[0], created: false };
  }

  /**
   * Record a state change and append it to the job's event log in one
   * statement. A job that already succeeded is never moved again: that makes
   * Postgres the durable "already processed" record once the Redis marker's
   * TTL has expired.
   */
  async transition(
    id: string,
    status: JobStatus,
    attempt: number,
    x: TransitionExtras = {},
    eventStatus: string = status,
  ): Promise<boolean> {
    const { rowCount } = await this.pool.query(
      `WITH u AS (
         UPDATE jobs SET
           status      = $2,
           attempts    = $3,
           last_error  = COALESCE($5, last_error),
           result      = COALESCE($6::jsonb, result),
           duration_ms = COALESCE($7, duration_ms),
           run_at      = COALESCE($8, run_at),
           started_at  = CASE WHEN $2 = 'processing' THEN now() ELSE started_at END,
           finished_at = CASE WHEN $2 IN ('succeeded','failed') THEN now()
                              WHEN $2 = 'pending' THEN NULL ELSE finished_at END,
           updated_at  = now()
         WHERE id = $1 AND status <> 'succeeded'
         RETURNING id
       )
       INSERT INTO job_events (job_id, status, attempt, worker, error)
       SELECT id, $9, $3, $4, $5 FROM u`,
      [
        id,
        status,
        attempt,
        x.worker ?? null,
        x.error ?? null,
        x.result === undefined ? null : JSON.stringify(x.result),
        x.durationMs ?? null,
        x.runAt ?? null,
        eventStatus,
      ],
    );
    return (rowCount ?? 0) > 0;
  }

  async isSucceeded(id: string): Promise<boolean> {
    const { rows } = await this.pool.query("SELECT 1 FROM jobs WHERE id = $1 AND status = 'succeeded'", [id]);
    return rows.length > 0;
  }

  async get(id: string): Promise<(JobRow & { events: JobEvent[] }) | null> {
    const { rows } = await this.pool.query<JobRow>('SELECT * FROM jobs WHERE id = $1', [id]);
    if (!rows[0]) return null;
    const ev = await this.pool.query<JobEvent>(
      'SELECT id, status, attempt, worker, error, at FROM job_events WHERE job_id = $1 ORDER BY id',
      [id],
    );
    return { ...rows[0], events: ev.rows };
  }

  async list(opts: { status?: string; type?: string; limit?: number }): Promise<JobRow[]> {
    const { rows } = await this.pool.query<JobRow>(
      `SELECT * FROM jobs
       WHERE ($1::text IS NULL OR status = $1) AND ($2::text IS NULL OR type = $2)
       ORDER BY created_at DESC LIMIT $3`,
      [opts.status ?? null, opts.type ?? null, Math.min(opts.limit ?? 50, 500)],
    );
    return rows;
  }

  async countByStatus(): Promise<Record<string, number>> {
    const { rows } = await this.pool.query<{ status: string; n: string }>(
      'SELECT status, count(*) AS n FROM jobs GROUP BY status',
    );
    return Object.fromEntries(rows.map((r) => [r.status, Number(r.n)]));
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
