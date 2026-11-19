/** What actually travels through Redis. Postgres holds the full history. */
export interface JobEnvelope {
  id: string;
  type: string;
  payload: unknown;
  /** 1-based attempt number this delivery represents. */
  attempt: number;
  maxAttempts: number;
  enqueuedAt: number;
  lastError?: string;
  failedAt?: number;
}

export type JobStatus = 'scheduled' | 'pending' | 'processing' | 'retrying' | 'succeeded' | 'failed';

export interface JobRow {
  id: string;
  type: string;
  payload: unknown;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  idempotency_key: string | null;
  result: unknown;
  last_error: string | null;
  run_at: Date | null;
  created_at: Date;
  updated_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
  duration_ms: number | null;
}

export interface JobEvent {
  id: string;
  status: JobStatus | 'replayed';
  attempt: number;
  worker: string | null;
  error: string | null;
  at: Date;
}
