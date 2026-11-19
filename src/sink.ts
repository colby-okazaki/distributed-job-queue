/**
 * A flaky webhook receiver for demos. Fails FAIL_RATE of requests with 503
 * (so you can watch retries/backoff happen) and dedupes by Idempotency-Key
 * (so you can see receiver-side idempotency catch redeliveries).
 */
import express from 'express';
import { logger } from './logger.js';

const port = Number(process.env.PORT ?? 4000);
const failRate = Number(process.env.FAIL_RATE ?? 0.3);
const seen = new Set<string>();
const stats = { requests: 0, failedOnPurpose: 0, delivered: 0, duplicatesIgnored: 0 };

const app = express();
app.use(express.json());

app.post('/hook', (req, res) => {
  stats.requests++;
  const key = req.header('idempotency-key') ?? '';
  if (Math.random() < failRate) {
    stats.failedOnPurpose++;
    return res.status(503).json({ error: 'simulated outage' });
  }
  if (key && seen.has(key)) {
    stats.duplicatesIgnored++;
    return res.status(200).json({ ok: true, duplicate: true });
  }
  if (key) seen.add(key);
  stats.delivered++;
  logger.info({ key, event: req.body?.event, attempt: req.header('x-attempt') }, 'notification delivered');
  res.json({ ok: true });
});

app.post('/reject', (_req, res) => res.status(422).json({ error: 'permanently invalid' }));
app.get('/stats', (_req, res) => res.json({ ...stats, failRate }));

app.listen(port, () => logger.info({ port, failRate }, 'webhook sink listening'));
