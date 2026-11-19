import { createRedis } from './redis.js';
import { loadConfig } from './config.js';
import { JobRepo } from './db.js';
import { registry } from './jobs/handlers.js';
import { logger } from './logger.js';
import { Queue } from './queue.js';
import { Worker } from './worker.js';

const config = loadConfig();
const redis = createRedis(config.redisUrl);
const repo = JobRepo.connect(config.databaseUrl);
await repo.migrate();

const worker = new Worker({ config, queue: new Queue(redis, config.keyPrefix, config.consumerGroup), repo, registry });
await worker.start();

let stopping = false;
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, async () => {
    if (stopping) return;
    stopping = true;
    logger.info({ sig }, 'shutting down, finishing in-flight jobs');
    await worker.stop();
    await Promise.all([redis.quit(), repo.close()]);
    process.exit(0);
  });
}
