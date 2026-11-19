import { createRedis } from './redis.js';
import { createApp } from './api.js';
import { loadConfig } from './config.js';
import { JobRepo } from './db.js';
import { registry } from './jobs/handlers.js';
import { logger } from './logger.js';
import { Queue } from './queue.js';

const config = loadConfig();
const redis = createRedis(config.redisUrl);
const repo = JobRepo.connect(config.databaseUrl);
await repo.migrate();
const queue = new Queue(redis, config.keyPrefix, config.consumerGroup);
await queue.init();

const server = createApp({ config, queue, repo, registry }).listen(config.port, () => {
  logger.info({ port: config.port }, 'api listening');
});

for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, () => {
    server.close(async () => {
      await Promise.all([redis.quit(), repo.close()]);
      process.exit(0);
    });
  });
}
