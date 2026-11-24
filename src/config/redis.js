const Redis = require('ioredis');
const env   = require('./env');

let client;

function getRedisClient() {
  if (!client) {
    client = new Redis(env.REDIS_URL, {
      maxRetriesPerRequest: null,         // required for blocking commands (XREADGROUP)
      retryStrategy(times) {
        const delay = Math.min(times * 100, 3000);
        console.log(`[Redis] Reconnecting... attempt ${times} (delay: ${delay}ms)`);
        return delay;
      },
      lazyConnect: false,
    });

    client.on('connect',   () => console.log('[Redis] Connected'));
    client.on('error',  (err) => console.error('[Redis] Error:', err.message));
  }
  return client;
}

module.exports = { getRedisClient };

