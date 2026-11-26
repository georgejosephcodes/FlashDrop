const { getRedisClient } = require('../config/redis');

/**
 * Sliding-window rate limiter per IP.
 * Limit is configurable via RATE_LIMIT_PER_SECOND env var (default 20).
 * Set RATE_LIMIT_PER_SECOND=10000 when running benchmarks locally.
 */
const LIMIT = parseInt(process.env.RATE_LIMIT_PER_SECOND || '20', 10);

async function rateLimiter(req, res, next) {
  const redis = getRedisClient();
  const ip    = req.ip || req.socket.remoteAddress;
  const key   = `ratelimit:${ip}`;

  const count = await redis.incr(key);

  if (count === 1) {
    await redis.expire(key, 1);
  }

  if (count > LIMIT) {
    return res.status(429).json({
      error: `Too many requests. Limit: ${LIMIT} requests per second.`,
    });
  }

  next();
}

module.exports = { rateLimiter };

