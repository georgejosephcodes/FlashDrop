const { getRedisClient } = require('../config/redis');

/**
 * Sliding-window rate limiter per IP.
 * Limit is configurable via RATE_LIMIT_PER_SECOND env var (default 20).
 * Set RATE_LIMIT_PER_SECOND=10000 when running benchmarks locally.
 */
const LIMIT = parseInt(process.env.RATE_LIMIT_PER_SECOND || '20', 10);

async function rateLimiter(req, res, next) {
  try {
    const redis = getRedisClient();
    const ip    = req.ip || req.socket.remoteAddress;
    const key   = `ratelimit:${ip}`;

    const count = await redis.incr(key);

    if (count === 1) {
      await redis.expire(key, 1);
    } else if (count > LIMIT) {
      // Self-heal missing TTL if a crash previously happened between INCR and EXPIRE
      const ttl = await redis.ttl(key);
      if (ttl === -1) {
        await redis.expire(key, 1);
      }
      return res.status(429).json({
        error: `Too many requests. Limit: ${LIMIT} requests per second.`,
      });
    }

    return next();
  } catch (err) {
    console.error('[RateLimiter] Redis error:', err.message);
    return res.status(503).json({ error: 'Service temporarily unavailable' });
  }
}

module.exports = { rateLimiter };
