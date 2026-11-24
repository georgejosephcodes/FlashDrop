const { getRedisClient } = require('../config/redis');

/**
 * Sliding-window rate limiter — 20 requests / second per IP.
 * Uses Redis INCR + EXPIRE so the window resets cleanly each second.
 * Returns 429 Too Many Requests when the limit is exceeded.
 */
async function rateLimiter(req, res, next) {
  const redis = getRedisClient();
  const ip    = req.ip || req.socket.remoteAddress;
  const key   = `ratelimit:${ip}`;

  const count = await redis.incr(key);

  if (count === 1) {
    // First hit in this window — set 1-second expiry
    await redis.expire(key, 1);
  }

  if (count > 20) {
    return res.status(429).json({
      error: 'Too many requests. Limit: 20 requests per second.',
    });
  }

  next();
}

module.exports = { rateLimiter };

