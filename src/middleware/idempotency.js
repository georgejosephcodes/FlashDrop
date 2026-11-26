const { getRedisClient } = require('../config/redis');
const { getUserId }      = require('./auth');

/**
 * Idempotency middleware.
 *
 * Scopes keys per authenticated user (`idempotency:{userId}:{key}`) so one
 * user's key can never replay another user's orderId. Uses a 30s TTL while
 * "pending" so crashes don't lock keys for 24h, extending to 24h once complete.
 */
async function idempotency(req, res, next) {
  const key = req.headers['x-idempotency-key'];

  if (!key || typeof key !== 'string' || key.length > 128) {
    return res.status(400).json({
      error: 'Missing or invalid header: X-Idempotency-Key (max 128 chars)',
    });
  }

  try {
    const redis    = getRedisClient();
    const userId   = getUserId(req) || 'anonymous';
    const redisKey = `idempotency:${userId}:${key}`;

    const set = await redis.set(redisKey, 'pending', 'NX', 'EX', 30);

    if (set === null) {
      const cached = await redis.get(redisKey);

      if (cached && cached !== 'pending') {
        const { statusCode, body } = JSON.parse(cached);
        return res.status(statusCode).json(body);
      }

      return res.status(409).json({
        error: 'Request with this idempotency key is already being processed. Retry shortly.',
      });
    }

    const originalJson = res.json.bind(res);
    res.json = (body) => {
      if (res.statusCode >= 500) {
        redis.del(redisKey).catch(() => {});
      } else {
        redis
          .set(redisKey, JSON.stringify({ statusCode: res.statusCode, body }), 'EX', 86400)
          .catch(() => {});
      }
      return originalJson(body);
    };

    return next();
  } catch (err) {
    console.error('[Idempotency] Redis error:', err.message);
    return res.status(503).json({ error: 'Service temporarily unavailable' });
  }
}

module.exports = { idempotency };
