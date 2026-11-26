const { v4: uuidv4 }     = require('uuid');
const { getRedisClient } = require('../config/redis');
const { getUserId }      = require('./auth');

const PENDING_TTL_SEC   = parseInt(process.env.IDEMPOTENCY_PENDING_TTL_SECONDS || '30', 10);
const COMPLETED_TTL_SEC = parseInt(process.env.IDEMPOTENCY_TTL_SECONDS || '86400', 10);
const RENEW_SCRIPT      = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], ARGV[2]) end return 0";

/**
 * Idempotency middleware.
 *
 * Scopes keys per authenticated user (`idempotency:{userId}:{key}`).
 * Uses a 30s pending lease (`pending:{token}`) with an in-flight heartbeat
 * renewal so slow requests (>30s) stay protected against duplicate reservations,
 * while crashed processes automatically expire their pending key after 30s.
 */
async function idempotency(req, res, next) {
  const key = req.headers['x-idempotency-key'];

  if (!key || typeof key !== 'string' || key.length > 128) {
    return res.status(400).json({
      error: 'Missing or invalid header: X-Idempotency-Key (max 128 chars)',
    });
  }

  try {
    const redis      = getRedisClient();
    const userId     = getUserId(req) || 'anonymous';
    const redisKey   = `idempotency:${userId}:${key}`;
    const pendingVal = `pending:${uuidv4()}`;

    const set = await redis.set(redisKey, pendingVal, 'NX', 'EX', PENDING_TTL_SEC);

    if (set === null) {
      const cached = await redis.get(redisKey);

      if (cached && !cached.startsWith('pending:')) {
        const { statusCode, body } = JSON.parse(cached);
        return res.status(statusCode).json(body);
      }

      return res.status(409).json({
        error: 'Request with this idempotency key is already being processed. Retry shortly.',
      });
    }

    let finished  = false;
    const renewMs = Math.max(1000, Math.floor((PENDING_TTL_SEC * 1000) / 3));

    const heartbeat = setInterval(() => {
      if (finished) return;
      redis.eval(RENEW_SCRIPT, 1, redisKey, pendingVal, String(PENDING_TTL_SEC)).catch(() => {});
    }, renewMs);
    if (heartbeat.unref) heartbeat.unref();

    const stopHeartbeat = () => {
      if (!finished) {
        finished = true;
        clearInterval(heartbeat);
      }
    };

    res.on('finish', stopHeartbeat);
    res.on('close', stopHeartbeat);

    const originalJson = res.json.bind(res);
    res.json = (body) => {
      stopHeartbeat();
      if (res.statusCode >= 500) {
        redis.del(redisKey).catch(() => {});
      } else {
        redis
          .set(redisKey, JSON.stringify({ statusCode: res.statusCode, body }), 'EX', COMPLETED_TTL_SEC)
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

module.exports = { idempotency, COMPLETED_TTL_SEC };
