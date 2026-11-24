const { getRedisClient } = require('../config/redis');

/**
 * Idempotency middleware.
 *
 * Reads the X-Idempotency-Key header and uses Redis SET NX EX to
 * detect duplicate requests. On a first request the key is set to
 * "pending"; once a response is produced it is overwritten with the
 * JSON-serialised result so subsequent duplicates get the same answer.
 *
 * Returns 400 if the header is missing.
 * Returns the cached response (202 or 409) if the key already has a
 * completed result stored.
 */
async function idempotency(req, res, next) {
  const key = req.headers['x-idempotency-key'];

  if (!key) {
    return res.status(400).json({
      error: 'Missing required header: X-Idempotency-Key',
    });
  }

  const redis    = getRedisClient();
  const redisKey = `idempotency:${key}`;

  // Try to set the key — NX means only if it does NOT already exist
  const set = await redis.set(redisKey, 'pending', 'NX', 'EX', 86400); // 24h TTL

  if (set === null) {
    // Key already existed — return the cached result if complete
    const cached = await redis.get(redisKey);

    if (cached && cached !== 'pending') {
      const { statusCode, body } = JSON.parse(cached);
      return res.status(statusCode).json(body);
    }

    // Still pending (another request in-flight) — tell the client to retry
    return res.status(409).json({
      error: 'Request with this idempotency key is already being processed. Retry shortly.',
    });
  }

  // First time seeing this key — intercept res.json to cache the result
  const originalJson = res.json.bind(res);
  res.json = (body) => {
    // Cache the response for future duplicates (overwrite "pending")
    redis
      .set(redisKey, JSON.stringify({ statusCode: res.statusCode, body }), 'EX', 86400)
      .catch(() => {});
    return originalJson(body);
  };

  next();
}

module.exports = { idempotency };

