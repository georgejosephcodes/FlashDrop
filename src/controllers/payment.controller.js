/**
 * payment.controller.js
 *
 * Simulated payment endpoint — no real payment gateway needed for demo.
 *
 * Flow:
 *  1. Validate :id is a UUID and identify authenticated user via getUserId(req)
 *  2. Run claim_hold.lua atomically (verifies ownership, checks deadline, DEL hold + ZREM expiry)
 *  3. Return 403 if non-owner, 410 if missing/expired
 *  4. Publish OrderConfirmed to Redis Stream → dbSyncWorker persists CONFIRMED
 *  5. Publish to Pub/Sub → socketService pushes order:confirmed to client
 *  6. Return 200
 */

const { z }                     = require('zod');
const { getRedisClient }        = require('../config/redis');
const { getUserId }             = require('../middleware/auth');
const { claimHold }             = require('../services/redisService');
const { publishOrderConfirmed } = require('../services/streamService');

const uuidSchema = z.string().uuid();

async function pay(req, res) {
  try {
    const { id: orderId } = req.params;
    if (!uuidSchema.safeParse(orderId).success) {
      return res.status(400).json({ error: 'Invalid order ID format' });
    }

    const callerUserId = getUserId(req);
    if (!callerUserId) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // 1. Atomically verify ownership, deadline, and consume the hold
    const result = await claimHold(orderId, callerUserId);

    if (result.status === 'FORBIDDEN') {
      return res.status(403).json({
        error: 'Forbidden — you do not own this order reservation.',
      });
    }

    if (result.status === 'NOT_FOUND' || result.status === 'EXPIRED') {
      return res.status(410).json({
        status:  'EXPIRED',
        message: 'Reservation has expired or was already processed. Please start a new order.',
      });
    }

    const { userId, itemId } = result;
    const confirmedAt = new Date().toISOString();
    const redis       = getRedisClient();

    // 2. Publish confirmed event → dbSyncWorker will UPSERT status=CONFIRMED
    await publishOrderConfirmed({ orderId, userId, itemId });

    // 3. Push real-time notification to client
    await redis.publish('order:events', JSON.stringify({
      type: 'order:confirmed',
      orderId,
      userId,
      confirmedAt,
    }));

    console.log(`[Payment] Confirmed orderId=${orderId} userId=${userId}`);

    return res.status(200).json({
      status: 'CONFIRMED',
      orderId,
      confirmedAt,
    });

  } catch (err) {
    console.error('[Payment] Error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = { pay };
