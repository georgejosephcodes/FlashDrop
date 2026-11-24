/**
 * payment.controller.js
 *
 * Simulated payment endpoint — no real payment gateway needed for demo.
 * In a production system this would call Stripe/Razorpay and only confirm
 * after the gateway webhook fires. Here we confirm immediately.
 *
 * Flow:
 *  1. Check hold:{orderId} still exists (not expired/already paid)
 *  2. DEL hold key — reservation is consumed
 *  3. ZREM from holds:expiry sorted set — stop autoReclaimWorker from reclaiming it
 *  4. Publish OrderConfirmed to Redis Stream → dbSyncWorker persists it
 *  5. Publish to Pub/Sub → socketService pushes order:confirmed to client
 *  6. Return 200
 */

const { getRedisClient }         = require('../config/redis');
const { publishOrderConfirmed }  = require('../services/streamService');

async function pay(req, res) {
  try {
    const { id: orderId } = req.params;
    const redis           = getRedisClient();
    const holdKey         = `hold:${orderId}`;

    // 1. Check hold exists
    const holdVal = await redis.get(holdKey);

    if (!holdVal) {
      return res.status(410).json({
        status:  'EXPIRED',
        message: 'Reservation has expired or was already processed. Please start a new order.',
      });
    }

    const [userId, itemId] = holdVal.split(':');

    // 2. DEL hold — atomically consume the reservation
    await redis.del(holdKey);

    // 3. Remove from expiry sorted set so autoReclaimWorker ignores it
    await redis.zrem('holds:expiry', orderId);

    // 4. Publish confirmed event → dbSyncWorker will UPSERT status=CONFIRMED
    await publishOrderConfirmed({ orderId, userId, itemId });

    // 5. Push real-time notification to client
    await redis.publish('order:events', JSON.stringify({
      type:        'order:confirmed',
      orderId,
      userId,
      confirmedAt: new Date().toISOString(),
    }));

    console.log(`[Payment] Confirmed orderId=${orderId} userId=${userId}`);

    return res.status(200).json({
      status:      'CONFIRMED',
      orderId,
      confirmedAt: new Date().toISOString(),
    });

  } catch (err) {
    console.error('[Payment] Error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = { pay };

