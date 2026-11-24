const { v4: uuidv4 }       = require('uuid');
const { z }                 = require('zod');
const { reserveStock }      = require('../services/redisService');
const { publishOrderReserved } = require('../services/streamService');
const env                   = require('../config/env');

const checkoutSchema = z.object({
  itemId:      z.string().uuid('itemId must be a valid UUID'),
  amountCents: z.number().int().positive('amountCents must be a positive integer'),
});

/**
 * POST /api/orders/checkout
 *
 * Flow:
 *  1. Validate body (Zod)
 *  2. Run reserve_stock.lua (atomic, O(1))
 *  3. SOLD_OUT → 409
 *  4. SUCCESS  → publish to Redis Stream → 202
 */
async function checkout(req, res) {
  try {
    // 1. Validate
    const parsed = checkoutSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: parsed.error.flatten().fieldErrors });
    }

    const { itemId, amountCents } = parsed.data;
    const userId         = req.auth.userId;
    const idempotencyKey = req.headers['x-idempotency-key'];
    const orderId        = uuidv4();

    // 2. Atomic stock check + decrement (Lua script)
    const result = await reserveStock(itemId);

    // 3. Sold out
    if (result === 'SOLD_OUT') {
      return res.status(409).json({
        status:  'SOLD_OUT',
        message: 'Item inventory has been completely claimed.',
      });
    }

    // 4. Reserved — publish event and return 202
    const holdExpiresAt = new Date(Date.now() + env.HOLD_TTL_SECONDS * 1000);

    await publishOrderReserved({ orderId, userId, itemId, amountCents, idempotencyKey });

    return res.status(202).json({
      status:       'RESERVED',
      orderId,
      holdExpiresAt,
      message:      `Stock reserved. Complete payment within ${env.HOLD_TTL_SECONDS / 60} minutes.`,
    });

  } catch (err) {
    console.error('[Checkout] Error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = { checkout };

