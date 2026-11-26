const crypto        = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { z }          = require('zod');
const { getUserId }           = require('../middleware/auth');
const { COMPLETED_TTL_SEC }   = require('../middleware/idempotency');
const {
  reserveStock,
  getItemMeta,
}                             = require('../services/redisService');
const env            = require('../config/env');

const checkoutSchema = z.object({
  itemId:      z.string().uuid('itemId must be a valid UUID'),
  amountCents: z.number().int().positive('amountCents must be a positive integer'),
});

/**
 * POST /api/orders/checkout
 *
 * Flow:
 *  1. Validate body (Zod) & verify authenticated userId
 *  2. Check cached item metadata (saleStartAt, authoritative priceCents)
 *  3. Run reserve_stock.lua (atomic DECR + XADD OrderReserved in one Redis call)
 *  4. SOLD_OUT → 409 | SUCCESS → 202
 */
async function checkout(req, res) {
  try {
    const parsed = checkoutSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: parsed.error.flatten().fieldErrors });
    }

    const itemId = parsed.data.itemId;
    const userId = getUserId(req);
    if (!userId) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const rawIdempotencyKey = req.headers['x-idempotency-key'];
    const scopedKey         = `${userId}:${rawIdempotencyKey}`;
    const idempotencyKey    = scopedKey.length <= 128
      ? scopedKey
      : crypto.createHash('sha256').update(scopedKey).digest('hex');
    const reservationKey    = `reservation:idempotency:${scopedKey}`;
    const orderId           = uuidv4();

    // Check item sale window & authoritative price if cached in Redis
    const meta = await getItemMeta(itemId);
    if (meta && meta.saleStartAt && Date.now() < Date.parse(meta.saleStartAt)) {
      return res.status(403).json({
        status:  'NOT_STARTED',
        message: 'Sale has not started yet.',
      });
    }
    const effectiveAmountCents = meta ? meta.priceCents : parsed.data.amountCents;

    // Atomic lookup/DECR/XADD/SET mapping in a single Lua transaction
    const result = await reserveStock({
      orderId,
      userId,
      itemId,
      amountCents: effectiveAmountCents,
      idempotencyKey,
      reservationKey,
      mappingTtlSeconds: COMPLETED_TTL_SEC,
    });

    if (result.status === 'SOLD_OUT') {
      return res.status(409).json({
        status:  'SOLD_OUT',
        message: 'Item inventory has been completely claimed.',
      });
    }

    const finalOrderId  = result.orderId || orderId;
    const holdExpiresAt = new Date(Date.now() + env.HOLD_TTL_SECONDS * 1000);

    return res.status(202).json({
      status:       'RESERVED',
      orderId:      finalOrderId,
      holdExpiresAt,
      message:      `Stock reserved. Complete payment within ${env.HOLD_TTL_SECONDS / 60} minutes.`,
    });

  } catch (err) {
    console.error('[Checkout] Error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = { checkout };
