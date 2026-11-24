/**
 * dbSyncWorker.js — Write-Behind Database Sync Worker
 *
 * Reads ALL order lifecycle events from orders:stream using a SEPARATE
 * consumer group ("dbSyncers") so it processes every event independently
 * of the orderProcessorWorker.
 *
 * For each event it enqueues a record into syncService which batches them
 * into bulk PostgreSQL INSERTs, keeping DB connection count stable under load.
 *
 * Event → DB status mapping:
 *   OrderReserved  → INSERT status=RESERVED  (with hold_expires_at)
 *   OrderConfirmed → UPSERT status=CONFIRMED
 *   OrderExpired   → UPSERT status=EXPIRED
 */

require('dotenv').config();
const { getRedisClient }              = require('../config/redis');
const { enqueue, startAutoFlush, shutdown } = require('../services/syncService');
const env                             = require('../config/env');

const STREAM_KEY  = 'orders:stream';
const GROUP_NAME  = 'dbSyncers';
const CONSUMER    = `dbsync-${process.pid}`;
const BLOCK_MS    = 2000;
const BATCH_COUNT = 50;   // larger batch — DB sync can lag slightly, that's fine

/** Parse flat Redis stream field array → plain object */
function parseFields(fields) {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[fields[i]] = fields[i + 1];
  }
  return obj;
}

/** Map a stream event to an order record for syncService */
function toRecord(fields) {
  const { event, orderId, userId, itemId, amountCents, idempotencyKey, timestamp } = fields;

  const holdExpiresAt = event === 'OrderReserved'
    ? new Date(Number(timestamp) + env.HOLD_TTL_SECONDS * 1000).toISOString()
    : null;

  const statusMap = {
    OrderReserved:  'RESERVED',
    OrderConfirmed: 'CONFIRMED',
    OrderExpired:   'EXPIRED',
  };

  const status = statusMap[event];
  if (!status) return null;   // unknown event — skip

  return {
    orderId,
    userId,
    itemId,
    status,
    amountCents:    Number(amountCents) || 0,
    idempotencyKey: idempotencyKey || orderId,  // fallback for Confirmed/Expired events
    holdExpiresAt,
  };
}

async function initGroup(redis) {
  try {
    // Read from beginning (0) so we don't miss events that arrived before this worker started
    await redis.xgroup('CREATE', STREAM_KEY, GROUP_NAME, '0', 'MKSTREAM');
    console.log(`[DBSync] Consumer group "${GROUP_NAME}" created`);
  } catch (err) {
    if (err.message.includes('BUSYGROUP')) {
      console.log(`[DBSync] Consumer group "${GROUP_NAME}" already exists — OK`);
    } else {
      throw err;
    }
  }
}

async function run() {
  const redis = getRedisClient();

  await initGroup(redis);
  startAutoFlush();   // start the 500ms interval flush timer

  console.log(`[DBSync] Starting consumer "${CONSUMER}" on stream "${STREAM_KEY}"`);

  // Graceful shutdown — flush remaining buffer before exit
  process.on('SIGTERM', async () => {
    console.log('[DBSync] SIGTERM received — draining buffer...');
    await shutdown();
    process.exit(0);
  });

  while (true) {
    try {
      const results = await redis.xreadgroup(
        'GROUP', GROUP_NAME, CONSUMER,
        'COUNT', BATCH_COUNT,
        'BLOCK', BLOCK_MS,
        'STREAMS', STREAM_KEY, '>'
      );

      if (!results) continue;

      for (const [, messages] of results) {
        for (const [streamId, rawFields] of messages) {
          const fields = parseFields(rawFields);
          const record = toRecord(fields);

          if (record) {
            enqueue(record);
          }

          // ACK regardless — unknown events shouldn't block the PEL
          await redis.xack(STREAM_KEY, GROUP_NAME, streamId);
        }
      }

    } catch (err) {
      console.error('[DBSync] Error:', err.message);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

run().catch((err) => {
  console.error('[DBSync] Fatal error:', err);
  process.exit(1);
});

