/**
 * dbSyncWorker.js — Write-Behind Database Sync Worker
 *
 * Reads ALL order lifecycle events from orders:stream using consumer group
 * "dbSyncers", batches them into syncService, and ACKs stream IDs ONLY AFTER
 * PostgreSQL confirms the bulk UPSERT.
 *
 * Uses Redis XAUTOCLAIM on startup (and periodically) so pending entries left
 * by crashed or old consumers (different PIDs) are recovered and flushed.
 */

require('dotenv').config();
const { getRedisClient } = require('../config/redis');
const {
  enqueue,
  flush,
  startAutoFlush,
  shutdown,
  setOnFlushed,
} = require('../services/syncService');
const env = require('../config/env');

const STREAM_KEY    = 'orders:stream';
const GROUP_NAME    = 'dbSyncers';
const CONSUMER      = `dbsync-${process.pid}`;
const BLOCK_MS      = 2000;
const BATCH_COUNT   = 50;
const CLAIM_IDLE_MS = parseInt(process.env.DB_SYNC_CLAIM_IDLE_MS || '5000', 10);

function parseFields(fields) {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[fields[i]] = fields[i + 1];
  }
  return obj;
}

function toRecord(fields, streamId) {
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
  if (!status) return null;

  return {
    streamId,
    orderId,
    userId,
    itemId,
    status,
    amountCents:    Number(amountCents) || 0,
    idempotencyKey: idempotencyKey || orderId,
    holdExpiresAt,
  };
}

async function initGroup(redis) {
  try {
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

/**
 * Reclaim stale pending entries from ANY consumer (including dead/previous PIDs)
 * using XAUTOCLAIM, enqueue them, and flush to PostgreSQL before ACKing.
 */
async function reclaimStalePending(
  ackRedis,
  minIdleMs = CLAIM_IDLE_MS,
  {
    streamKey = STREAM_KEY,
    groupName = GROUP_NAME,
    consumer  = CONSUMER,
  } = {}
) {
  let cursor = '0-0';

  while (true) {
    const [nextCursor, messages, deletedIds] = await ackRedis.xautoclaim(
      streamKey,
      groupName,
      consumer,
      minIdleMs,
      cursor,
      'COUNT',
      BATCH_COUNT
    );

    if (deletedIds && deletedIds.length > 0) {
      await ackRedis.xack(streamKey, groupName, ...deletedIds);
    }

    if (messages && messages.length > 0) {
      console.log(`[DBSync] Reclaimed ${messages.length} pending entry(ies) via XAUTOCLAIM`);
      for (const [streamId, rawFields] of messages) {
        if (!rawFields) {
          await ackRedis.xack(streamKey, groupName, streamId);
          continue;
        }
        const record = toRecord(parseFields(rawFields), streamId);
        if (record) {
          enqueue(record);
        } else {
          await ackRedis.xack(streamKey, groupName, streamId);
        }
      }
      await flush();
    }

    if (!nextCursor || nextCursor === '0-0') break;
    cursor = nextCursor;
  }
}

async function run() {
  const streamRedis = getRedisClient();
  // Separate non-blocking connection so XACK / XAUTOCLAIM never wait behind XREADGROUP BLOCK
  const ackRedis    = streamRedis.duplicate();

  await initGroup(streamRedis);

  setOnFlushed(async (streamIds) => {
    await ackRedis.xack(STREAM_KEY, GROUP_NAME, ...streamIds);
  });

  startAutoFlush();

  console.log(`[DBSync] Starting consumer "${CONSUMER}" on stream "${STREAM_KEY}"`);

  process.on('SIGTERM', async () => {
    console.log('[DBSync] SIGTERM received — draining buffer...');
    try {
      await shutdown();
    } catch (err) {
      console.error('[DBSync] Shutdown flush failed:', err.message);
    }
    process.exit(0);
  });

  // On boot, recover any stale pending entries left by crashed/old consumers
  await reclaimStalePending(ackRedis);

  let lastReclaimAt = Date.now();

  while (true) {
    try {
      const results = await streamRedis.xreadgroup(
        'GROUP', GROUP_NAME, CONSUMER,
        'COUNT', BATCH_COUNT,
        'BLOCK', BLOCK_MS,
        'STREAMS', STREAM_KEY, '>'
      );

      if (results) {
        for (const [, messages] of results) {
          for (const [streamId, rawFields] of messages) {
            const fields = parseFields(rawFields);
            const record = toRecord(fields, streamId);

            if (record) {
              enqueue(record);
            } else {
              await ackRedis.xack(STREAM_KEY, GROUP_NAME, streamId);
            }
          }
        }
      }

      if (Date.now() - lastReclaimAt >= CLAIM_IDLE_MS) {
        lastReclaimAt = Date.now();
        await reclaimStalePending(ackRedis);
      }
    } catch (err) {
      console.error('[DBSync] Error:', err.message);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

if (require.main === module) {
  run().catch((err) => {
    console.error('[DBSync] Fatal error:', err);
    process.exit(1);
  });
}

module.exports = { reclaimStalePending, toRecord, parseFields };
