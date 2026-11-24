/**
 * syncService.js — Write-Behind Buffer
 *
 * Collects order records in memory and flushes them to PostgreSQL in bulk.
 * Flushing is triggered by EITHER:
 *   - Buffer reaching SYNC_BATCH_SIZE records (default 50)
 *   - SYNC_INTERVAL_MS milliseconds elapsing since last flush (default 500ms)
 *
 * WHY this matters:
 *   10,000 individual INSERTs → PostgreSQL collapse.
 *   20 bulk INSERTs of 500 records → DB stays stable, IOPS drop 95%+.
 *
 * ON CONFLICT DO NOTHING makes every flush idempotent — safe to retry
 * if the worker crashes mid-flush.
 */

const { query } = require('../config/db');
const env       = require('../config/env');

const BATCH_SIZE    = env.SYNC_BATCH_SIZE;    // default 50
const INTERVAL_MS   = env.SYNC_INTERVAL_MS;   // default 500

let buffer      = [];      // pending order records
let lastFlushAt = Date.now();
let flushTimer  = null;

/**
 * Add an order record to the buffer.
 * Triggers an immediate flush if the batch size threshold is reached.
 *
 * @param {{
 *   orderId:        string,
 *   userId:         string,
 *   itemId:         string,
 *   status:         'RESERVED'|'CONFIRMED'|'EXPIRED'|'CANCELLED',
 *   amountCents:    number,
 *   idempotencyKey: string,
 *   holdExpiresAt?: string,
 * }} record
 */
function enqueue(record) {
  buffer.push(record);

  if (buffer.length >= BATCH_SIZE) {
    flush().catch(err => console.error('[SyncService] Flush error (batch):', err.message));
  }
}

/**
 * Flush all buffered records to PostgreSQL in a single bulk INSERT.
 * Safe to call even when the buffer is empty.
 */
async function flush() {
  if (buffer.length === 0) return;

  const toInsert  = buffer.splice(0, buffer.length);  // drain buffer atomically
  lastFlushAt     = Date.now();

  // Build parameterised bulk INSERT
  // Each row needs 7 values → $1..$7, $8..$14, ...
  const values  = [];
  const placeholders = toInsert.map((r, i) => {
    const base = i * 7;
    values.push(
      r.orderId,
      r.userId,
      r.itemId,
      r.status,
      r.amountCents,
      r.idempotencyKey,
      r.holdExpiresAt ?? null
    );
    return `($${base+1}, $${base+2}, $${base+3}, $${base+4}, $${base+5}, $${base+6}, $${base+7})`;
  });

  const sql = `
    INSERT INTO orders (id, user_id, item_id, status, amount_cents, idempotency_key, hold_expires_at)
    VALUES ${placeholders.join(', ')}
    ON CONFLICT (idempotency_key) DO UPDATE
      SET status     = EXCLUDED.status,
          updated_at = NOW()
  `;

  try {
    const result = await query(sql, values);
    console.log(`[SyncService] Flushed ${toInsert.length} record(s) → DB rows affected: ${result.rowCount}`);
  } catch (err) {
    // On DB error: put records back at the front of the buffer for next flush
    buffer.unshift(...toInsert);
    throw err;
  }
}

/**
 * Start the interval-based auto-flush timer.
 * Call once at worker startup.
 */
function startAutoFlush() {
  if (flushTimer) return;

  flushTimer = setInterval(() => {
    const timeSinceLast = Date.now() - lastFlushAt;
    if (buffer.length > 0 && timeSinceLast >= INTERVAL_MS) {
      flush().catch(err => console.error('[SyncService] Flush error (timer):', err.message));
    }
  }, INTERVAL_MS);

  console.log(`[SyncService] Auto-flush started — batch=${BATCH_SIZE} interval=${INTERVAL_MS}ms`);
}

/**
 * Stop the auto-flush timer and flush any remaining records.
 * Call on graceful shutdown.
 */
async function shutdown() {
  if (flushTimer) {
    clearInterval(flushTimer);
    flushTimer = null;
  }
  await flush();
  console.log('[SyncService] Shutdown complete — buffer drained');
}

module.exports = { enqueue, flush, startAutoFlush, shutdown };

