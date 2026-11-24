/**
 * server.js — FlashDrop entry point
 *
 * Startup sequence:
 *   1. Validate env (Zod — exits on bad config)
 *   2. Load reserve_stock.lua into Redis (SCRIPT LOAD → EVALSHA)
 *   3. Create Redis Stream consumer groups
 *   4. Attach Socket.io + Redis Adapter (cross-replica push)
 *   5. Mount all API routes
 *   6. Mount Scalar interactive docs at /docs
 *   7. Listen
 */

const express           = require('express');
const http              = require('http');
const { apiReference }  = require('@scalar/express-api-reference');

const env                        = require('./config/env');
const { loadLuaScript }          = require('./services/redisService');
const { initStream }             = require('./services/streamService');
const { initSocket }             = require('./services/socketService');

// ── Routes ──────────────────────────────────────────────────────────────────
const checkoutRoutes = require('./routes/checkout.routes');
const paymentRoutes  = require('./routes/payment.routes');
const itemsRoutes    = require('./routes/items.routes');
const metricsRoutes  = require('./routes/metrics.routes');

// ── App & HTTP server ────────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

app.use(express.json());

// ── Health check ─────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', instance: env.INSTANCE_ID, ts: new Date().toISOString() });
});

// ── API routes ────────────────────────────────────────────────────────────────
app.use('/api/orders/checkout', checkoutRoutes);
app.use('/api/orders',          paymentRoutes);
app.use('/api/items',           itemsRoutes);
app.use('/api/metrics',         metricsRoutes);

// ── Scalar interactive API docs at /docs ─────────────────────────────────────
app.use('/docs', apiReference({
  spec: { url: '/openapi.json' },
  theme: 'purple',
}));

// Serve the raw spec so Scalar can fetch it
app.get('/openapi.json', (_req, res) => {
  res.sendFile(require('path').join(__dirname, 'docs/openapi.json'));
});

// ── Startup ───────────────────────────────────────────────────────────────────
async function start() {
  // 1. Load Lua script into Redis
  await loadLuaScript();

  // 2. Create Stream consumer groups (idempotent — safe to call on every boot)
  await initStream();

  // 3. Attach Socket.io + Redis Pub/Sub bridge
  initSocket(server);

  // 4. Start listening
  server.listen(env.PORT, () => {
    console.log(`[FlashDrop] ✅  Instance "${env.INSTANCE_ID}" listening on port ${env.PORT}`);
    console.log(`[FlashDrop] 📖  API docs → http://localhost:${env.PORT}/docs`);
  });
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
process.on('SIGTERM', () => {
  console.log('[FlashDrop] SIGTERM — closing HTTP server...');
  server.close(() => {
    console.log('[FlashDrop] HTTP server closed');
    process.exit(0);
  });
});

start().catch((err) => {
  console.error('[FlashDrop] Startup failed:', err);
  process.exit(1);
});

module.exports = { app, server };

