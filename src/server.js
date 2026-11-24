// src/server.js — grows each phase; fully completed in Phase 4

const express        = require('express');
const http           = require('http');
const env            = require('./config/env');
const { initStream } = require('./services/streamService');
const { loadLuaScript } = require('./services/redisService');

// Routes (Phase 2)
const checkoutRoutes = require('./routes/checkout.routes');
const itemsRoutes    = require('./routes/items.routes');

const app    = express();
const server = http.createServer(app);

app.use(express.json());

// ── Health check ────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', instance: env.INSTANCE_ID });
});

// ── API Routes ──────────────────────────────────────────────────────────────
app.use('/api/orders/checkout', checkoutRoutes);
app.use('/api/items',           itemsRoutes);

// ── Startup: load Lua script + create stream consumer group ────────────────
async function start() {
  await loadLuaScript();
  await initStream();

  server.listen(env.PORT, () => {
    console.log(`[FlashDrop] Instance ${env.INSTANCE_ID} running on port ${env.PORT}`);
  });
}

start().catch((err) => {
  console.error('[FlashDrop] Startup failed:', err);
  process.exit(1);
});

module.exports = { app, server };

