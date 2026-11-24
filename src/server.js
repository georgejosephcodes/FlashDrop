// src/server.js — placeholder, will be fully built in Phase 4
// Keeps Docker builds valid from Phase 1 onwards

const express = require('express');
const http    = require('http');
const env     = require('./config/env');

const app    = express();
const server = http.createServer(app);

app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', instance: env.INSTANCE_ID });
});

server.listen(env.PORT, () => {
  console.log(`[FlashDrop] Instance ${env.INSTANCE_ID} running on port ${env.PORT}`);
});

module.exports = { app, server };

