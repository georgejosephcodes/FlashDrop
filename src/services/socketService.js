/**
 * socketService.js
 *
 * Owns the Socket.io server instance and the Redis Pub/Sub subscriber
 * that bridges incoming order:events channel messages to WebSocket rooms.
 *
 * Architecture:
 *   Redis Pub/Sub (order:events) ──► subscriber here ──► socket.to(room).emit()
 *
 * The @socket.io/redis-adapter ensures that even when app1 receives the
 * Pub/Sub message, it can push to a client connected to app2 — shared
 * state across all API replicas with zero sticky-session requirement for
 * the emit path (Nginx ip_hash only needed for the initial WS handshake).
 */

const { Server }       = require('socket.io');
const { createAdapter } = require('@socket.io/redis-adapter');
const { getRedisClient } = require('../config/redis');

let io = null;   // singleton Socket.io server

/**
 * Initialise Socket.io on the given HTTP server.
 * Must be called once during app startup (server.js).
 *
 * @param {import('http').Server} httpServer
 */
function initSocket(httpServer) {
  // Two separate Redis connections required by the adapter (pub + sub)
  const pubClient = getRedisClient();
  const subClient = pubClient.duplicate();

  io = new Server(httpServer, {
    cors: { origin: '*' },          // tighten in production
    transports: ['websocket', 'polling'],
  });

  io.adapter(createAdapter(pubClient, subClient));

  // ── Client connection handler ────────────────────────────────────────────
  io.on('connection', (socket) => {
    console.log(`[Socket] Client connected: ${socket.id}`);

    /**
     * Client emits: socket.emit('subscribe', { userId: 'usr_xxx' })
     * We join them to room:user_{userId} so we can push targeted events.
     */
    socket.on('subscribe', ({ userId }) => {
      if (!userId) return;
      const room = `room:user_${userId}`;
      socket.join(room);
      console.log(`[Socket] ${socket.id} joined ${room}`);
      socket.emit('subscribed', { room });
    });

    socket.on('disconnect', () => {
      console.log(`[Socket] Client disconnected: ${socket.id}`);
    });
  });

  // ── Redis Pub/Sub → WebSocket bridge ────────────────────────────────────
  // Subscribe on the duplicate client to avoid blocking the pub client
  subClient.subscribe('order:events', (err) => {
    if (err) console.error('[Socket] Pub/Sub subscribe error:', err.message);
    else     console.log('[Socket] Subscribed to order:events channel');
  });

  subClient.on('message', (_channel, message) => {
    try {
      const payload = JSON.parse(message);
      const { type, userId, ...rest } = payload;

      if (!userId || !type) return;

      const room = `room:user_${userId}`;

      // Push to the specific user's room across ALL replicas via Redis Adapter
      io.to(room).emit(type, rest);

      console.log(`[Socket] Emitted "${type}" to ${room}`);
    } catch (err) {
      console.error('[Socket] Failed to parse Pub/Sub message:', err.message);
    }
  });

  console.log('[Socket] Socket.io initialised with Redis Adapter');
  return io;
}

/**
 * Emit an event directly to a user's room.
 * Can be called from any part of the app after initSocket().
 *
 * @param {string} userId
 * @param {string} event   e.g. 'order:reserved'
 * @param {object} data
 */
function emitToUser(userId, event, data) {
  if (!io) {
    console.warn('[Socket] emitToUser called before initSocket()');
    return;
  }
  io.to(`room:user_${userId}`).emit(event, data);
}

module.exports = { initSocket, emitToUser };

