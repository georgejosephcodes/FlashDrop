/**
 * socketService.js
 *
 * Owns the Socket.io server instance and the Redis Pub/Sub subscriber
 * that bridges incoming order:events channel messages to WebSocket rooms.
 */

const { Server }         = require('socket.io');
const { createAdapter }  = require('@socket.io/redis-adapter');
const { verifyToken }    = require('@clerk/express');
const { getRedisClient } = require('../config/redis');

let io = null;

function initSocket(httpServer) {
  const pubClient = getRedisClient();
  const subClient = pubClient.duplicate();
  const skipAuth  = process.env.SKIP_AUTH === 'true';

  io = new Server(httpServer, {
    cors:       { origin: '*' },
    transports: ['websocket', 'polling'],
  });

  io.adapter(createAdapter(pubClient, subClient));

  // Verify Clerk token during handshake when SKIP_AUTH is disabled
  io.use(async (socket, next) => {
    if (skipAuth) {
      const devId = socket.handshake.auth?.userId || socket.handshake.headers['x-dev-user-id'];
      socket.userId = typeof devId === 'string' && devId ? devId : null;
      return next();
    }

    try {
      const rawAuth = socket.handshake.headers?.authorization || '';
      const token   = socket.handshake.auth?.token || rawAuth.replace(/^Bearer\s+/i, '');
      if (!token) {
        return next(new Error('Unauthorized'));
      }

      const verified = await verifyToken(token, { secretKey: process.env.CLERK_SECRET_KEY });
      if (!verified?.sub) {
        return next(new Error('Unauthorized'));
      }
      socket.userId = verified.sub;
      return next();
    } catch (err) {
      return next(new Error('Unauthorized'));
    }
  });

  io.on('connection', (socket) => {
    console.log(`[Socket] Client connected: ${socket.id}`);

    socket.on('subscribe', (data) => {
      const requestedId = data && typeof data.userId === 'string' ? data.userId : null;
      const targetId    = skipAuth ? (socket.userId || requestedId) : socket.userId;

      if (!targetId) {
        socket.emit('error', { message: 'Missing or invalid userId' });
        return;
      }

      const room = `room:user_${targetId}`;
      socket.join(room);
      console.log(`[Socket] ${socket.id} joined ${room}`);
      socket.emit('subscribed', { room });
    });

    socket.on('disconnect', () => {
      console.log(`[Socket] Client disconnected: ${socket.id}`);
    });
  });

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
      io.to(room).emit(type, rest);
      console.log(`[Socket] Emitted "${type}" to ${room}`);
    } catch (err) {
      console.error('[Socket] Failed to parse Pub/Sub message:', err.message);
    }
  });

  console.log('[Socket] Socket.io initialised with Redis Adapter');
  return io;
}

function emitToUser(userId, event, data) {
  if (!io) {
    console.warn('[Socket] emitToUser called before initSocket()');
    return;
  }
  io.to(`room:user_${userId}`).emit(event, data);
}

module.exports = { initSocket, emitToUser };
