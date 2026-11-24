const express  = require('express');
const router   = express.Router();

const { authMiddleware } = require('../middleware/auth');
const { rateLimiter }    = require('../middleware/rateLimiter');
const { idempotency }    = require('../middleware/idempotency');
const { checkout }       = require('../controllers/checkout.controller');

/**
 * POST /api/orders/checkout
 *
 * Middleware chain (in order):
 *   authMiddleware  → Clerk JWT verification, attaches req.auth.userId
 *   rateLimiter     → 20 req/s per IP via Redis INCR+EXPIRE
 *   idempotency     → Redis SET NX dedup on X-Idempotency-Key header
 *   checkout        → Lua atomic reserve + stream publish
 */
router.post('/', ...authMiddleware, rateLimiter, idempotency, checkout);

module.exports = router;

