const { clerkMiddleware, requireAuth } = require('@clerk/express');

/**
 * authMiddleware
 *
 * Two-layer Clerk integration:
 *   1. clerkMiddleware()  — attaches auth state to every request (req.auth)
 *   2. requireAuth()      — enforces that req.auth.userId is present;
 *                           returns 401 Unauthorized if not
 *
 * Usage: router.post('/checkout', ...authMiddleware, controller)
 */
const authMiddleware = [clerkMiddleware(), requireAuth()];

module.exports = { authMiddleware };

