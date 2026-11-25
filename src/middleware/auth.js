const { clerkMiddleware, requireAuth } = require('@clerk/express');

/**
 * authMiddleware
 *
 * Production: Two-layer Clerk integration:
 *   1. clerkMiddleware()  — attaches auth state to every request (req.auth)
 *   2. requireAuth()      — returns 401 if no valid session
 *
 * Development (SKIP_AUTH=true in .env):
 *   Bypasses Clerk entirely and injects a fake userId so you can test
 *   the full checkout → pay → confirm flow with curl/Scalar without
 *   needing a real Clerk account or JWT token.
 *
 * ⚠️  NEVER set SKIP_AUTH=true in production.
 *
 * Usage: router.post('/checkout', ...authMiddleware, controller)
 */

const SKIP_AUTH = process.env.SKIP_AUTH === 'true';

function devAuthBypass(req, _res, next) {
  // Inject a fake auth object so controllers can read req.auth.userId normally
  req.auth = { userId: req.headers['x-dev-user-id'] || 'dev_user_001' };
  next();
}

const authMiddleware = SKIP_AUTH
  ? [devAuthBypass]
  : [clerkMiddleware(), requireAuth()];

if (SKIP_AUTH) {
  console.warn('[Auth] ⚠️  SKIP_AUTH=true — Clerk bypassed. Dev mode only.');
}

module.exports = { authMiddleware };

