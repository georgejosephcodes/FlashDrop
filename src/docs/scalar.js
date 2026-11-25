/**
 * scalar.js — Scalar API Reference middleware
 *
 * Mounted in server.js at /docs.
 * Fetches the OpenAPI spec from /openapi.json and renders an interactive
 * playground where you can authenticate with a Clerk Bearer token and
 * execute requests directly from the browser — no frontend needed.
 *
 * Usage: visit http://localhost:8080/docs
 */

const { apiReference } = require('@scalar/express-api-reference');

const scalarMiddleware = apiReference({
  spec:  { url: '/openapi.json' },
  theme: 'purple',
  layout: 'modern',
  defaultHttpClient: {
    targetKey: 'javascript',
    clientKey: 'fetch',
  },
});

module.exports = { scalarMiddleware };

