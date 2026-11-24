const { Pool } = require('pg');
const env       = require('./env');

const pool = new Pool({
  connectionString: env.DATABASE_URL,
  max:              10,   // hard cap — Write-Behind keeps this low intentionally
  idleTimeoutMillis:   30000,
  connectionTimeoutMillis: 5000,
});

pool.on('connect', () => console.log('[DB] New client connected'));
pool.on('error',  (err) => console.error('[DB] Unexpected error:', err.message));

/**
 * Run a parameterised query. Usage:
 *   const { rows } = await query('SELECT * FROM items WHERE id = $1', [id]);
 */
async function query(text, params) {
  const start = Date.now();
  const res   = await pool.query(text, params);
  const ms    = Date.now() - start;
  if (env.NODE_ENV === 'development') {
    console.log(`[DB] query (${ms}ms) rows=${res.rowCount}`);
  }
  return res;
}

module.exports = { query, pool };

