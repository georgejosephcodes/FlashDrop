const { z }              = require('zod');
const { query }          = require('../config/db');
const { getRedisClient } = require('../config/redis');
const { seedItemStock }  = require('../services/redisService');

const createItemSchema = z.object({
  title:         z.string().min(1).max(120),
  description:   z.string().optional(),
  priceCents:    z.number().int().positive(),
  totalStock:    z.number().int().positive(),
  saleStartAt:   z.string().datetime(),
});

/**
 * GET /api/items
 * Returns all items enriched with live Redis stock when available.
 */
async function listItems(_req, res) {
  try {
    const { rows } = await query('SELECT * FROM items ORDER BY created_at DESC');
    if (rows.length === 0) return res.json(rows);

    const redis  = getRedisClient();
    const keys   = rows.map(r => `item:${r.id}:stock`);
    const stocks = await redis.mget(...keys);

    const enriched = rows.map((row, idx) => ({
      ...row,
      available_stock: stocks[idx] !== null ? parseInt(stocks[idx], 10) : row.available_stock,
    }));

    return res.json(enriched);
  } catch (err) {
    console.error('[Items] listItems error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

/**
 * POST /api/items  (admin seed endpoint — no auth for demo convenience)
 *
 * Inserts the item into Postgres AND seeds item:{id}:stock + item:{id}:meta in Redis.
 * Rolls back the PG row if Redis seeding fails.
 */
async function createItem(req, res) {
  let createdItemId = null;

  try {
    const parsed = createItemSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: parsed.error.flatten().fieldErrors });
    }

    const { title, description, priceCents, totalStock, saleStartAt } = parsed.data;

    const { rows } = await query(
      `INSERT INTO items (title, description, price_cents, total_stock, available_stock, sale_start_at)
       VALUES ($1, $2, $3, $4, $4, $5)
       RETURNING *`,
      [title, description ?? null, priceCents, totalStock, saleStartAt]
    );

    const item    = rows[0];
    createdItemId = item.id;

    await seedItemStock(item.id, totalStock, { priceCents, saleStartAt });

    return res.status(201).json(item);

  } catch (err) {
    if (createdItemId) {
      await query('DELETE FROM items WHERE id = $1', [createdItemId]).catch(() => {});
    }
    console.error('[Items] createItem error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = { listItems, createItem };
