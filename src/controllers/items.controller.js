const { z }             = require('zod');
const { query }         = require('../config/db');
const { seedItemStock } = require('../services/redisService');

const createItemSchema = z.object({
  title:         z.string().min(1).max(120),
  description:   z.string().optional(),
  priceCents:    z.number().int().positive(),
  totalStock:    z.number().int().positive(),
  saleStartAt:   z.string().datetime(),
});

/**
 * GET /api/items
 * Returns all items with live Redis stock in the response.
 */
async function listItems(req, res) {
  try {
    const { rows } = await query('SELECT * FROM items ORDER BY created_at DESC');
    return res.json(rows);
  } catch (err) {
    console.error('[Items] listItems error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

/**
 * POST /api/items  (admin seed endpoint — no auth for demo convenience)
 *
 * Inserts the item into Postgres AND seeds item:{id}:stock in Redis.
 * Both must succeed for the item to be active.
 */
async function createItem(req, res) {
  try {
    const parsed = createItemSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: parsed.error.flatten().fieldErrors });
    }

    const { title, description, priceCents, totalStock, saleStartAt } = parsed.data;

    // Insert into Postgres
    const { rows } = await query(
      `INSERT INTO items (title, description, price_cents, total_stock, available_stock, sale_start_at)
       VALUES ($1, $2, $3, $4, $4, $5)
       RETURNING *`,
      [title, description ?? null, priceCents, totalStock, saleStartAt]
    );

    const item = rows[0];

    // Seed Redis stock counter — must match Postgres available_stock
    await seedItemStock(item.id, totalStock);

    return res.status(201).json(item);

  } catch (err) {
    console.error('[Items] createItem error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = { listItems, createItem };

