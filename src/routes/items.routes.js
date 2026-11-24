const express  = require('express');
const router   = express.Router();

const { listItems, createItem } = require('../controllers/items.controller');

/**
 * GET  /api/items      — list all items
 * POST /api/items      — create + seed item (admin, no auth for demo)
 */
router.get('/',  listItems);
router.post('/', createItem);

module.exports = router;

