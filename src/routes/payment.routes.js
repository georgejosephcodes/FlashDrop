const express = require('express');
const router  = express.Router({ mergeParams: true });

const { authMiddleware } = require('../middleware/auth');
const { pay }            = require('../controllers/payment.controller');

/**
 * POST /api/orders/:id/pay
 * Requires Clerk auth — userId must match the order holder.
 */
router.post('/:id/pay', ...authMiddleware, pay);

module.exports = router;

