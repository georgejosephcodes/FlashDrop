-- ─────────────────────────────────────────────────────────────────────────
-- FlashDrop — PostgreSQL Schema
-- Run automatically on first `docker compose up postgres`
-- ─────────────────────────────────────────────────────────────────────────

-- Enable uuid generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Items for sale ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS items (
  id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  title            VARCHAR(120) NOT NULL,
  description      TEXT,
  price_cents      INTEGER      NOT NULL CHECK (price_cents > 0),
  total_stock      INTEGER      NOT NULL CHECK (total_stock >= 0),
  available_stock  INTEGER      NOT NULL CHECK (available_stock >= 0),
  sale_start_at    TIMESTAMPTZ  NOT NULL,
  created_at       TIMESTAMPTZ  DEFAULT NOW()
);

-- ── Orders ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
  id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id          VARCHAR(64)  NOT NULL,
  item_id          UUID         REFERENCES items(id),
  status           VARCHAR(20)  NOT NULL
                   CHECK (status IN ('RESERVED','CONFIRMED','CANCELLED','EXPIRED')),
  amount_cents     INTEGER      NOT NULL,
  hold_expires_at  TIMESTAMPTZ,
  idempotency_key  VARCHAR(128) UNIQUE NOT NULL,
  created_at       TIMESTAMPTZ  DEFAULT NOW(),
  updated_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_user   ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- ── Immutable Event Audit Log ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_events (
  id          BIGSERIAL    PRIMARY KEY,
  order_id    UUID         REFERENCES orders(id),
  event_type  VARCHAR(50)  NOT NULL
              CHECK (event_type IN (
                'ORDER_REQUESTED','STOCK_RESERVED',
                'PAYMENT_SUCCESS','HOLD_EXPIRED','ORDER_CANCELLED'
              )),
  payload     JSONB        NOT NULL,
  created_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_order ON order_events(order_id);

