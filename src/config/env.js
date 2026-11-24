require('dotenv').config();
const { z } = require('zod');

const envSchema = z.object({
  PORT:               z.string().default('3000'),
  NODE_ENV:           z.enum(['development', 'production', 'test']).default('development'),
  INSTANCE_ID:        z.string().default('local'),

  DATABASE_URL:       z.string().url(),
  REDIS_URL:          z.string().url(),

  CLERK_PUBLISHABLE_KEY: z.string().min(1),
  CLERK_SECRET_KEY:      z.string().min(1),

  HOLD_TTL_SECONDS:   z.string().default('600').transform(Number),
  SYNC_BATCH_SIZE:    z.string().default('50').transform(Number),
  SYNC_INTERVAL_MS:   z.string().default('500').transform(Number),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('❌  Missing or invalid environment variables:');
  console.error(parsed.error.flatten().fieldErrors);
  process.exit(1);
}

module.exports = parsed.data;

