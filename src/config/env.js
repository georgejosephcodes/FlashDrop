require('dotenv').config();
const { z } = require('zod');

const envSchema = z.object({
  PORT:               z.string().default('3000'),
  NODE_ENV:           z.enum(['development', 'production', 'test']).default('development'),
  INSTANCE_ID:        z.string().default('local'),

  DATABASE_URL:       z.string().url(),
  REDIS_URL:          z.string().url(),

  // Clerk keys are optional when SKIP_AUTH=true (local dev / testing)
  SKIP_AUTH:             z.string().default('false'),
  CLERK_PUBLISHABLE_KEY: z.string().default(''),
  CLERK_SECRET_KEY:      z.string().default(''),

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

const data = parsed.data;

// Disallow SKIP_AUTH in production
if (data.NODE_ENV === 'production' && data.SKIP_AUTH === 'true') {
  console.error('❌  SKIP_AUTH=true is forbidden when NODE_ENV=production.');
  process.exit(1);
}

// Enforce Clerk keys only when auth is not skipped
if (data.SKIP_AUTH !== 'true' && (!data.CLERK_PUBLISHABLE_KEY || !data.CLERK_SECRET_KEY)) {
  console.error('❌  CLERK_PUBLISHABLE_KEY and CLERK_SECRET_KEY are required when SKIP_AUTH is not true.');
  process.exit(1);
}

module.exports = data;

