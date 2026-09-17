const { spawnSync } = require("node:child_process");
const result = spawnSync(
  process.execPath,
  [
    "--test",
    "--test-concurrency=1",
    "test/integration.js",
    "test/recovery.js",
    "seat-service/mongo.integration.js",
    "ticket-service/effects.test.js",
  ],
  {
    stdio: "inherit",
    env: { ...process.env, CINEMA_EFFECTS_INTEGRATION: "1" },
  },
);
process.exit(result.status ?? 1);
