const { spawn } = require("node:child_process");
const path = require("node:path");
const names = [
  "seat-service",
  "payment-service",
  "ticket-service",
  "notification-service",
  "analytics-service",
  "ticket-purchase-api",
];
const children = names.map((name) =>
  spawn(process.execPath, [path.join(__dirname, "..", name, "index.js")], {
    stdio: "inherit",
    env: process.env,
  }),
);
let stopping = false;
function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  children.forEach((c) => c.kill("SIGTERM"));
  Promise.all(
    children.map((c) =>
      c.exitCode !== null
        ? Promise.resolve()
        : new Promise((r) => c.once("exit", r)),
    ),
  ).then(() => process.exit(code));
}
children.forEach((c) =>
  c.on("exit", (code) => {
    if (!stopping) stop(code || 1);
  }),
);
process.on("SIGINT", () => stop());
process.on("SIGTERM", () => stop());
