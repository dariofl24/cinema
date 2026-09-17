const express = require("express");
const { randomUUID } = require("node:crypto");
const { MongoClient } = require("mongodb");
const config = require("../shared/config");
const { log } = require("../shared/runtime");

function validateAuthorization(input) {
  if (
    !input ||
    typeof input.purchaseId !== "string" ||
    !input.purchaseId.trim()
  )
    return "purchaseId requerido";
  if (!Number.isSafeInteger(input.amount) || input.amount <= 0)
    return "Importe inválido";
  if (input.currency !== "MXN") return "Moneda inválida";
  if (!["approved", "rejected", "delayed"].includes(input.paymentMode))
    return "Modo de pago inválido";
  return null;
}

async function authorize(collection, input) {
  const error = validateAuthorization(input);
  if (error) throw Object.assign(new Error(error), { status: 400 });
  const authorization = {
    _id: input.purchaseId,
    purchaseId: input.purchaseId,
    authorizationId: randomUUID(),
    amount: input.amount,
    currency: input.currency,
    paymentMode: input.paymentMode,
    status: input.paymentMode === "rejected" ? "rejected" : "approved",
    authorizationCount: 1,
    createdAt: new Date().toISOString(),
  };
  try {
    await collection.updateOne(
      { _id: input.purchaseId },
      { $setOnInsert: authorization },
      { upsert: true },
    );
  } catch (error) {
    if (error.code !== 11000) throw error;
  }
  const persisted = await collection.findOne({ _id: input.purchaseId });
  if (
    ["amount", "currency", "paymentMode"].some(
      (key) => persisted[key] !== input[key],
    )
  ) {
    throw Object.assign(
      new Error("La compra ya tiene una autorización con datos distintos"),
      { status: 409 },
    );
  }
  return persisted;
}

function createApp(collection) {
  const app = express();
  app.use(express.json({ limit: "16kb" }));
  app.get("/health", (_req, res) =>
    res.json({ service: "payment-service", status: "ok" }),
  );
  app.post("/payment-authorizations", async (req, res) => {
    const result = await authorize(collection, req.body);
    log("payment-service", {
      purchaseId: result.purchaseId,
      event: "PaymentAuthorization",
      status: result.status,
    });
    // The durable result exists before a delayed response: reconciliation never charges again.
    if (result.paymentMode === "delayed")
      await new Promise((resolve) => setTimeout(resolve, 3000));
    res.status(result.status === "rejected" ? 422 : 200).json(result);
  });
  app.get("/payment-authorizations/:purchaseId", async (req, res) => {
    const result = await collection.findOne({ _id: req.params.purchaseId });
    if (!result)
      return res.status(404).json({ error: "Autorización no encontrada" });
    res.json(result);
  });
  app.use((error, _req, res, _next) => {
    log("payment-service", { error: error.message });
    res
      .status(error.status || 500)
      .json({ error: error.status ? error.message : "Error interno de pago" });
  });
  return app;
}

async function main() {
  const client = new MongoClient(config.mongo);
  await client.connect();
  const app = createApp(
    client.db(`${config.dbPrefix}_payments`).collection("authorizations"),
  );
  const port = Number(process.env.PAYMENT_PORT || 3002);
  const server = app.listen(port, () =>
    log("payment-service", { listening: port }),
  );
  let closing = false;
  const close = async () => {
    if (closing) return;
    closing = true;
    await new Promise((resolve) => server.close(resolve));
    await client.close();
  };
  for (const signal of ["SIGINT", "SIGTERM"])
    process.once(signal, () => close().then(() => process.exit(0)));
}
if (require.main === module)
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
module.exports = { validateAuthorization, authorize, createApp };
