// Real HTTP + Kafka + MongoDB contract tests. Requires running services.
const { test, before, after } = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { MongoClient } = require("mongodb");
const { Kafka, logLevel } = require("kafkajs");
const config = require("../shared/config");
const { event, topics } = require("../shared/events");
const createdShows = [];
const base = process.env.API_URL || "http://localhost:3000";
const client = new MongoClient(config.mongo);
const kafka = new Kafka({
  brokers: config.brokers,
  clientId: "cinema-tests",
  logLevel: logLevel.ERROR,
});
const producer = kafka.producer();
const db = (suffix) => client.db(`${config.dbPrefix}_${suffix}`);
async function request(path, body, key) {
  const r = await fetch(base + path, {
    method: body ? "POST" : "GET",
    headers: {
      "Content-Type": "application/json",
      ...(key ? { "Idempotency-Key": key } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  return { status: r.status, body: await r.json() };
}
async function until(fn, predicate, ms = 20000) {
  const start = Date.now();
  let value;
  while (Date.now() - start < ms) {
    value = await fn();
    if (predicate(value)) return value;
    await new Promise((r) => setTimeout(r, 150));
  }
  throw Error("Timed out: " + JSON.stringify(value));
}
async function show() {
  const id = `showtime-test-${randomUUID()}`;
  createdShows.push(id);
  await db("seats")
    .collection("showtimes")
    .insertOne({
      _id: id,
      title: "Prueba aislada",
      time: "20:00",
      price: 120,
      currency: "MXN",
      revision: 0,
      holds: {},
      outbox: [],
      seats: ["F8", "F9", "F10"].map((id) => ({ id, status: "available" })),
    });
  return id;
}
const body = (
  showtimeId,
  seatIds = ["F8", "F9"],
  paymentMode = "approved",
) => ({
  customer: { name: "Ana", email: "ana@example.com" },
  showtimeId,
  seatIds,
  paymentMode,
});
async function buy(b, key = randomUUID()) {
  const r = await request("/ticket-purchases", b, key);
  assert.equal(r.status, 202, JSON.stringify(r));
  return r.body.purchaseId;
}
const purchase = (id) => request(`/ticket-purchases/${id}`).then((r) => r.body);
const terminal = (id, status) =>
  until(
    () => purchase(id),
    (p) => p.status === status,
  );
before(async () => {
  await client.connect();
  await producer.connect();
});
after(async () => {
  await producer.disconnect();
  await db("seats")
    .collection("showtimes")
    .deleteMany({ _id: { $in: createdShows } });
  await client.close();
});
test("success persists tickets, notification and analytics; repeated events and requests have one effect", async () => {
  const b = body(await show()),
    key = randomUUID();
  const id = await buy(b, key);
  assert.equal(await buy(b, key), id);
  assert.equal(
    (await request("/ticket-purchases", { ...b, seatIds: ["F8"] }, key)).status,
    409,
  );
  const p = await terminal(id, "completed");
  assert.equal(p.amount, 240);
  await until(
    () => purchase(id),
    (p) => p.tickets?.length === 2,
  );
  await until(
    () => db("notifications").collection("notifications").findOne({ _id: id }),
    Boolean,
  );
  await until(
    () => db("analytics").collection("purchases").findOne({ _id: id }),
    Boolean,
  );
  const e = event("TicketPurchaseCompleted", { ...p, purchaseId: id });
  await producer.send({
    topic: topics.purchases,
    messages: [
      { key: id, value: JSON.stringify(e) },
      { key: id, value: JSON.stringify(e) },
    ],
  });
  await producer.send({
    topic: topics.requests,
    messages: [
      {
        key: b.showtimeId,
        value: JSON.stringify(
          event("SeatHoldRequested", { ...b, purchaseId: id }),
        ),
      },
    ],
  });
  await new Promise((r) => setTimeout(r, 800));
  for (const [suffix, col] of [
    ["tickets", "tickets"],
    ["payments", "authorizations"],
    ["notifications", "notifications"],
    ["analytics", "purchases"],
  ])
    assert.equal(
      await db(suffix).collection(col).countDocuments({ _id: id }),
      1,
    );
  assert.equal((await purchase(id)).tickets.length, 2);
});
test("overlapping concurrent purchases accept only one complete seat set", async () => {
  const showtimeId = await show();
  const ids = await Promise.all([
    buy(body(showtimeId, ["F8", "F9"])),
    buy(body(showtimeId, ["F9", "F10"])),
  ]);
  const ps = await until(
    () => Promise.all(ids.map(purchase)),
    (ps) => ps.every((p) => ["completed", "rejected"].includes(p.status)),
  );
  assert.deepEqual(ps.map((p) => p.status).sort(), ["completed", "rejected"]);
  const seats = (await request(`/showtimes/${showtimeId}/seats`)).body.seats;
  assert.equal(seats.filter((s) => s.status === "sold").length, 2);
  assert.equal(seats.filter((s) => s.status === "available").length, 1);
});
test("rejected payment releases every seat", async () => {
  const b = body(await show(), ["F8", "F9"], "rejected");
  const id = await buy(b);
  await terminal(id, "rejected");
  const seats = (await request(`/showtimes/${b.showtimeId}/seats`)).body.seats;
  assert.ok(seats.every((s) => s.status === "available"));
});
test("timeout protects seats and reconciliation only queries existing authorization", async () => {
  const b = body(await show(), ["F8", "F9"], "delayed");
  const id = await buy(b);
  await terminal(id, "pending_review");
  const seats = (await request(`/showtimes/${b.showtimeId}/seats`)).body.seats;
  assert.equal(seats.filter((s) => s.status === "paying").length, 2);
  const auth = await db("payments")
    .collection("authorizations")
    .findOne({ _id: id });
  assert.equal(
    (await request(`/ticket-purchases/${id}/reconcile-payment`, {})).status,
    202,
  );
  await terminal(id, "completed");
  assert.deepEqual(
    await db("payments").collection("authorizations").findOne({ _id: id }),
    auth,
  );
});
module.exports = { until };
