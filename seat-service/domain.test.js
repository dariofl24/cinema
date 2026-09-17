const test = require("node:test");
const assert = require("node:assert/strict");
const { seed, handle, expire } = require("./domain");
// In-memory atomic collection exercises contention without requiring infrastructure.
class Collection {
  constructor() {
    this.docs = new Map();
  }
  async updateOne(query, update) {
    if (!this.docs.has(query._id))
      this.docs.set(query._id, structuredClone(update.$setOnInsert));
  }
  async findOne(query) {
    return structuredClone(this.docs.get(query._id) || null);
  }
  async replaceOne(query, value) {
    const old = this.docs.get(query._id);
    if (old?.revision !== query.revision) return { matchedCount: 0 };
    this.docs.set(query._id, structuredClone(value));
    return { matchedCount: 1 };
  }
  find() {
    return { toArray: async () => structuredClone([...this.docs.values()]) };
  }
}
const request = (type, id, seats = ["F8"]) => ({
  eventId: `${type}-${id}`,
  eventType: type,
  data: { purchaseId: id, showtimeId: "showtime-1", seatIds: seats },
});
async function setup() {
  const c = new Collection();
  await seed(c);
  return c;
}
test("concurrent overlapping groups accept exactly one with no partial hold", async () => {
  const c = await setup();
  await Promise.all([
    handle(c, request("SeatHoldRequested", "one", ["F8", "F9"])),
    handle(c, request("SeatHoldRequested", "two", ["F9", "F10"])),
  ]);
  const d = await c.findOne({ _id: "showtime-1" });
  assert.equal(
    Object.values(d.holds).filter((h) => h.status === "held").length,
    1,
  );
  const winner = Object.entries(d.holds).find(
    ([, h]) => h.status === "held",
  )[0];
  assert.equal(d.seats.filter((s) => s.purchaseId === winner).length, 2);
  assert.equal(
    d.seats.filter((s) => s.purchaseId && s.purchaseId !== winner).length,
    0,
  );
});
test("duplicates never append more effects and rejection remains final", async () => {
  const c = await setup();
  const e = request("SeatHoldRequested", "one");
  await handle(c, e);
  await handle(c, e);
  await handle(c, request("SeatHoldRequested", "two"));
  await handle(c, request("SeatReleaseRequested", "one"));
  await handle(c, request("SeatHoldRequested", "two"));
  const d = await c.findOne({ _id: "showtime-1" });
  assert.equal(d.holds.two.status, "rejected");
  assert.equal(d.outbox.length, 3);
});
test("payment protects seats from expiration and confirmation sells the whole group", async () => {
  const c = await setup();
  await handle(c, request("SeatHoldRequested", "one", ["F8", "F9"]));
  await handle(c, request("SeatPaymentRequested", "one"));
  await expire(c, Date.now() + 600000);
  let d = await c.findOne({ _id: "showtime-1" });
  assert.equal(d.holds.one.status, "paying");
  await handle(c, request("SeatConfirmRequested", "one"));
  await handle(c, request("SeatConfirmRequested", "one"));
  d = await c.findOne({ _id: "showtime-1" });
  assert.equal(d.seats.filter((s) => s.status === "sold").length, 2);
  assert.equal(d.outbox.length, 3);
});
test("expiration wins against payment after deadline and emits durable result", async () => {
  const c = await setup();
  await handle(c, request("SeatHoldRequested", "one"));
  const d = await c.findOne({ _id: "showtime-1" });
  d.holds.one.expiresAt = Date.now() - 1;
  c.docs.set(d._id, d);
  await Promise.all([
    expire(c),
    handle(c, request("SeatPaymentRequested", "one")),
  ]);
  const final = await c.findOne({ _id: "showtime-1" });
  assert.equal(final.holds.one.status, "expired");
  assert.equal(final.seats.find((s) => s.id === "F8").status, "available");
  assert.equal(
    final.outbox.filter((o) => o.event.eventType === "SeatHoldExpired").length,
    1,
  );
});
test("release cannot free sold seats or seats owned by another purchase", async () => {
  const c = await setup();
  await handle(c, request("SeatHoldRequested", "one"));
  await handle(c, request("SeatPaymentRequested", "one"));
  await handle(c, request("SeatConfirmRequested", "one"));
  await handle(c, request("SeatReleaseRequested", "one"));
  await handle(c, request("SeatReleaseRequested", "unknown"));
  assert.equal(
    (await c.findOne({ _id: "showtime-1" })).seats.find((s) => s.id === "F8")
      .status,
    "sold",
  );
});
