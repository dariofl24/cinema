const { test, before, after } = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { MongoClient } = require("mongodb");
const { mongo } = require("../shared/config");
const { event } = require("../shared/events");
const { seed, handle, expire } = require("./domain");

const client = new MongoClient(mongo, { serverSelectionTimeoutMS: 5000 });
const databaseName = `cinema_seat_test_${randomUUID().replaceAll("-", "")}`;
let db;
before(async () => {
  await client.connect();
  db = client.db(databaseName);
});
after(async () => {
  if (db) await db.dropDatabase();
  await client.close();
});
async function fixture() {
  const collection = db.collection(`showtimes_${randomUUID()}`);
  await seed(collection);
  return collection;
}
const command = (type, purchaseId, seatIds = ["F8", "F9"]) =>
  event(type, { purchaseId, showtimeId: "showtime-1", seatIds });
const read = (collection) => collection.findOne({ _id: "showtime-1" });
const events = (document, type) =>
  document.outbox.filter((item) => item.event.eventType === type);

test("Mongo: 24 competing whole-group requests persist exactly one winner", async () => {
  const collection = await fixture();
  const purchases = Array.from({ length: 24 }, () => randomUUID());
  await Promise.all(
    purchases.map((id, i) =>
      handle(
        collection,
        command("SeatHoldRequested", id, ["F8", i % 2 ? "F9" : "F10"]),
      ),
    ),
  );
  const document = await read(collection);
  const winners = Object.entries(document.holds).filter(
    ([, hold]) => hold.status === "held",
  );
  assert.equal(winners.length, 1);
  assert.equal(
    document.seats.filter((seat) => seat.status === "held").length,
    2,
  );
  assert.ok(
    document.seats
      .filter((seat) => seat.purchaseId)
      .every((seat) => seat.purchaseId === winners[0][0]),
  );
  assert.equal(events(document, "SeatHoldAccepted").length, 1);
  assert.equal(events(document, "SeatHoldRejected").length, 23);
  await Promise.all(
    purchases.map((id) => handle(collection, command("SeatHoldRequested", id))),
  );
  assert.equal((await read(collection)).outbox.length, 24);
});

test("Mongo: payment racing expiry at the exact deadline has one coherent winner", async () => {
  // Expiration receives its clock explicitly. Payment starts before that deadline;
  // Mongo decides which CAS wins, and the loser must reread that committed state.
  for (let iteration = 0; iteration < 12; iteration += 1) {
    const collection = await fixture();
    const purchaseId = randomUUID();
    await handle(collection, command("SeatHoldRequested", purchaseId));
    const deadline = (await read(collection)).holds[purchaseId].expiresAt;
    await Promise.all([
      expire(collection, deadline),
      handle(collection, command("SeatPaymentRequested", purchaseId)),
    ]);
    const document = await read(collection);
    const hold = document.holds[purchaseId];
    assert.ok(["paying", "expired"].includes(hold.status));
    if (hold.status === "paying") {
      assert.equal(hold.paymentResult, "accepted");
      assert.equal(
        document.seats.filter((seat) => seat.status === "paying").length,
        2,
      );
      assert.equal(events(document, "SeatHoldExpired").length, 0);
      assert.equal(events(document, "SeatPaymentAccepted").length, 1);
    } else {
      assert.equal(hold.paymentResult, "rejected");
      assert.ok(document.seats.every((seat) => seat.status === "available"));
      assert.equal(events(document, "SeatHoldExpired").length, 1);
      assert.equal(events(document, "SeatPaymentRejected").length, 1);
    }
  }
});

test("Mongo: already expired holds cannot start payment; earlier payments never expire", async () => {
  const collection = await fixture();
  const expired = randomUUID();
  await handle(collection, command("SeatHoldRequested", expired));
  await collection.updateOne(
    { _id: "showtime-1" },
    {
      $set: { [`holds.${expired}.expiresAt`]: Date.now() - 1 },
      $inc: { revision: 1 },
    },
  );
  await Promise.all([
    expire(collection),
    handle(collection, command("SeatPaymentRequested", expired)),
  ]);
  let document = await read(collection);
  assert.equal(document.holds[expired].status, "expired");
  assert.equal(events(document, "SeatHoldExpired").length, 1);
  assert.equal(events(document, "SeatPaymentRejected").length, 1);
  const paying = randomUUID();
  await handle(collection, command("SeatHoldRequested", paying));
  await handle(collection, command("SeatPaymentRequested", paying));
  await expire(collection, Date.now() + 86400000);
  document = await read(collection);
  assert.equal(document.holds[paying].status, "paying");
  assert.equal(
    document.seats.filter(
      (seat) => seat.purchaseId === paying && seat.status === "paying",
    ).length,
    2,
  );
});

test("Mongo: outbox acknowledgement during a seat update forces reread without resurrecting events", async () => {
  const collection = await fixture();
  const purchaseId = randomUUID();
  await handle(collection, command("SeatHoldRequested", purchaseId));
  const acceptedId = (await read(collection)).outbox[0].event.eventId;
  let injected = false;
  // Gate one real Mongo read to schedule the dispatcher acknowledgement between
  // the read and replace; the production mutation must retry its stale revision.
  const interleaved = {
    findOne: async (query) => {
      const document = await collection.findOne(query);
      if (!injected) {
        injected = true;
        await collection.updateOne(query, {
          $pull: { outbox: { "event.eventId": acceptedId } },
          $inc: { revision: 1 },
        });
      }
      return document;
    },
    replaceOne: (...args) => collection.replaceOne(...args),
  };
  await handle(interleaved, command("SeatPaymentRequested", purchaseId));
  const document = await read(collection);
  assert.equal(document.holds[purchaseId].status, "paying");
  assert.equal(document.outbox.length, 1);
  assert.equal(document.outbox[0].event.eventType, "SeatPaymentAccepted");
  assert.notEqual(document.outbox[0].event.eventId, acceptedId);
});
