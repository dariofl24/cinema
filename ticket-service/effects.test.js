const test = require("node:test");
const assert = require("node:assert/strict");
const { MongoClient } = require("mongodb");
const { randomUUID } = require("node:crypto");
const config = require("../shared/config");
const { issueTickets } = require("./index");
const { recordNotification } = require("../notification-service");
const { recordPurchase, summary } = require("../analytics-service");
const { authorize } = require("../payment-service");

test(
  "replayed completion has one ticket batch, notification and analytics effect; authorization is immutable",
  { skip: process.env.CINEMA_EFFECTS_INTEGRATION !== "1" },
  async () => {
    const client = new MongoClient(config.mongo);
    await client.connect();
    const db = client.db(
      `cinema_effects_test_${randomUUID().replaceAll("-", "")}`,
    );
    try {
      const data = {
        purchaseId: "purchase-1",
        amount: 250,
        currency: "MXN",
        seatIds: ["F8", "F9"],
        showtimeId: "show-1",
        customer: { name: "Ana", email: "ana@example.com" },
      };
      const event = { eventType: "TicketPurchaseCompleted", data };
      await Promise.all(
        Array.from({ length: 4 }, () =>
          issueTickets(db.collection("tickets"), event),
        ),
      );
      await Promise.all(
        Array.from({ length: 4 }, () =>
          recordNotification(db.collection("notifications"), event),
        ),
      );
      await Promise.all(
        Array.from({ length: 4 }, () =>
          recordPurchase(db.collection("purchases"), event),
        ),
      );
      const tickets = await db
        .collection("tickets")
        .findOne({ _id: data.purchaseId });
      assert.equal(tickets.tickets.length, 2);
      assert.equal(tickets.outbox.length, 1);
      assert.equal(tickets.outbox[0].event.eventType, "TicketsIssued");
      assert.equal(await db.collection("notifications").countDocuments(), 1);
      assert.deepEqual(await summary(db.collection("purchases")), {
        purchases: 1,
        tickets: 2,
        revenue: 250,
        currency: "MXN",
      });
      const payment = { ...data, paymentMode: "delayed" };
      const authorizations = db.collection("authorizations");
      const results = await Promise.all(
        Array.from({ length: 4 }, () => authorize(authorizations, payment)),
      );
      assert.equal(new Set(results.map((r) => r.authorizationId)).size, 1);
      assert.equal(results[0].status, "approved");
      assert.equal(results[0].authorizationCount, 1);
      assert.equal(await authorizations.countDocuments(), 1);
      await assert.rejects(
        authorize(authorizations, { ...payment, amount: 500 }),
        { status: 409 },
      );
      assert.equal(
        (
          await authorize(authorizations, {
            ...payment,
            purchaseId: "rejected",
            paymentMode: "rejected",
          })
        ).status,
        "rejected",
      );
    } finally {
      await db.dropDatabase();
      await client.close();
    }
  },
);
