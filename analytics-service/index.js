const { boot, log } = require("../shared/runtime");
const { topics } = require("../shared/events");

async function recordPurchase(collection, event) {
  if (event.eventType !== "TicketPurchaseCompleted") return;
  const data = event.data;
  const document = {
    _id: data.purchaseId,
    purchaseId: data.purchaseId,
    showtimeId: data.showtimeId,
    ticketCount: data.seatIds.length,
    amount: data.amount,
    currency: data.currency,
    createdAt: new Date().toISOString(),
  };
  try {
    await collection.updateOne(
      { _id: data.purchaseId },
      { $setOnInsert: document },
      { upsert: true },
    );
  } catch (error) {
    if (error.code !== 11000) throw error;
  }
}
async function summary(collection) {
  const [result] = await collection
    .aggregate([
      {
        $group: {
          _id: null,
          purchases: { $sum: 1 },
          tickets: { $sum: "$ticketCount" },
          revenue: { $sum: "$amount" },
        },
      },
      { $project: { _id: 0, purchases: 1, tickets: 1, revenue: 1 } },
    ])
    .toArray();
  return {
    ...(result || { purchases: 0, tickets: 0, revenue: 0 }),
    currency: "MXN",
  };
}
async function main() {
  const runtime = await boot("analytics-service", "analytics");
  const collection = runtime.db.collection("purchases");
  await runtime.consume([topics.purchases], async (event) => {
    await recordPurchase(collection, event);
    if (event.eventType === "TicketPurchaseCompleted")
      log("analytics-service", {
        purchaseId: event.data.purchaseId,
        event: "AnalyticsSummary",
        ...(await summary(collection)),
      });
  });
}
if (require.main === module)
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
module.exports = { recordPurchase, summary };
