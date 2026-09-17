const { boot } = require("../shared/runtime");
const { topics } = require("../shared/events");

async function recordNotification(collection, event) {
  if (event.eventType !== "TicketPurchaseCompleted") return;
  const data = event.data;
  const document = {
    _id: data.purchaseId,
    purchaseId: data.purchaseId,
    customer: data.customer,
    showtimeId: data.showtimeId,
    message: `Compra confirmada: ${data.seatIds.join(", ")}`,
    status: "simulated",
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
async function main() {
  const runtime = await boot("notification-service", "notifications");
  const collection = runtime.db.collection("notifications");
  await runtime.consume([topics.purchases], (event) =>
    recordNotification(collection, event),
  );
}
if (require.main === module)
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
module.exports = { recordNotification };
