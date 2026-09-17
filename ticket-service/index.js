const { randomUUID } = require("node:crypto");
const { boot } = require("../shared/runtime");
const { pending, topics } = require("../shared/events");

async function issueTickets(collection, event) {
  if (event.eventType !== "TicketPurchaseCompleted") return;
  const data = event.data;
  const tickets = data.seatIds.map((seatId) => ({
    id: randomUUID(),
    seatId,
    showtimeId: data.showtimeId,
  }));
  const document = {
    _id: data.purchaseId,
    purchaseId: data.purchaseId,
    tickets,
    customer: data.customer,
    showtimeId: data.showtimeId,
    createdAt: new Date().toISOString(),
    outbox: [
      pending(
        "TicketsIssued",
        { purchaseId: data.purchaseId, tickets },
        topics.tickets,
        data.purchaseId,
      ),
    ],
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
  const runtime = await boot("ticket-service", "tickets");
  const collection = runtime.db.collection("tickets");
  runtime.dispatch(collection);
  await runtime.consume([topics.purchases], (event) =>
    issueTickets(collection, event),
  );
}
if (require.main === module)
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
module.exports = { issueTickets };
