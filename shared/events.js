const { randomUUID } = require("node:crypto");
const topics = {
  requests: "cinema.seat-hold-requests",
  seats: "cinema.seat-hold-events",
  purchases: "cinema.purchase-events",
  tickets: "cinema.ticket-events",
};
const event = (eventType, data) => ({
  eventId: randomUUID(),
  eventType,
  occurredAt: new Date().toISOString(),
  data,
});
const pending = (type, data, topic, key) => ({
  event: event(type, data),
  topic,
  key: key || data.purchaseId,
});
module.exports = { event, pending, topics };
