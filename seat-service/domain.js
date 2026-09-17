const { pending } = require("../shared/events");
const TOPIC = "cinema.seat-hold-events";
const TYPES = new Set([
  "SeatHoldRequested",
  "SeatPaymentRequested",
  "SeatConfirmRequested",
  "SeatReleaseRequested",
]);

async function seed(collection) {
  for (const [index, title] of [
    "Viaje a las estrellas",
    "La ciudad de los sueños",
  ].entries()) {
    const _id = `showtime-${index + 1}`;
    await collection.updateOne(
      { _id },
      {
        $setOnInsert: {
          _id,
          title,
          time: index ? "20:30" : "18:00",
          price: 120,
          currency: "MXN",
          revision: 0,
          seats: ["D", "E", "F"].flatMap((row) =>
            Array.from({ length: 10 }, (_, i) => ({
              id: `${row}${i + 1}`,
              status: "available",
            })),
          ),
          holds: {},
          outbox: [],
        },
      },
      { upsert: true },
    );
  }
}

// Every state transition and its outgoing events share one atomic document replacement.
// A competing request (or the outbox dispatcher) changes revision and forces a fresh read.
async function mutate(collection, showtimeId, change) {
  for (;;) {
    const document = await collection.findOne({ _id: showtimeId });
    if (!document) return null;
    const revision = document.revision;
    if (!change(document)) return document;
    document.revision += 1;
    const result = await collection.replaceOne(
      { _id: showtimeId, revision },
      document,
    );
    if (result.matchedCount) return document;
  }
}
function emit(document, type, data) {
  document.outbox.push(pending(type, data, TOPIC, document._id));
}
function release(document, purchaseId) {
  for (const seat of document.seats) {
    if (
      seat.purchaseId === purchaseId &&
      ["held", "paying"].includes(seat.status)
    ) {
      seat.status = "available";
      delete seat.purchaseId;
    }
  }
}
function expireHolds(document, now) {
  let changed = false;
  for (const [purchaseId, hold] of Object.entries(document.holds)) {
    if (hold.status !== "held" || hold.expiresAt > now) continue;
    hold.status = "expired";
    release(document, purchaseId);
    changed = true;
    emit(document, "SeatHoldExpired", {
      purchaseId,
      showtimeId: document._id,
      seatIds: hold.seatIds,
      reason: "El bloqueo venció antes de iniciar el pago.",
    });
  }
  return changed;
}
async function handle(collection, message) {
  if (!TYPES.has(message.eventType)) return;
  const { purchaseId, showtimeId } = message.data;
  if (
    typeof purchaseId !== "string" ||
    ["__proto__", "prototype", "constructor"].includes(purchaseId)
  )
    throw new Error("purchaseId inválido");
  return mutate(collection, showtimeId, (document) => {
    let changed = expireHolds(document, Date.now());
    const hold = Object.hasOwn(document.holds, purchaseId)
      ? document.holds[purchaseId]
      : null;
    const data = {
      ...message.data,
      seatIds: hold?.seatIds || message.data.seatIds,
    };
    if (message.eventType === "SeatHoldRequested") {
      if (hold) return changed;
      const seatIds = message.data.seatIds;
      const valid =
        Array.isArray(seatIds) &&
        seatIds.length > 0 &&
        new Set(seatIds).size === seatIds.length;
      const available =
        valid &&
        seatIds.every((id) =>
          document.seats.some(
            (seat) => seat.id === id && seat.status === "available",
          ),
        );
      const expiresAt = Date.now() + Number(process.env.HOLD_MS || 300000);
      document.holds[purchaseId] = {
        status: available ? "held" : "rejected",
        seatIds: valid ? seatIds : [],
        expiresAt,
      };
      if (available)
        for (const seat of document.seats)
          if (seatIds.includes(seat.id))
            Object.assign(seat, { status: "held", purchaseId });
      emit(document, available ? "SeatHoldAccepted" : "SeatHoldRejected", {
        ...data,
        expiresAt,
        ...(available
          ? {}
          : { reason: "Uno o más asientos no están disponibles." }),
      });
      return true;
    }
    if (!hold) return changed;
    if (message.eventType === "SeatPaymentRequested") {
      if (hold.paymentResult) return changed;
      const accepted = hold.status === "held";
      hold.paymentResult = accepted ? "accepted" : "rejected";
      if (accepted) {
        hold.status = "paying";
        for (const seat of document.seats)
          if (seat.purchaseId === purchaseId) seat.status = "paying";
      }
      emit(document, accepted ? "SeatPaymentAccepted" : "SeatPaymentRejected", {
        ...data,
        ...(accepted ? {} : { reason: "El bloqueo ya no está vigente." }),
      });
      return true;
    }
    if (
      message.eventType === "SeatConfirmRequested" &&
      hold.status === "paying"
    ) {
      hold.status = "sold";
      for (const seat of document.seats)
        if (seat.purchaseId === purchaseId) seat.status = "sold";
      emit(document, "SeatConfirmed", data);
      return true;
    }
    if (
      message.eventType === "SeatReleaseRequested" &&
      ["held", "paying"].includes(hold.status)
    ) {
      hold.status = "released";
      release(document, purchaseId);
      emit(document, "SeatReleased", data);
      return true;
    }
    return changed;
  });
}
async function expire(collection, now = Date.now()) {
  for (const document of await collection.find({}).toArray())
    await mutate(collection, document._id, (latest) =>
      expireHolds(latest, now),
    );
}
const summary = ({ _id, title, time, price, currency }) => ({
  id: _id,
  showtimeId: _id,
  title,
  time,
  price,
  currency,
});
async function list(collection) {
  return (await collection.find({}).toArray()).map(summary);
}
async function seats(collection, id) {
  const document = await mutate(collection, id, (latest) =>
    expireHolds(latest, Date.now()),
  );
  return document
    ? {
        ...summary(document),
        seats: document.seats.map(({ id, status }) => ({ id, status })),
      }
    : null;
}
module.exports = { seed, handle, expire, list, seats };
