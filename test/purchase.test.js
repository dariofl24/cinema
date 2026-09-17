const { test } = require("node:test");
const assert = require("node:assert/strict");
const { normalize } = require("../ticket-purchase-api/model");
test("normalizes seat order and rejects malformed purchase input", () => {
  const body = {
    customer: { name: " Ana ", email: "ana@example.com" },
    showtimeId: "showtime-1",
    seatIds: ["F9", "F8"],
    paymentMode: "approved",
  };
  assert.deepEqual(normalize(body).seatIds, ["F8", "F9"]);
  assert.equal(normalize(body).customer.name, "Ana");
  for (const patch of [
    { seatIds: [] },
    { seatIds: ["F8", "F8"] },
    { paymentMode: "other" },
    { customer: {} },
    { seatIds: ["$bad"] },
  ])
    assert.throws(() => normalize({ ...body, ...patch }));
});
