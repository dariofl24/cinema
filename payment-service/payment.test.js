const test = require("node:test");
const assert = require("node:assert/strict");
const { validateAuthorization } = require("./index");

test("authorization validates amount, identity and supported simulation mode", () => {
  const valid = {
    purchaseId: "purchase-1",
    amount: 250,
    currency: "MXN",
    paymentMode: "approved",
  };
  assert.equal(validateAuthorization(valid), null);
  for (const invalid of [
    { amount: 0 },
    { amount: -1 },
    { amount: Infinity },
    { amount: "250" },
    { purchaseId: "" },
    { paymentMode: "unknown" },
    { currency: "USD" },
  ]) {
    assert.equal(
      typeof validateAuthorization({ ...valid, ...invalid }),
      "string",
    );
  }
});
