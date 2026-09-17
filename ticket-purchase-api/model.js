function normalize(body) {
  const { customer, showtimeId, seatIds, paymentMode } = body || {};
  if (
    !customer ||
    typeof customer.name !== "string" ||
    !customer.name.trim() ||
    customer.name.length > 100 ||
    typeof customer.email !== "string" ||
    !/^\S+@\S+\.\S+$/.test(customer.email) ||
    customer.email.length > 200 ||
    typeof showtimeId !== "string" ||
    !/^showtime-[\w-]+$/.test(showtimeId) ||
    !Array.isArray(seatIds) ||
    !seatIds.length ||
    seatIds.length > 12 ||
    seatIds.some((s) => typeof s !== "string" || !/^[A-Z]\d{1,2}$/.test(s)) ||
    new Set(seatIds).size !== seatIds.length ||
    !["approved", "rejected", "delayed"].includes(paymentMode)
  )
    throw Object.assign(new Error("Datos de compra inválidos"), {
      status: 400,
    });
  return {
    customer: { name: customer.name.trim(), email: customer.email.trim() },
    showtimeId,
    seatIds: [...seatIds].sort(),
    paymentMode,
  };
}
module.exports = { normalize };
