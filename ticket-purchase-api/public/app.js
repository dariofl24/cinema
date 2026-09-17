"use strict";
const $ = (id) => document.getElementById(id);
const labels = {
  requested: "Solicitud registrada",
  held: "Asientos reservados",
  paying: "Pago en proceso",
  confirming: "Confirmando asientos",
  completed: "Compra completada",
  rejected: "Compra rechazada",
  pending_review: "Pago pendiente de revisión",
  expired: "Reserva vencida",
  releasing: "Liberando asientos",
};
let showtimes = [],
  selected = new Set(),
  purchaseId = localStorage.getItem("cinema.purchaseId"),
  pendingRequest = null,
  submitting = false,
  polling = false;
try {
  pendingRequest = JSON.parse(
    localStorage.getItem("cinema.pendingRequest") || "null",
  );
} catch {
  localStorage.removeItem("cinema.pendingRequest");
}
const money = (value, currency = "MXN") =>
  new Intl.NumberFormat("es-MX", { style: "currency", currency }).format(value);
async function api(path, options) {
  const response = await fetch(path, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(
      data.message ||
        data.error ||
        `No se pudo completar la solicitud (${response.status}).`,
    );
    error.status = response.status;
    throw error;
  }
  return data;
}
function updateTotal() {
  const show = showtimes.find((item) => item._id === $("showtime").value);
  $("selection").textContent = selected.size
    ? `Asientos: ${[...selected].join(", ")}`
    : "Sin asientos seleccionados";
  $("total").textContent = money(
    (show?.price || 0) * selected.size,
    show?.currency,
  );
  $("buy").disabled = submitting || !selected.size || Boolean(pendingRequest);
  $("recover").hidden = !pendingRequest;
  $("recover").disabled = submitting;
}
async function refreshSeats() {
  const id = $("showtime").value;
  if (!id) return;
  try {
    const show = await api(`/showtimes/${encodeURIComponent(id)}/seats`);
    if ($("showtime").value !== id) return;
    const seats = show.seats || [];
    for (const seat of seats)
      if (seat.status !== "available") selected.delete(seat.id);
    $("seats").replaceChildren(
      ...seats.map((seat) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "seat";
        button.textContent = seat.id;
        button.disabled = seat.status !== "available";
        button.setAttribute(
          "aria-label",
          `Asiento ${seat.id}, ${button.disabled ? "ocupado" : "disponible"}`,
        );
        button.setAttribute("aria-pressed", String(selected.has(seat.id)));
        button.onclick = () => {
          selected.has(seat.id)
            ? selected.delete(seat.id)
            : selected.add(seat.id);
          button.setAttribute("aria-pressed", String(selected.has(seat.id)));
          updateTotal();
        };
        return button;
      }),
    );
    $("availability").textContent =
      `${seats.filter((seat) => seat.status === "available").length} asientos disponibles · Actualización automática`;
    updateTotal();
  } catch (error) {
    $("availability").textContent =
      `No se pudo actualizar la disponibilidad: ${error.message}`;
  }
}
function renderPurchase(purchase) {
  $("empty").hidden = true;
  $("purchase").hidden = false;
  $("purchase-id").textContent = purchase._id || purchaseId;
  $("status").textContent = labels[purchase.status] || purchase.status;
  $("purchase-summary").textContent =
    `${purchase.customer?.name || ""} · ${(purchase.seatIds || []).join(", ")} · ${money(purchase.amount || 0, purchase.currency)}`;
  $("reconcile").hidden = purchase.status !== "pending_review";
  $("pending-explanation").hidden = purchase.status !== "pending_review";
  $("history").replaceChildren(
    ...(purchase.history || []).map((step) => {
      const li = document.createElement("li");
      li.textContent = `${labels[step.status] || step.status}${step.detail ? ` · ${typeof step.detail === "string" ? step.detail : JSON.stringify(step.detail)}` : ""}`;
      const time = document.createElement("time");
      time.textContent = new Date(step.at).toLocaleString("es-MX");
      li.append(time);
      return li;
    }),
  );
  $("tickets").replaceChildren(
    ...(purchase.tickets || []).map((ticket) => {
      const div = document.createElement("div");
      div.className = "ticket";
      const title = document.createElement("strong");
      title.textContent = `Asiento ${ticket.seatId}`;
      const id = document.createElement("small");
      id.textContent = `Boleto ${ticket.id || ticket._id}`;
      div.append(title, id);
      return div;
    }),
  );
  if (!purchase.tickets?.length)
    $("tickets").textContent =
      purchase.status === "completed"
        ? "Compra confirmada. Esperando la emisión de boletos…"
        : "Los boletos aparecerán después de confirmar la compra.";
}
async function refreshPurchase() {
  if (!purchaseId || polling) return;
  polling = true;
  try {
    renderPurchase(
      await api(`/ticket-purchases/${encodeURIComponent(purchaseId)}`),
    );
    $("poll-error").textContent = "";
  } catch (error) {
    $("empty").hidden = true;
    $("purchase").hidden = false;
    $("poll-error").textContent =
      `No se pudo consultar la compra. Se reintentará automáticamente. ${error.message}`;
  } finally {
    polling = false;
  }
}
$("showtime").onchange = () => {
  selected.clear();
  updateTotal();
  refreshSeats();
};
async function sendPendingRequest() {
  if (submitting || !pendingRequest) return;
  submitting = true;
  updateTotal();
  $("error").textContent = "";
  try {
    const result = await api("/ticket-purchases", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": pendingRequest.key,
      },
      body: pendingRequest.body,
    });
    purchaseId = result.purchaseId;
    localStorage.setItem("cinema.purchaseId", purchaseId);
    pendingRequest = null;
    localStorage.removeItem("cinema.pendingRequest");
    selected.clear();
    await refreshPurchase();
    await refreshSeats();
  } catch (error) {
    if (error.status && error.status < 500) {
      pendingRequest = null;
      localStorage.removeItem("cinema.pendingRequest");
    }
    $("error").textContent =
      `${error.message}${pendingRequest ? " Usa Recuperar solicitud pendiente para reenviar los datos originales con la misma clave, incluso después de recargar la página." : ""}`;
  } finally {
    submitting = false;
    updateTotal();
  }
}
$("purchase-form").onsubmit = async (event) => {
  event.preventDefault();
  if (submitting || !selected.size || pendingRequest) return;
  const body = {
    customer: { name: $("name").value.trim(), email: $("email").value.trim() },
    showtimeId: $("showtime").value,
    seatIds: [...selected].sort(),
    paymentMode: $("payment-mode").value,
  };
  pendingRequest = { key: crypto.randomUUID(), body: JSON.stringify(body) };
  localStorage.setItem("cinema.pendingRequest", JSON.stringify(pendingRequest));
  await sendPendingRequest();
};
$("recover").onclick = sendPendingRequest;
$("reconcile").onclick = async () => {
  $("reconcile").disabled = true;
  try {
    await api(
      `/ticket-purchases/${encodeURIComponent(purchaseId)}/reconcile-payment`,
      { method: "POST" },
    );
    await refreshPurchase();
    await refreshSeats();
  } catch (error) {
    $("poll-error").textContent = error.message;
  } finally {
    $("reconcile").disabled = false;
  }
};
async function init() {
  updateTotal();
  try {
    showtimes = (await api("/showtimes")).map((show) => ({
      ...show,
      _id: show._id || show.id || show.showtimeId,
    }));
    $("showtime").replaceChildren(
      ...showtimes.map((show) => {
        const option = document.createElement("option");
        option.value = show._id;
        option.textContent = `${show.title} · ${/^\d{2}:\d{2}$/.test(show.time) ? show.time : new Date(show.time).toLocaleString("es-MX")} · ${money(show.price, show.currency)}`;
        return option;
      }),
    );
    if (!showtimes.length)
      $("availability").textContent =
        "No hay funciones. Ejecuta npm run setup.";
    await refreshSeats();
  } catch (error) {
    $("error").textContent =
      `No se pudieron cargar las funciones. ${error.message} Recarga la página para reintentar.`;
  }
  await refreshPurchase();
}
init();
setInterval(refreshSeats, 2500);
setInterval(refreshPurchase, 1500);
