const express = require("express");
const axios = require("axios");
const {randomUUID} = require("node:crypto");
const path = require("node:path");
const {boot} = require("../shared/runtime");
const config = require("../shared/config");
const {pending, topics} = require("../shared/events");
const {normalize} = require("./model");

async function start() {

    const runtime = await boot("ticket-purchase-api", "purchases");
    const purchases = runtime.db.collection("purchases");
    await purchases.createIndex({idempotencyKey: 1}, {unique: true});
    const app = express();

    app.use(express.json({limit: "16kb"}));

    const data = (p) => ({
        purchaseId: p._id,
        customer: p.customer,
        showtimeId: p.showtimeId,
        seatIds: p.seatIds,
        amount: p.amount,
        currency: p.currency,
        paymentMode: p.paymentMode,
    });

    async function change(p, from, status, events = [], detail = "") {
        return purchases.updateOne(
            {_id: p._id, status: {$in: from}},
            {
                $set: {status, updatedAt: new Date()},
                $push: {
                    history: {status, at: new Date().toISOString(), detail},
                    ...(events.length ? {outbox: {$each: events}} : {}),
                },
            },
        );
    }

    const command = (type, p) =>
        pending(type, data(p), topics.requests, p.showtimeId);

    async function outcome(p, approved) {
        await change(
            p,
            ["paying", "pending_review"],
            approved ? "confirming" : "releasing",
            [
                ...(!approved
                    ? [pending("PaymentRejected", data(p), topics.purchases)]
                    : []),
                command(approved ? "SeatConfirmRequested" : "SeatReleaseRequested", p),
            ],
            approved
                ? "Pago aprobado; confirmando asientos"
                : "Pago rechazado; liberando asientos",
        );
    }

    async function pay(p) {
        try {
            await axios.post(`${config.paymentUrl}/payment-authorizations`, data(p), {
                timeout: 2000,
            });
            await outcome(p, true);
        } catch (error) {
            if (error.response?.status === 422) {
                await outcome(p, false);
                return;
            }
            await change(
                p,
                ["paying"],
                "pending_review",
                [pending("PaymentPendingReview", data(p), topics.purchases)],
                "Resultado incierto; asientos protegidos. Consulta el pago sin repetirlo.",
            );
        }
    }

    async function handle(e) {
        const p = await purchases.findOne({_id: e.data.purchaseId});
        if (!p) return;
        switch (e.eventType) {
            case "SeatHoldAccepted":
                await change(p, ["requested"], "held", [
                    command("SeatPaymentRequested", p),
                ]);
                break;
            case "SeatHoldRejected":
                await change(
                    p,
                    ["requested"],
                    "rejected",
                    [],
                    e.data.reason || "Asientos no disponibles",
                );
                break;
            case "SeatPaymentAccepted": {
                await change(p, ["held"], "paying");
                const current = await purchases.findOne({_id: p._id});
                if (current.status === "paying") await pay(current);
                break;
            }
            case "SeatPaymentRejected":
            case "SeatHoldExpired":
                await change(
                    p,
                    ["requested", "held"],
                    "expired",
                    [],
                    e.data.reason || "El bloqueo venció",
                );
                break;
            case "SeatConfirmed":
                await change(p, ["confirming"], "completed", [
                    pending("TicketPurchaseCompleted", data(p), topics.purchases),
                ]);
                break;
            case "SeatReleased":
                await change(
                    p,
                    ["releasing"],
                    "rejected",
                    [],
                    "Pago rechazado; asientos liberados",
                );
                break;
            case "TicketsIssued":
                await purchases.updateOne(
                    {_id: p._id},
                    {$set: {tickets: e.data.tickets}},
                );
                break;
        }
    }

    app.get("/health", (_q, r) => r.json({ok: true}));
    for (const route of ["/showtimes", "/showtimes/:id/seats"])
        app.get(route, async (q, r) => {
            const response = await axios.get(`${config.seatUrl}${q.path}`, {
                timeout: 2000,
            });
            r.json(response.data);
        });

    app.post("/ticket-purchases", async (q, r) => {

        const b = normalize(q.body),
            key = q.get("Idempotency-Key");

        if (!key || key.length > 128)
            return r.status(400).json({
                error: "Idempotency-Key es obligatorio (máximo 128 caracteres)",
            });

        const fingerprint = JSON.stringify(b);

        let existing = await purchases.findOne({idempotencyKey: key});

        if (existing) {
            if (existing.fingerprint !== fingerprint)
                return r
                    .status(409)
                    .json({error: "La clave ya se usó con otros datos"});
            return r.status(202).json({purchaseId: existing._id});
        }

        const {data: show} = await axios.get(
            `${config.seatUrl}/showtimes/${b.showtimeId}/seats`,
            {timeout: 2000},
        );

        if (b.seatIds.some((id) => !show.seats.some((s) => s.id === id)))
            return r.status(400).json({error: "Asiento inexistente"});
        const p = {
            _id: randomUUID(),
            ...b,
            amount: show.price * b.seatIds.length,
            currency: show.currency,
            idempotencyKey: key,
            fingerprint,
            status: "requested",
            tickets: [],
            history: [
                {
                    status: "requested",
                    at: new Date().toISOString(),
                    detail: "Solicitud registrada",
                },
            ],
            createdAt: new Date(),
            outbox: [],
        };
        p.outbox.push(command("SeatHoldRequested", p));
        try {
            await purchases.insertOne(p);
        } catch (error) {
            if (error.code !== 11000) throw error;
            existing = await purchases.findOne({idempotencyKey: key});
            if (existing.fingerprint !== fingerprint)
                return r
                    .status(409)
                    .json({error: "La clave ya se usó con otros datos"});
            return r.status(202).json({purchaseId: existing._id});
        }
        r.status(202).json({purchaseId: p._id});
    });

    app.get("/ticket-purchases/:id", async (q, r) => {
        const p = await purchases.findOne(
            {_id: q.params.id},
            {projection: {outbox: 0, fingerprint: 0, idempotencyKey: 0}},
        );
        if (!p) return r.status(404).json({error: "Compra no encontrada"});
        r.json(p);
    });

    app.post("/ticket-purchases/:id/reconcile-payment", async (q, r) => {
        const p = await purchases.findOne({_id: q.params.id});
        if (!p) return r.status(404).json({error: "Compra no encontrada"});
        if (p.status !== "pending_review")
            return r.status(409).json({error: "La compra no requiere revisión"});
        try {
            const {data: auth} = await axios.get(
                `${config.paymentUrl}/payment-authorizations/${p._id}`,
                {timeout: 2000},
            );
            if (!["approved", "rejected"].includes(auth.status))
                return r
                    .status(409)
                    .json({error: "El pago aún no tiene resultado definitivo"});
            await outcome(p, auth.status === "approved");
            r.status(202).json({purchaseId: p._id});
        } catch (error) {
            if (error.response?.status === 404)
                return r.status(409).json({
                    error:
                        "No hay resultado registrado; se conservan los asientos para revisión",
                });
            throw error;
        }
    });

    app.use(express.static(path.join(__dirname, "public")));

    app.use((error, _q, r, _next) =>
        r.status(error.status || error.response?.status || 503).json({
            error:
                error.status === 400
                    ? error.message
                    : "Servicio no disponible o solicitud inválida; intenta nuevamente",
        }),
    );

    // A crash after persisting paying but before processing the HTTP result is uncertain.
    // Never automatically recharge: startup moves it to manual review.
    for await (const p of purchases.find({status: "paying"}))
        await change(
            p,
            ["paying"],
            "pending_review",
            [pending("PaymentPendingReview", data(p), topics.purchases)],
            "Proceso reiniciado; consulta el resultado persistido",
        );
    runtime.dispatch(purchases);
    await runtime.consume([topics.seats, topics.tickets], handle);
    await runtime.listen(app, Number(process.env.PORT || 3000));
    return runtime;
}

if (require.main === module)
    start().catch((e) => {
        console.error(e);
        process.exit(1);
    });
module.exports = {start};
