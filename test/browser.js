const { chromium, expect } = require("@playwright/test");
const { MongoClient } = require("mongodb");
const config = require("../shared/config");
const { randomUUID } = require("node:crypto");
(async () => {
  const mongo = new MongoClient(config.mongo);
  await mongo.connect();
  const collection = mongo
    .db(`${config.dbPrefix}_seats`)
    .collection("showtimes");
  const id = `showtime-browser-${randomUUID()}`;
  await collection.insertOne({
    _id: id,
    title: "Prueba navegador",
    time: "21:00",
    price: 120,
    currency: "MXN",
    revision: 0,
    holds: {},
    outbox: [],
    seats: ["F8", "F9", "F10"].map((id) => ({ id, status: "available" })),
  });
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    const errors = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(process.env.API_URL || "http://localhost:3000");
    await page.locator("#showtime").selectOption(id);
    await page
      .getByRole("button", { name: "Asiento F8, disponible", exact: true })
      .click();
    await page
      .getByRole("button", { name: "Asiento F9, disponible", exact: true })
      .click();
    await expect(page.locator("#total")).toContainText("240");
    await page.locator("#name").fill("Ana Navegador");
    await page.locator("#email").fill("ana@example.com");
    await page.locator("#payment-mode").selectOption("delayed");
    await page.locator("#buy").click();
    await expect(page.locator("#status")).toHaveText(
      "Pago pendiente de revisión",
      { timeout: 20000 },
    );
    await expect(
      page.getByRole("button", { name: "Asiento F8, ocupado", exact: true }),
    ).toBeDisabled();
    await page.locator("#reconcile").click();
    await expect(page.locator("#status")).toHaveText("Compra completada", {
      timeout: 20000,
    });
    await expect(page.locator(".ticket")).toHaveCount(2, { timeout: 20000 });
    await page
      .getByRole("button", { name: "Asiento F10, disponible", exact: true })
      .click();
    await page.locator("#payment-mode").selectOption("rejected");
    await page.locator("#buy").click();
    await expect(page.locator("#status")).toHaveText("Compra rechazada", {
      timeout: 20000,
    });
    await expect(
      page.getByRole("button", {
        name: "Asiento F10, disponible",
        exact: true,
      }),
    ).toBeEnabled({ timeout: 10000 });
    await page.route("**/ticket-purchases", (route) =>
      route.fulfill({
        status: 503,
        contentType: "application/json",
        body: JSON.stringify({ error: "Error de prueba" }),
      }),
    );
    await page
      .getByRole("button", { name: "Asiento F10, disponible", exact: true })
      .click();
    await page.locator("#buy").click();
    await expect(page.locator("#error")).toContainText("Error de prueba");
    await page.unroute("**/ticket-purchases");
    // Retry the saved body/key even after availability changes or a page reload.
    await page.reload();
    await expect(page.locator("#recover")).toBeVisible();
    const previousId = await page.locator("#purchase-id").textContent();
    await page.locator("#recover").click();
    await expect(page.locator("#purchase-id")).not.toHaveText(previousId);
    await expect(page.locator("#status")).toHaveText("Compra rechazada", {
      timeout: 20000,
    });
    await expect(page.locator("#recover")).toBeHidden();
    // Lose the HTTP response only after the real API has accepted the purchase.
    await page.locator("#showtime").selectOption(id);
    await page
      .getByRole("button", { name: "Asiento F10, disponible", exact: true })
      .click();
    await page.locator("#name").fill("Respuesta perdida");
    await page.locator("#email").fill("lost@example.com");
    await page.locator("#payment-mode").selectOption("approved");
    let acceptedId;
    await page.route("**/ticket-purchases", async (route) => {
      const response = await route.fetch();
      acceptedId = (await response.json()).purchaseId;
      await route.abort("failed");
    });
    await page.locator("#buy").click();
    await expect(page.locator("#recover")).toBeVisible();
    await expect(page.locator("#recover")).toBeEnabled();
    await page.unroute("**/ticket-purchases");
    await page.reload();
    await page.locator("#recover").click();
    await expect(page.locator("#purchase-id")).toHaveText(acceptedId);
    await expect(page.locator("#status")).toHaveText("Compra completada", {
      timeout: 20000,
    });

    if (errors.length) throw Error(errors.join("\n"));
    console.log(
      "Browser: selección, importe, timeout, asientos protegidos, conciliación, boletos, rechazo y error HTTP verificados.",
    );
  } finally {
    await browser?.close();
    await collection.deleteOne({ _id: id });
    await mongo.close();
  }
})().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
