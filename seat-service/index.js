const express = require("express");
const { boot } = require("../shared/runtime");
const { seed, handle, expire, list, seats } = require("./domain");
async function main() {
  const runtime = await boot("seat-service", "seats");
  const collection = runtime.db.collection("showtimes");
  await seed(collection);
  runtime.dispatch(collection);
  await runtime.consume(["cinema.seat-hold-requests"], (message) =>
    handle(collection, message),
  );
  runtime.repeat(() => expire(collection), 1000);
  const app = express();
  app.get("/showtimes", async (_request, response, next) => {
    try {
      response.json(await list(collection));
    } catch (error) {
      next(error);
    }
  });
  app.get("/showtimes/:id/seats", async (request, response, next) => {
    try {
      const result = await seats(collection, request.params.id);
      result
        ? response.json(result)
        : response.status(404).json({ error: "Función no encontrada." });
    } catch (error) {
      next(error);
    }
  });
  app.use((error, _request, response, _next) =>
    response.status(500).json({ error: error.message }),
  );
  await runtime.listen(app, Number(process.env.SEAT_PORT || 3001));
}
if (require.main === module)
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
module.exports = { main };
