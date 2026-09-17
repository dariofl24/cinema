const { MongoClient } = require("mongodb");
const { Kafka, logLevel } = require("kafkajs");
const config = require("./config");
const log = (service, fields) =>
  console.log(
    JSON.stringify({ at: new Date().toISOString(), service, ...fields }),
  );
async function boot(service, dbSuffix) {
  const client = new MongoClient(config.mongo);
  await client.connect();
  const db = client.db(`${config.dbPrefix}_${dbSuffix}`);
  const kafka = new Kafka({
    clientId: service,
    brokers: config.brokers,
    logLevel: logLevel.ERROR,
  });
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  const consumers = [],
    timers = [],
    servers = [];
  let closing = false;
  const active = new Set();
  function repeat(fn, ms) {
    let running = false;
    const tick = () => {
      if (running || closing) return;
      running = true;
      const work = Promise.resolve()
        .then(fn)
        .catch((e) => log(service, { error: e.message }))
        .finally(() => {
          running = false;
          active.delete(work);
        });
      active.add(work);
    };
    const timer = setInterval(tick, ms);
    timers.push(timer);
    tick();
    return timer;
  }
  async function consume(topics, handler) {
    const consumer = kafka.consumer({
      groupId: `${config.groupPrefix}-${service}`,
      allowAutoTopicCreation: false,
    });
    consumers.push(consumer);
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      log(service, {
        error: payload.error.message,
        event: "ConsumerCrash",
        restart: payload.restart,
      });
      if (!payload.restart) process.exit(1);
    });
    await consumer.connect();
    await consumer.subscribe({ topics, fromBeginning: true });
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const event = JSON.parse(message.value.toString());
        if (
          !event ||
          typeof event.eventType !== "string" ||
          typeof event.eventId !== "string" ||
          !event.data ||
          typeof event.data.purchaseId !== "string"
        )
          throw new TypeError("Sobre de evento inválido");
        await handler(event);
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
        log(service, {
          purchaseId: event.data.purchaseId,
          event: event.eventType,
          eventId: event.eventId,
          topic,
          partition,
          offset: message.offset,
        });
      },
    });
  }
  async function flush(collection) {
    for await (const doc of collection.find({
      "outbox.0": { $exists: true },
    })) {
      for (const item of doc.outbox) {
        const result = await producer.send({
          topic: item.topic,
          messages: [{ key: item.key, value: JSON.stringify(item.event) }],
        });
        await collection.updateOne(
          { _id: doc._id },
          {
            $pull: { outbox: { "event.eventId": item.event.eventId } },
            $inc: { revision: 1 },
          },
        );
        log(service, {
          purchaseId: item.event.data.purchaseId,
          event: item.event.eventType,
          eventId: item.event.eventId,
          topic: item.topic,
          partition: result[0]?.partition,
          offset: result[0]?.baseOffset,
        });
      }
    }
  }
  const dispatch = (collection) => repeat(() => flush(collection), 200);
  const listen = (app, port) =>
    new Promise((resolve) => {
      const server = app.listen(port, () => {
        log(service, { listening: port });
        resolve(server);
      });
      servers.push(server);
    });
  async function close() {
    if (closing) return;
    closing = true;
    timers.forEach(clearInterval);
    await Promise.all(servers.map((s) => new Promise((r) => s.close(r))));
    await Promise.all(consumers.map((c) => c.disconnect()));
    await Promise.all(active);
    await producer.disconnect();
    await client.close();
  }
  for (const signal of ["SIGINT", "SIGTERM"])
    process.once(signal, () => close().then(() => process.exit(0)));
  return { db, consume, dispatch, listen, close, repeat, flush };
}
module.exports = { boot, log };
