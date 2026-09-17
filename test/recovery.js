const { test, before, after } = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const path = require("node:path");
const { MongoClient } = require("mongodb");
const { Kafka, logLevel } = require("kafkajs");
const config = require("../shared/config");
const { event, pending, topics } = require("../shared/events");
const prefix = `cinema_recovery_${Date.now()}`;
const client = new MongoClient(config.mongo);
const kafka = new Kafka({
  brokers: config.brokers,
  clientId: prefix,
  logLevel: logLevel.ERROR,
});
const producer = kafka.producer(),
  children = new Set();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function until(fn, predicate = Boolean) {
  let val;
  for (let i = 0; i < 150; i++) {
    val = await fn();
    if (predicate(val)) return val;
    await sleep(200);
  }
  throw Error("Recovery timeout " + JSON.stringify(val));
}
function start(service) {
  const child = spawn(
    process.execPath,
    [path.join(__dirname, "..", service, "index.js")],
    {
      env: { ...process.env, DB_PREFIX: prefix, GROUP_PREFIX: prefix },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  children.add(child);
  let output = "";
  child.stdout.on("data", (s) => (output += s));
  child.stderr.on("data", (s) => (output += s));
  child.output = () => output;
  return child;
}
async function stop(child) {
  if (child.exitCode !== null) return;
  await new Promise((resolve) => {
    child.once("exit", resolve);
    child.kill("SIGTERM");
  });
  children.delete(child);
}
const completed = (id) =>
  event("TicketPurchaseCompleted", {
    purchaseId: id,
    showtimeId: "recovery",
    seatIds: ["F8"],
    customer: { name: "Recovery", email: "recovery@example.com" },
    amount: 120,
    currency: "MXN",
  });
const send = (e) =>
  producer.send({
    topic: topics.purchases,
    messages: [{ key: e.data.purchaseId, value: JSON.stringify(e) }],
  });
before(async () => {
  await client.connect();
  await producer.connect();
});
after(async () => {
  await Promise.all([...children].map(stop));
  await producer.disconnect();
  for (const suffix of ["analytics", "tickets"])
    await client.db(`${prefix}_${suffix}`).dropDatabase();
  await client.close();
});
test("analytics stopped during an event catches up with stable group and no duplicate effects", async () => {
  const collection = client.db(`${prefix}_analytics`).collection("purchases");
  let child = start("analytics-service");
  const first = completed(randomUUID());
  await send(first);
  await until(() => collection.findOne({ _id: first.data.purchaseId }));
  await stop(child);
  const second = completed(randomUUID());
  await send(second);
  assert.equal(
    await collection.countDocuments({ _id: second.data.purchaseId }),
    0,
  );
  child = start("analytics-service");
  await until(() => collection.findOne({ _id: second.data.purchaseId }));
  await send(second);
  await sleep(600);
  assert.equal(
    await collection.countDocuments({ _id: second.data.purchaseId }),
    1,
  );
  await stop(child);
});
test("ticket worker recovers a durable unpublished outbox entry after restart", async () => {
  const collection = client.db(`${prefix}_tickets`).collection("tickets");
  const id = randomUUID();
  const queued = pending(
    "TicketsIssued",
    { purchaseId: id, tickets: [{ id: "recovered-ticket", seatId: "F8" }] },
    topics.tickets,
    id,
  );
  await collection.insertOne({
    _id: id,
    tickets: queued.event.data.tickets,
    outbox: [queued],
  });
  const consumer = kafka.consumer({ groupId: `${prefix}-observer` });
  let received;
  await consumer.connect();
  await consumer.subscribe({ topics: [topics.tickets], fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const e = JSON.parse(message.value);
      if (e.eventId === queued.event.eventId) received = e;
    },
  });
  try {
    const child = start("ticket-service");
    await until(() => Promise.resolve(received));
    await until(
      () => collection.findOne({ _id: id }),
      (d) => d.outbox.length === 0,
    );
    await stop(child);
    assert.equal(received.data.tickets[0].id, "recovered-ticket");
  } finally {
    await consumer.disconnect();
  }
});

test("a non-retriable malformed Kafka message terminates visibly instead of silently stopping consumption", async () => {
  const topic = `${prefix}-malformed`,
    admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: [{ topic, numPartitions: 3, replicationFactor: 1 }],
  });
  let child;
  try {
    const runtimePath = path.join(__dirname, "../shared/runtime");
    const code = `require(${JSON.stringify(runtimePath)}).boot('invalid-test','invalid').then(async r=>{await r.consume([${JSON.stringify(topic)}],async()=>{});console.log('READY')}).catch(e=>{console.error(e);process.exit(1)})`;
    child = spawn(process.execPath, ["-e", code], {
      env: { ...process.env, DB_PREFIX: prefix, GROUP_PREFIX: prefix },
      stdio: ["ignore", "pipe", "pipe"],
    });
    children.add(child);
    let output = "";
    child.stdout.on("data", (s) => (output += s));
    child.stderr.on("data", (s) => (output += s));
    await until(
      () => Promise.resolve(output),
      (s) => s.includes("READY"),
    );
    await producer.send({
      topic,
      messages: [{ key: "bad", value: "{invalid json" }],
    });
    await until(
      () => Promise.resolve(child.exitCode),
      (code) => code !== null,
    );
    assert.equal(child.exitCode, 1);
    assert.match(output, /ConsumerCrash/);
  } finally {
    if (child) await stop(child);
    await admin.deleteTopics({ topics: [topic] });
    await admin.disconnect();
  }
});
