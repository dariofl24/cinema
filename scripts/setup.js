const { Kafka, logLevel } = require("kafkajs");
const { MongoClient } = require("mongodb");
const config = require("../shared/config");
const { topics } = require("../shared/events");
async function setup() {
  const admin = new Kafka({
    brokers: config.brokers,
    clientId: "cinema-setup",
    logLevel: logLevel.ERROR,
  }).admin();
  const mongo = new MongoClient(config.mongo);
  try {
    await admin.connect();
    const existing = new Set(await admin.listTopics());
    const missing = Object.values(topics).filter(
      (topic) => !existing.has(topic),
    );
    if (missing.length)
      await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map((topic) => ({
          topic,
          numPartitions: 3,
          replicationFactor: 1,
        })),
      });
    for (const t of (
      await admin.fetchTopicMetadata({ topics: Object.values(topics) })
    ).topics) {
      if (t.partitions.length !== 3)
        throw Error(`${t.name}: se requieren tres particiones`);
    }
    await mongo.connect();
    await require("../seat-service/domain").seed(
      mongo.db(`${config.dbPrefix}_seats`).collection("showtimes"),
    );
    await mongo
      .db(`${config.dbPrefix}_purchases`)
      .collection("purchases")
      .createIndex({ idempotencyKey: 1 }, { unique: true });
    console.log("Tópicos, índices y funciones listos.");
  } finally {
    await admin.disconnect();
    await mongo.close();
  }
}
setup().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
