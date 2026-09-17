require("dotenv").config({
  path: require("node:path").join(__dirname, "../.env"),
  quiet: true,
});
module.exports = {
  mongo:
    process.env.MONGODB_URI ||
    "mongodb://admin:admin123@localhost:27017/?authSource=admin",
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  dbPrefix: process.env.DB_PREFIX || "cinema",
  groupPrefix: process.env.GROUP_PREFIX || "cinema",
  seatUrl: process.env.SEAT_SERVICE_URL || "http://localhost:3001",
  paymentUrl: process.env.PAYMENT_SERVICE_URL || "http://localhost:3002",
};
