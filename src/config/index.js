require("dotenv").config();

const config = {
  server: {
    port: process.env.SERVER_PORT || 3001,
  },
  db: {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    name: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT, 10),
  },
  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID,
    brokers: process.env.KAFKA_BROKERS.split(","),
    topic: process.env.KAFKA_TOPIC,
    consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID,
    consumerClientId: process.env.KAFKA_CONSUMER_CLIENT_ID,
  },
  jwt: {
    secret: process.env.JWT_SECRET,
    expiresIn: process.env.JWT_EXPIRES_IN || "24h",
  },
};

module.exports = config;
