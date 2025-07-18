require("dotenv").config();

const config = {
  server: {
    port: process.env.SERVER_PORT || 3001,
  },
  db: {
    user: process.env.DB_USER || "inventory_user",
    host: process.env.DB_HOST || "localhost",
    name: process.env.DB_NAME || "inventory_db",
    password: process.env.DB_PASSWORD || "inventorypass",
    port: parseInt(process.env.DB_PORT || "5432", 10),
  },
  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || "inventory-app",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    topic: process.env.KAFKA_TOPIC || "inventory-events",
    consumerGroupId:
      process.env.KAFKA_CONSUMER_GROUP_ID || "inventory-fifo-group",
    consumerClientId:
      process.env.KAFKA_CONSUMER_CLIENT_ID || "inventory-app-consumer",  
  },
  jwt: {
    secret:
      process.env.JWT_SECRET || "supersecretjwtkey_default_fallback_change_me",
    expiresIn: process.env.JWT_EXPIRES_IN || "1h",
  },
};

module.exports = config;
