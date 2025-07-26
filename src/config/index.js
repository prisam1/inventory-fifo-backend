require("dotenv").config();  
const path = require("path");

const config = {
  // --- Server Configuration ---
  server: {
    port: parseInt(process.env.SERVER_PORT, 10), // Use PORT from .env, default to 10000
  },

  // --- Authenticated URI for CORS ---
  authenticated: {
    uri: process.env.AUTHENTICATED_URI, // Use AUTHENTICATED_URI for frontend URL
  },

  // --- Database Configuration ---
  db: {
    connectionString: process.env.DATABASE_URL,
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    name: process.env.DB_NAME, // Use DB_NAME consistently for database name
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT, 10),
  },

  // --- Kafka Configuration (for node-rdkafka) ---
  kafka: {
    // General Kafka client properties
    clientId: process.env.KAFKA_CLIENT_ID, // Client ID for broker identification
    brokers: process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(",")
      : [],

    // SASL authentication
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
    saslMechanism: process.env.KAFKA_SASL_MECHANISM,

    // SSL/TLS
    caCertPath: path.join(process.cwd(), process.env.KAFKA_CA_CERT_PATH), // Path to CA certificate
    // TLS / SASL
    useMtls: process.env.KAFKA_USE_MTLS === "true",
    sslRejectUnauthorized:
      (process.env.KAFKA_SSL_REJECT_UNAUTHORIZED || "true") === "true",
    clientCertPath: path.join(
      process.cwd(),
      process.env.KAFKA_CLIENT_CERT_PATH
    ),
    clientKeyPath: path.join(process.cwd(), process.env.KAFKA_CLIENT_KEY_PATH),

    // Topics used by the application
    topics: {
      inventoryUpdates: process.env.KAFKA_TOPIC_INVENTORY_UPDATES, // Producer topic
      orderEvents: process.env.KAFKA_TOPIC_ORDER_EVENTS, // Consumer topic
      // You can add more topics here as needed, e.g., KAFKA_TOPIC_SALES_REPORTS
    },

    // Consumer specific properties
    consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID,
    consumerClientId: process.env.KAFKA_CONSUMER_CLIENT_ID, // Specific client ID for consumer if needed
  },

  // --- JWT Configuration ---
  jwt: {
    secret: process.env.JWT_SECRET,
    expiresIn: process.env.JWT_EXPIRES_IN, // Default to 1 hour if not specified
  },

  // --- Logger Configuration ---
  logger: {
    level: process.env.LOG_LEVEL, // Default log level
  },
};

module.exports = config;
