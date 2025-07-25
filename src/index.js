// src/index.js
const express = require("express");
const cors = require("cors");
const http = require("http"); // Import http for server shutdown
const config = require("./config");
const db = require("./db"); // Assuming this module manages your DB connection pool
const {
  initializeProducer,
  sendKafkaMessage,
  disconnectProducer,
} = require("./kafka/producer");
const { initializeConsumer, disconnectConsumer } = require("./kafka/consumer");
const logger = require("./utils/logger");

// Import consumer message handler from its dedicated service file
const { handleOrderEventMessage } = require("./services/kafkaConsumerService");

// Import routes
const authRoutes = require("./routes/authRoutes");
const productRoutes = require("./routes/productRoutes");
const {
  router: inventoryEventRoutes,
  setKafkaProducer,
} = require("./routes/inventoryEventRoutes"); // For producer integration

const app = express();
const PORT = config.server.port;
const URL = config.authenticated.uri; // Assuming this is your frontend URL for CORS

// Middleware
app.use(express.json());

const allowedOrigins = ["http://localhost:3000"];

app.use(cors())
//     {
//     origin: function (origin, callback) {
//       if (!origin) return callback(null, true);
//       if (allowedOrigins.indexOf(origin) === -1) {
//         const msg =
//           "The CORS policy for this site does not allow access from the specified Origin.";
//         return callback(new Error(msg), false);
//       }
//       return callback(null, true);
//     },
//   })
// );

// --- Database Connection Test ---
app.get("/api/test-db", async (req, res) => {
  try {
    const result = await db.query("SELECT NOW()");
    res.status(200).json({
      message: "Database connection successful",
      timestamp: result.rows[0].now,
    });
  } catch (error) {
    logger.error("Database connection test error:", error);
    res.status(500).json({
      message: "Database connection failed",
      error: error.message,
    });
  }
});
// --- End Database Connection Test ---

// --- Inject Kafka producer into routes ---
// This ensures your routes can use sendKafkaMessage for specific topics
setKafkaProducer(sendKafkaMessage, config.kafka.topics.inventoryUpdates);

// Use routes
app.use("/api/auth", authRoutes);
app.use("/api/products", productRoutes);
app.use("/api/inventory-events", inventoryEventRoutes); // Now uses the router from the modified module

// Basic home route
app.get("/", (req, res) => {
  res.send("Welcome to the Inventory FIFO System Backend!");
});

db.query('SELECT 1')
  .then(() => logger.info("✅ PostgreSQL connection test successful"))
  .catch(err => logger.error("❌ PostgreSQL connection test failed", err));


// Start the server
const server = app.listen(PORT, async () => {
  logger.info(`Server running on port ${PORT}`);
 // logger.info(`DB User: ${config.db.user}, DB Name: ${config.db.name}`);
  try {
    // Initialize Kafka Producer
    await initializeProducer();
    logger.info("Kafka Producer initialized successfully.");

    // Initialize Kafka Consumer with the message handler from services
    await initializeConsumer(handleOrderEventMessage);
    logger.info("Kafka Consumer initialized successfully and listening.");
  } catch (error) {
    logger.error("Failed to initialize Kafka:", error);
    // It's generally good practice to exit if Kafka is a core dependency
    process.exit(1);
  }
});

// Handle graceful shutdown
const gracefulShutdown = async () => {
  logger.info("Initiating graceful shutdown...");

  // Disconnect Kafka clients
  await Promise.all([
    disconnectProducer().catch((err) =>
      logger.error("Error disconnecting producer:", err)
    ),
    disconnectConsumer().catch((err) =>
      logger.error("Error disconnecting consumer:", err)
    ),
  ]);

  // Close DB pool connection gracefully
  await db
    .end()
    .catch((err) =>
      logger.error("Error closing database connection pool:", err)
    );

  // Close HTTP server
  server.close(() => {
    logger.info("HTTP server closed.");
    process.exit(0);
  });
};

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown); // For local Ctrl+C
