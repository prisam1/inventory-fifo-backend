const express = require("express");
const cors = require("cors");
const config = require("./config");
const db = require("./db");
const { initializeProducer, disconnectProducer } = require("./kafka/producer");
const { initializeConsumer, disconnectConsumer } = require("./kafka/consumer");
const logger = require("./utils/logger");

// Import routes
const authRoutes = require("./routes/authRoutes");
const productRoutes = require("./routes/productRoutes");
const inventoryEventRoutes = require("./routes/inventoryEventRoutes");

const app = express();
const PORT = config.server.port;

// Middleware
app.use(cors());
app.use(express.json());

// --- Database Connection Test ---
app.get("/api/test-db", async (req, res) => {
  try {
    const result = await db.query("SELECT NOW()");
    res.status(200).json({
      message: "Database connection successful",
      timestamp: result.rows[0].now,
    });
  } catch (error) {
    console.error("Database connection test error:", error);
    res.status(500).json({
      message: "Database connection failed",
      error: error.message,
    });
  }
});
// --- End Database Connection Test ---

// Use routes
app.use("/api/auth", authRoutes);
app.use("/api/products", productRoutes);
app.use("/api/inventory-events", inventoryEventRoutes);

// Basic home route
app.get("/", (req, res) => {
  res.send("Welcome to the Inventory FIFO System Backend!");
});

// Start the server
const server = app.listen(PORT, async () => {
  logger.info(`Server running on port ${PORT}`);
  logger.info(`DB User: ${config.db.user}, DB Name: ${config.db.name}`);
  try {
    await initializeProducer();
    // initializeConsumer no longer needs db as it's passed to inventoryService directly
    await initializeConsumer();
  } catch (error) {
    logger.error("Failed to initialize Kafka:", error);
    // process.exit(1); // Consider if app should exit if Kafka fails
  }
});

// Handle graceful shutdown
process.on("SIGTERM", async () => {
  logger.info("SIGTERM signal received: closing HTTP server and Kafka clients");
  await disconnectProducer();
  await disconnectConsumer();
  await db.end(); // Close DB pool connection gracefully
  server.close(() => {
    logger.info("HTTP server closed.");
    process.exit(0);
  });
});

process.on("SIGINT", async () => {
  logger.info("SIGINT signal received: closing HTTP server and Kafka clients");
  await disconnectProducer();
  await disconnectConsumer();
  await db.end(); // Close DB pool connection gracefully
  server.close(() => {
    logger.info("HTTP server closed.");
    process.exit(0);
  });
});
