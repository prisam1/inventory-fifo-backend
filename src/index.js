const express = require("express");
const cors = require("cors");
const config = require("./config");
const db = require("./db");
const logger = require("./utils/logger");

const { ensureTopics } = require("./kafka/admin");
const { initializeProducer, sendKafkaMessage, disconnectProducer } = require("./kafka/producer");
const { initializeConsumer, disconnectConsumer } = require("./kafka/consumer");
const { handleInventoryEventMessage } = require("./services/kafkaConsumerService");

const authRoutes = require("./routes/authRoutes");
const productRoutes = require("./routes/productRoutes");
const { router: inventoryEventRoutes, setKafkaProducer } = require("./routes/inventoryEventRoutes");

const app = express();
const PORT = config.server.port;

app.use(express.json());
app.use(cors());

// Safe routes
app.use("/api/auth", authRoutes);
app.use("/api/products", productRoutes);

// DB test
app.get("/api/test-db", async (req, res) => {
  try {
    const result = await db.query("SELECT NOW()");
    res.status(200).json({ message: "Database connection successful", timestamp: result.rows[0].now });
  } catch (error) {
    logger.error("Database connection test error:", error);
    res.status(500).json({ message: "Database connection failed", error: error.message });
  }
});

app.get("/", (req, res) => res.send("Welcome to the Inventory FIFO System Backend!"));

(async function boot() {
  logger.info(`--------> ${config.kafka.topics.inventoryUpdates}`);

  try {
    // 1) Ensure topics exist
    await ensureTopics();

    // 2) Producer
    logger.info("🟡 Initializing KafkaJS Producer...");
    await initializeProducer();
    setKafkaProducer(sendKafkaMessage, config.kafka.topics.inventoryUpdates);
    logger.info("✅ KafkaJS Producer injected.");

    // 3) Consumer
    logger.info("🟡 Initializing KafkaJS Consumer...");
    await initializeConsumer(handleInventoryEventMessage);
    logger.info("✅ KafkaJS Consumer ready.");

    // 4) Now mount inventory routes (they need the injected producer)
    app.use("/api/inventory-events", inventoryEventRoutes);

    const server = app.listen(PORT, () => {
      logger.info(`Server running on port ${PORT}`);
      logger.info(`DB User: ${config.db.user}, DB Name: ${config.db.name}`);
    });

    const gracefulShutdown = async () => {
      logger.info("Initiating graceful shutdown...");
      await Promise.all([
        disconnectProducer().catch((err) => logger.error("Error disconnecting producer:", err)),
        disconnectConsumer().catch((err) => logger.error("Error disconnecting consumer:", err)),
      ]);
      await db.end().catch((err) => logger.error("Error closing DB connection pool:", err));
      server.close(() => {
        logger.info("HTTP server closed.");
        process.exit(0);
      });
    };

    process.on("SIGTERM", gracefulShutdown);
    process.on("SIGINT", gracefulShutdown);
  } catch (err) {
    logger.error("❌ Fatal boot error:", err);
    process.exit(1);
  }
})();
