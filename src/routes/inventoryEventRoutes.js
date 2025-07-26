const express = require("express");
const router = express.Router();
const authenticateToken = require("../middleware/authenticateToken");
const inventoryController = require("../controllers/inventoryController");
const logger = require("../utils/logger");

let injectedSendKafkaMessageFn = null;
let injectedKafkaTopic = null;

const setKafkaProducer = (fn, topic) => {
  logger.info("[routes] setKafkaProducer called:", { fnExists: typeof fn === 'function', topic });
  if (typeof fn === "function" && typeof topic === "string") {
    injectedSendKafkaMessageFn = fn;
    injectedKafkaTopic = topic;
    logger.info(`[routes] ✅ injected for topic: ${injectedKafkaTopic}`);
  } else {
    logger.error("[routes] ❌ Invalid arguments to setKafkaProducer");
  }
};

router.post("/send-event", authenticateToken, async (req, res) => {
  logger.info("[routes] /send-event hit");
  if (!injectedSendKafkaMessageFn || !injectedKafkaTopic) {
    logger.error("[routes] Kafka producer function or topic not provided.");
    return res.status(500).json({ message: "Kafka producer not initialized." });
  }

  await inventoryController.sendInventoryEvent(req, res, injectedSendKafkaMessageFn, injectedKafkaTopic);
});

router.get("/transactions", authenticateToken, inventoryController.getInventoryTransactions);

module.exports = { router, setKafkaProducer };
