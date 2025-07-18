// src/routes/inventoryEventRoutes.js
const express = require("express");
const router = express.Router();
const authenticateToken = require("../middleware/authenticateToken");
const inventoryController = require("../controllers/inventoryController"); // Import the whole controller
const logger = require('../utils/logger');

// --- Kafka Producer Injection ---
let injectedSendKafkaMessageFn = null;
let injectedKafkaTopic = null;

const setKafkaProducer = (fn, topic) => {
    if (typeof fn === 'function' && typeof topic === 'string') {
        injectedSendKafkaMessageFn = fn;
        injectedKafkaTopic = topic;
        logger.info(`Kafka producer functions set for inventoryEventRoutes for topic: ${injectedKafkaTopic}`);
    } else {
        logger.error("Invalid arguments passed to setKafkaProducer. Expected (function, string).");
    }
};

// Route to send an inventory event (purchase or sale) to Kafka
router.post("/send-event", authenticateToken, async (req, res) => {
    // Call the controller method, passing the injected Kafka function and topic
    await inventoryController.sendInventoryEvent(req, res, injectedSendKafkaMessageFn, injectedKafkaTopic);
});

// NEW ROUTE for Transaction Ledger
router.get("/transactions", authenticateToken, inventoryController.getInventoryTransactions); // No change needed here

// --- Correct Export for setKafkaProducer ---
module.exports = { router, setKafkaProducer };