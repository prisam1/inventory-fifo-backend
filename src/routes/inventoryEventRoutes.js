// src/routes/inventoryEventRoutes.js
const express = require("express");
const router = express.Router();
const authenticateToken = require("../middleware/authenticateToken");
const {
  sendInventoryEvent,
  getInventoryTransactions,
} = require("../controllers/inventoryController");

// Route to send an inventory event (purchase or sale) to Kafka
router.post("/send-event", authenticateToken, sendInventoryEvent); // <-- Using the named handlerFunction

// NEW ROUTE for Transaction Ledger
router.get("/transactions", authenticateToken, getInventoryTransactions);

module.exports = router;
