const { v4: uuidv4 } = require("uuid");
const inventoryService = require("../services/inventoryService");
const logger = require("../utils/logger");

const sendInventoryEvent = async (req, res, sendKafkaMessageFn, kafkaTopic) => {
  const { product_id, event_type, quantity, unit_price } = req.body;

  if (!product_id || !event_type || !quantity) {
    return res.status(400).json({ message: "Product ID, event type, and quantity are required." });
  }

  if (event_type === "purchase" && (unit_price === undefined || unit_price === null)) {
    return res.status(400).json({ message: "Unit price is required for purchase events." });
  }

  if (!sendKafkaMessageFn || !kafkaTopic) {
    logger.error("Kafka producer function or topic not provided to sendInventoryEvent controller.");
    return res.status(500).json({ message: "Kafka producer not initialized." });
  }

  try {
    const eventId = uuidv4();
    const timestamp = new Date().toISOString();

    const event = {
      eventId,
      product_id,
      event_type,
      quantity: parseFloat(quantity),
      unit_price: event_type === "purchase" ? parseFloat(unit_price) : undefined,
      timestamp,
    };

    await sendKafkaMessageFn(kafkaTopic, event, String(product_id));
    logger.info(`Inventory event accepted for product ${product_id}: ${event_type} ${quantity}`);
    res.status(202).json({ message: "Event accepted for processing", eventId });
  } catch (error) {
    logger.error(`Error sending inventory event: ${error.message}`, error);
    res.status(500).json({ message: "Failed to send inventory event" });
  }
};

const getInventoryTransactions = async (req, res) => {
  try {
    const transactions = await inventoryService.getInventoryTransactions();
    res.status(200).json(transactions);
  } catch (error) {
    logger.error(`Error fetching inventory transactions: ${error.message}`, error);
    res.status(500).json({ message: "Failed to fetch inventory transactions" });
  }
};

module.exports = {
  sendInventoryEvent,
  getInventoryTransactions,
};
