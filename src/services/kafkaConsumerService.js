const logger = require('../utils/logger');
const { processInventoryEvent } = require('./inventoryService');  

/**
 * Handle messages coming from Kafka (inventory-updates topic)
 * We pass (topic, message) from consumer.run(...)
 */
const handleInventoryEventMessage = async (topic, message) => {
  try {
    if (!message || !message.value) {
      logger.error(`[Kafka] Empty message on ${topic}`);
      return;
    }

    const payload = JSON.parse(message.value.toString());
    logger.info(`[Kafka] Message on ${topic}: ${JSON.stringify(payload)}`);

    // producer sends inventory events (purchase/sale), so always process them
    await processInventoryEvent(payload);
    logger.info(`[Kafka] Event processed for product ${payload.product_id} (eventId: ${payload.eventId})`);
  } catch (error) {
    logger.error(`[Kafka] Failed to process message on ${topic}: ${error.message}`, error);
  }
};

module.exports = {
  handleInventoryEventMessage,
};
