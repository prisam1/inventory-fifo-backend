// src/services/kafkaConsumerService.js
const logger = require('../utils/logger'); // Your logger
const db = require('../db'); // Your database connection

/**
 * Handles incoming Kafka messages from the consumer.
 * This function contains the business logic for processing messages.
 * @param {object} message - The Kafka message object.
 */
const handleOrderEventMessage = async (message) => {
    try {
        const payload = JSON.parse(message.value.toString());
        logger.info(`Processing Order Event:`, payload);

        // --- Business Logic: Consumer writes to DB based on the message ---
        const { orderId, products, totalAmount } = payload;
        if (orderId && products && totalAmount) {
            try {
                // IMPORTANT: Replace this with your actual database logic.
                // This is a placeholder for inserting/updating inventory based on an order.
                // Example: Insert into an 'orders_processed' table
                // await db.query(
                //     `INSERT INTO orders_processed (order_id, products_json, total_amount, processed_at) VALUES ($1, $2, $3, NOW())`,
                //     [orderId, JSON.stringify(products), totalAmount]
                // );
                logger.info(`Order ${orderId} successfully processed and recorded in DB.`);
            } catch (dbError) {
                logger.error(`Error saving order ${orderId} to DB: ${dbError.message}`);
                // Implement robust error handling for DB writes
                // (e.g., retry mechanisms, logging to a dead-letter topic, alerting)
            }
        } else {
            logger.warn(`Received malformed order event message: ${JSON.stringify(payload)}`);
        }
        // --- End Business Logic ---

    } catch (error) {
        logger.error(`Error parsing or processing order event message: ${error.message}`, message.value.toString());
    }
};

module.exports = {
    handleOrderEventMessage,
};