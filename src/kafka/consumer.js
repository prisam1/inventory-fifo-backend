// src/kafka/consumer.js
const { Kafka } = require('kafkajs');
const config = require('../config');
const logger = require('../utils/logger');
const inventoryService = require('../services/inventoryService'); // <--- Import the service

let consumer = null;

const initializeConsumer = async () => { // Removed dbInstance param as service imports db
    if (consumer) {
        logger.info('Kafka Consumer already initialized.');
        return consumer;
    }

    try {
        const kafka = new Kafka({
            clientId: config.kafka.consumerClientId,
            brokers: config.kafka.brokers,
        });

        consumer = kafka.consumer({ groupId: config.kafka.consumerGroupId });

        await consumer.connect();
        await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const event = JSON.parse(message.value.toString());
                logger.info(`Received event from Kafka: ${JSON.stringify(event)}`);

                try {
                    // Call the service function without passing db explicitly here,
                    // as inventoryService imports db directly.
                    await inventoryService.processInventoryEvent(event);
                    logger.info(`Processed event for product ${event.product_id}, type ${event.event_type}`);
                } catch (error) {
                    logger.error(`Error processing Kafka event for product ${event.product_id}: ${error.message}`, error);
                }
            },
        });
        logger.info(`Kafka Consumer connected and subscribed to topic: ${config.kafka.topic}`);
        return consumer;
    } catch (error) {
        logger.error('Error connecting Kafka Consumer:', error);
        throw error;
    }
};

const disconnectConsumer = async () => {
    if (consumer) {
        try {
            await consumer.disconnect();
            logger.info('Kafka Consumer disconnected.');
            consumer = null;
        } catch (error) {
            logger.error(`Error disconnecting Kafka Consumer: ${error.message}`, error);
        }
    }
};

module.exports = {
    initializeConsumer,
    disconnectConsumer,
};