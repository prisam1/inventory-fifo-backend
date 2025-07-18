const { Kafka } = require("kafkajs");
const config = require("../config");
const logger = require("../utils/logger");
const inventoryService = require("../services/inventoryService");
const fs = require('fs');  
const path = require('path');  
let consumer = null;

const initializeConsumer = async () => {
  if (consumer) {
    logger.info("Kafka Consumer already initialized.");
    return consumer;
  }

  try {
    const brokers = process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(",")
      : config.kafka.brokers.split(",");

    const saslUsername = process.env.KAFKA_USERNAME;
    const saslPassword = process.env.KAFKA_PASSWORD;

    const kafkaConfig = {
      clientId: config.kafka.clientId,
      brokers: brokers,
      ssl: true, 
    };

    // *** FOR CA CERTIFICATE HANDLING ***
    if (process.env.KAFKA_CA_CERT_BASE64) {
        const caCertPath = path.join('/tmp', 'aiven-ca-consumer.pem');  
        
        if (!fs.existsSync(path.dirname(caCertPath))) {
            fs.mkdirSync(path.dirname(caCertPath), { recursive: true });
        }
        fs.writeFileSync(caCertPath, Buffer.from(process.env.KAFKA_CA_CERT_BASE64, 'base64').toString('utf-8'));

        kafkaConfig.ssl = {
            ca: [fs.readFileSync(caCertPath, 'utf-8')],  
        };
        logger.info('Kafka Consumer configured with SSL/TLS and custom CA certificate.');
    } else {
        logger.warn('KAFKA_CA_CERT_BASE64 not found for Consumer. Kafka client proceeding with basic SSL, which may cause certificate errors.');
        
        kafkaConfig.ssl = true; 
    }
 
    if (saslUsername && saslPassword) {
      kafkaConfig.sasl = {
        mechanism: "scram-sha-256",  
        username: saslUsername,
        password: saslPassword,
      };
      logger.info("Kafka Consumer configured with SASL authentication.");
    } else {
      logger.warn("Kafka Consumer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing).");
    }
    // *** END OF CA CERTIFICATE BLOCK ***

    const kafka = new Kafka(kafkaConfig);

    consumer = kafka.consumer({ groupId: config.kafka.consumerGroupId });

    await consumer.connect();
    await consumer.subscribe({
      topic: config.kafka.topic,
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const event = JSON.parse(message.value.toString());
        logger.info(`Received event from Kafka: ${JSON.stringify(event)}`);

        try {
          await inventoryService.processInventoryEvent(event);
          logger.info(
            `Processed event for product ${event.product_id}, type ${event.event_type}`
          );
        } catch (error) {
          logger.error(
            `Error processing Kafka event for product ${event.product_id}: ${error.message}`,
            error
          );
        }
      },
    });
    logger.info(
      `Kafka Consumer connected and subscribed to topic: ${config.kafka.topic}`
    );
    return consumer;
  } catch (error) {
    logger.error("Error connecting Kafka Consumer:", error);
    throw error;
  }
};

const disconnectConsumer = async () => {
  if (consumer) {
    try {
      await consumer.disconnect();
      logger.info("Kafka Consumer disconnected.");
      consumer = null;
    } catch (error) {
      logger.error(
        `Error disconnecting Kafka Consumer: ${error.message}`,
        error
      );
    }
  }
};

module.exports = {
  initializeConsumer,
  disconnectConsumer,
};
