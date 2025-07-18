const { Kafka } = require("kafkajs");
const config = require("../config");
const logger = require("../utils/logger");
const fs = require('fs');  
const path = require('path');  

let producer = null;

const initializeProducer = async () => {
  if (producer) {
    logger.info("Kafka Producer already initialized.");
    return producer;
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

    // *** IMPORTANT: SSL Configuration with CA Certificate ***
    if (process.env.KAFKA_CA_CERT_BASE64) {
        const caCertPath = path.join('/tmp', 'aiven-ca-producer.pem'); 
        fs.writeFileSync(caCertPath, Buffer.from(process.env.KAFKA_CA_CERT_BASE64, 'base64').toString('utf-8'));

        kafkaConfig.ssl = {
            ca: [fs.readFileSync(caCertPath, 'utf-8')],  
        };
        logger.info('Kafka Producer configured with SSL/TLS and custom CA certificate.');
    } else {
        logger.warn('KAFKA_CA_CERT_BASE64 not found for Producer. Kafka client proceeding with basic SSL, which may cause certificate errors.');
        kafkaConfig.ssl = true; // Fallback to basic SSL
    }
 
    if (saslUsername && saslPassword) {
      kafkaConfig.sasl = {
        mechanism: "scram-sha-256",  
        username: saslUsername,
        password: saslPassword,
      };
      logger.info("Kafka Producer configured with SASL authentication.");
    } else {
      logger.warn("Kafka Producer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing).");
    }

    const kafka = new Kafka(kafkaConfig);

    producer = kafka.producer();
    await producer.connect();
    logger.info("Kafka Producer connected!");
    return producer;
  } catch (error) {
    logger.error(`Failed to connect Kafka Producer: ${error.message}`, error);
    producer = null; // Reset on failure
    throw error;
  }
};

const sendKafkaMessage = async (topic, messages) => {
  if (!producer) {
    logger.error("Kafka Producer not initialized. Cannot send message.");
    throw new Error("Kafka Producer is not connected.");
  }
  try {
    await producer.send({
      topic,
      messages: Array.isArray(messages) ? messages : [messages],
    });
    logger.info(`Message sent to topic ${topic}: ${JSON.stringify(messages)}`);
  } catch (error) {
    logger.error(
      `Error sending message to Kafka topic ${topic}: ${error.message}`,
      error
    );
    throw error;
  }
};

const disconnectProducer = async () => {
  if (producer) {
    try {
      await producer.disconnect();
      logger.info("Kafka Producer disconnected.");
      producer = null;
    } catch (error) {
      logger.error(
        `Error disconnecting Kafka Producer: ${error.message}`,
        error
      );
    }
  }
};

module.exports = {
  initializeProducer,
  sendKafkaMessage,
  disconnectProducer,
};