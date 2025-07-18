// src/kafka/producer.js
const { Kafka } = require("kafkajs");
const config = require("../config");
const logger = require("../utils/logger");

let producer = null; // Declare as mutable

const initializeProducer = async () => {
  if (producer) {
    logger.info("Kafka Producer already initialized.");
    return producer;
  }

  try {
    const brokers = process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(",")
      : config.kafka.brokers.split(",");

    // Configuration for KafkaJS
    const kafkaConfig = {
      clientId: config.kafka.clientId,
      brokers: brokers,
      ssl: true, // Always use SSL for managed services
      sasl: {
        mechanism: "scram-sha-256", // Or 'plain', 'scram-sha-512' as required by your provider
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
      },
      // You might need this if the SSL certificate is self-signed or not globally trusted
      ssl: { rejectUnauthorized: false },
    };

    const kafka = new Kafka(kafkaConfig);
    // const kafka = new Kafka({
    //     clientId: config.kafka.consumerClientId,
    //     brokers: config.kafka.brokers,
    // });

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
  sendKafkaMessage, // Export this for controllers to use
  disconnectProducer,
};
