// src/kafka/kafkaClientFactory.js
const { Kafka } = require("kafkajs");
const fs = require("fs");
const path = require("path");
const logger = require("../utils/logger"); // Ensure your logger is accessible

/**
 * Creates and returns a Kafka client instance with shared configuration logic.
 * @param {object} options - Configuration options.
 * @param {string} options.clientId - The Kafka client ID.
 * @param {string} [options.consumerGroupId] - Optional: The consumer group ID for consumers.
 * @param {'producer'|'consumer'} options.type - The type of client ('producer' or 'consumer').
 * @returns {Kafka} A configured Kafka instance.
 */
const createKafkaInstance = (options) => {
  const { clientId, consumerGroupId, type } = options;

  const brokers = process.env.KAFKA_BROKERS
    ? process.env.KAFKA_BROKERS.split(",")
    : [];
  const saslUsername = process.env.KAFKA_USERNAME;
  const saslPassword = process.env.KAFKA_PASSWORD;
  const useSSL = process.env.KAFKA_USE_SSL === "true";

  if (brokers.length === 0) {
    logger.error("KAFKA_BROKERS environment variable is not set or is empty.");
    throw new Error("Kafka brokers not configured.");
  }

  const kafkaConfig = {
    clientId: clientId,
    brokers: brokers,
    ssl: useSSL,
  };

  // --- Conditional SSL/TLS Configuration with CA Certificate ---
  if (useSSL && process.env.KAFKA_CA_CERT_BASE64) {
    const caCertFileName = `aiven-ca-${type}.pem`; // e.g., aiven-ca-producer.pem or aiven-ca-consumer.pem
    const caCertPath = path.join("/tmp", caCertFileName);

    try {
      fs.writeFileSync(
        caCertPath,
        Buffer.from(process.env.KAFKA_CA_CERT_BASE64, "base64").toString(
          "utf-8"
        )
      );
      kafkaConfig.ssl = {
        ca: [fs.readFileSync(caCertPath, "utf-8")],
      };
      logger.info(
        `Kafka client (${type}) configured with SSL/TLS and custom CA certificate.`
      );
    } catch (error) {
      logger.error(
        `Error writing or reading CA certificate for ${type} client: ${error.message}`
      );
      // Decide if you want to throw here or proceed without CA (likely to fail connection)
      throw new Error(
        `Failed to configure CA certificate for Kafka client (${type}).`
      );
    }
  } else if (useSSL) {
    logger.warn(
      `KAFKA_CA_CERT_BASE64 not found for ${type} client. Kafka client proceeding with basic SSL, which may cause certificate errors.`
    );
  } else {
    logger.info(
      `Kafka client (${type}) configured without SSL (KAFKA_USE_SSL is false or not set to "true").`
    );
  }

  // --- Conditional SASL Authentication Configuration ---
  if (saslUsername && saslPassword) {
    kafkaConfig.sasl = {
      mechanism: "scram-sha-256", // Confirm with Aiven: scram-sha-256 or scram-sha-512
      username: saslUsername,
      password: saslPassword,
    };
    logger.info(`Kafka client (${type}) configured with SASL authentication.`);
  } else if (useSSL) {
    logger.warn(
      `Kafka client (${type}) not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing) despite SSL being enabled. This may cause connection failures.`
    );
  }

  // >>> CRUCIAL DEBUG LOG FOR RENDER DIAGNOSIS <<<
  logger.debug(
    `Final Kafka ${type} Config: ${JSON.stringify(kafkaConfig, null, 2)}`
  );

  const kafka = new Kafka(kafkaConfig);

  return kafka;
};

module.exports = { createKafkaInstance };
