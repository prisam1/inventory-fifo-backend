// const { Kafka } = require("kafkajs");
// const config = require("../config");
// const logger = require("../utils/logger");
// const inventoryService = require("../services/inventoryService");
// const fs = require('fs');
// const path = require('path');
// let consumer = null;

// const initializeConsumer = async () => {
//   if (consumer) {
//     logger.info("Kafka Consumer already initialized.");
//     return consumer;
//   }

//   try {
//     const brokers = process.env.KAFKA_BROKERS
//       ? process.env.KAFKA_BROKERS.split(",")
//       : config.kafka.brokers.split(",");

//     const saslUsername = process.env.KAFKA_USERNAME;
//     const saslPassword = process.env.KAFKA_PASSWORD;

//     const kafkaConfig = {
//       clientId: config.kafka.clientId,
//       brokers: brokers,
//       ssl: false,
//     };

//     // *** FOR CA CERTIFICATE HANDLING ***
//     if (process.env.KAFKA_CA_CERT_BASE64) {
//         const caCertPath = path.join('/tmp', 'aiven-ca-consumer.pem');

//         if (!fs.existsSync(path.dirname(caCertPath))) {
//             fs.mkdirSync(path.dirname(caCertPath), { recursive: true });
//         }
//         fs.writeFileSync(caCertPath, Buffer.from(process.env.KAFKA_CA_CERT_BASE64, 'base64').toString('utf-8'));

//         kafkaConfig.ssl = {
//             ca: [fs.readFileSync(caCertPath, 'utf-8')],
//         };
//         logger.info('Kafka Consumer configured with SSL/TLS and custom CA certificate.');
//     } else {
//         logger.warn('KAFKA_CA_CERT_BASE64 not found for Consumer. Kafka client proceeding with basic SSL, which may cause certificate errors.');

//         kafkaConfig.ssl = true;
//     }

//     if (saslUsername && saslPassword) {
//       kafkaConfig.sasl = {
//         mechanism: "scram-sha-256",
//         username: saslUsername,
//         password: saslPassword,
//       };
//       logger.info("Kafka Consumer configured with SASL authentication.");
//     } else {
//       logger.warn("Kafka Consumer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing).");
//     }
//     // *** END OF CA CERTIFICATE BLOCK ***

//     const kafka = new Kafka(kafkaConfig);

//     consumer = kafka.consumer({ groupId: config.kafka.consumerGroupId });

//     await consumer.connect();
//     await consumer.subscribe({
//       topic: config.kafka.topic,
//       fromBeginning: true,
//     });

//     await consumer.run({
//       eachMessage: async ({ topic, partition, message }) => {
//         const event = JSON.parse(message.value.toString());
//         logger.info(`Received event from Kafka: ${JSON.stringify(event)}`);

//         try {
//           await inventoryService.processInventoryEvent(event);
//           logger.info(
//             `Processed event for product ${event.product_id}, type ${event.event_type}`
//           );
//         } catch (error) {
//           logger.error(
//             `Error processing Kafka event for product ${event.product_id}: ${error.message}`,
//             error
//           );
//         }
//       },
//     });
//     logger.info(
//       `Kafka Consumer connected and subscribed to topic: ${config.kafka.topic}`
//     );
//     return consumer;
//   } catch (error) {
//     logger.error("Error connecting Kafka Consumer:", error);
//     throw error;
//   }
// };

// const disconnectConsumer = async () => {
//   if (consumer) {
//     try {
//       await consumer.disconnect();
//       logger.info("Kafka Consumer disconnected.");
//       consumer = null;
//     } catch (error) {
//       logger.error(
//         `Error disconnecting Kafka Consumer: ${error.message}`,
//         error
//       );
//     }
//   }
// };

// module.exports = {
//   initializeConsumer,
//   disconnectConsumer,
// };

// src/kafka/consumer.js
// const { Kafka } = require('kafkajs');
// const config = require('../config');
// const logger = require('../utils/logger');
// const inventoryService = require('../services/inventoryService'); // <--- Import the service

// let consumer = null;

// const initializeConsumer = async () => { // Removed dbInstance param as service imports db
//     if (consumer) {
//         logger.info('Kafka Consumer already initialized.');
//         return consumer;
//     }

//     try {
//         const kafka = new Kafka({
//             clientId: config.kafka.consumerClientId,
//             brokers: config.kafka.brokers,
//         });

//         consumer = kafka.consumer({ groupId: config.kafka.consumerGroupId });

//         await consumer.connect();
//         await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });

//         await consumer.run({
//             eachMessage: async ({ topic, partition, message }) => {
//                 const event = JSON.parse(message.value.toString());
//                 logger.info(`Received event from Kafka: ${JSON.stringify(event)}`);

//                 try {
//                     // Call the service function without passing db explicitly here,
//                     // as inventoryService imports db directly.
//                     await inventoryService.processInventoryEvent(event);
//                     logger.info(`Processed event for product ${event.product_id}, type ${event.event_type}`);
//                 } catch (error) {
//                     logger.error(`Error processing Kafka event for product ${event.product_id}: ${error.message}`, error);
//                 }
//             },
//         });
//         logger.info(`Kafka Consumer connected and subscribed to topic: ${config.kafka.topic}`);
//         return consumer;
//     } catch (error) {
//         logger.error('Error connecting Kafka Consumer:', error);
//         throw error;
//     }
// };

// const disconnectConsumer = async () => {
//     if (consumer) {
//         try {
//             await consumer.disconnect();
//             logger.info('Kafka Consumer disconnected.');
//             consumer = null;
//         } catch (error) {
//             logger.error(`Error disconnecting Kafka Consumer: ${error.message}`, error);
//         }
//     }
// };

// module.exports = {
//     initializeConsumer,
//     disconnectConsumer,
// };

// const { Kafka } = require("kafkajs");
// const config = require("../config");
// const logger = require("../utils/logger");
// const inventoryService = require("../services/inventoryService");
// const fs = require("fs"); // <--- ADD THIS
// const path = require("path"); // <--- ADD THIS

// let consumer = null;

// const initializeConsumer = async () => {
//   if (consumer) {
//     logger.info("Kafka Consumer already initialized.");
//     return consumer;
//   }

//   try {
//     // Retrieve brokers from config, which should come from process.env.KAFKA_BROKERS
//     const brokers = config.kafka.brokers;
//     const saslUsername = process.env.KAFKA_USERNAME;
//     const saslPassword = process.env.KAFKA_PASSWORD;
//     const useSSL = process.env.KAFKA_USE_SSL === "true"; // Controlled by env var

//     const kafkaConfig = {
//       clientId: config.kafka.consumerClientId,
//       brokers: brokers,
//       ssl: useSSL, // Conditional SSL based on KAFKA_USE_SSL
//     };

//     // --- PRODUCTION-SPECIFIC: SSL/TLS with CA Certificate ---
//     if (useSSL && process.env.KAFKA_CA_CERT_BASE64) {
//       const caCertPath = path.join("/tmp", "aiven-ca-consumer.pem"); // Unique name for consumer
//       // Ensure /tmp exists (Render provides it)
//       fs.writeFileSync(
//         caCertPath,
//         Buffer.from(process.env.KAFKA_CA_CERT_BASE64, "base64").toString(
//           "utf-8"
//         )
//       );

//       kafkaConfig.ssl = {
//         ca: [fs.readFileSync(caCertPath, "utf-8")], // Provide the trusted CA certificate
//         // Do NOT use rejectUnauthorized: false here for production!
//       };
//       logger.info(
//         "Kafka Consumer configured with SSL/TLS and custom CA certificate."
//       );
//     } else if (useSSL) {
//       logger.warn(
//         "KAFKA_CA_CERT_BASE64 not found for Consumer. Kafka client proceeding with SSL, but may cause certificate errors."
//       );
//       // This case might still lead to self-signed cert errors if CA is required and not provided.
//     } else {
//       logger.info(
//         'Kafka Consumer configured without SSL (KAFKA_USE_SSL is false or not set to "true").'
//       );
//     }

//     // --- PRODUCTION-SPECIFIC: SASL Authentication ---
//     if (saslUsername && saslPassword) {
//       kafkaConfig.sasl = {
//         mechanism: "scram-sha-256", // Or 'scram-sha-512' - confirm with Aiven
//         username: saslUsername,
//         password: saslPassword,
//       };
//       logger.info("Kafka Consumer configured with SASL authentication.");
//     } else if (useSSL) {
//       // Only warn if SSL is enabled, as production usually requires SASL
//       logger.warn(
//         "Kafka Consumer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing) despite SSL being enabled."
//       );
//     }
//     // --- END PRODUCTION-SPECIFIC CONFIG ---

//     const kafka = new Kafka(kafkaConfig);

//     consumer = kafka.consumer({ groupId: config.kafka.consumerGroupId });

//     await consumer.connect();
//     await consumer.subscribe({
//       topic: config.kafka.topic,
//       fromBeginning: true,
//     });

//     await consumer.run({
//       eachMessage: async ({ topic, partition, message }) => {
//         const event = JSON.parse(message.value.toString());
//         logger.info(`Received event from Kafka: ${JSON.stringify(event)}`);

//         try {
//           await inventoryService.processInventoryEvent(event);
//           logger.info(
//             `Processed event for product ${event.product_id}, type ${event.event_type}`
//           );
//         } catch (error) {
//           logger.error(
//             `Error processing Kafka event for product ${event.product_id}: ${error.message}`,
//             error
//           );
//         }
//       },
//     });
//     logger.info(
//       `Kafka Consumer connected and subscribed to topic: ${config.kafka.topic}`
//     );
//     return consumer;
//   } catch (error) {
//     logger.error("Error connecting Kafka Consumer:", error);
//     throw error;
//   }
// };

// const disconnectConsumer = async () => {
//   if (consumer) {
//     try {
//       await consumer.disconnect();
//       logger.info("Kafka Consumer disconnected.");
//       consumer = null;
//     } catch (error) {
//       logger.error(
//         `Error disconnecting Kafka Consumer: ${error.message}`,
//         error
//       );
//     }
//   }
// };

// module.exports = {
//   initializeConsumer,
//   disconnectConsumer,
// };


const { Kafka } = require('kafkajs');
const config = require('../config');
const logger = require('../utils/logger');
const inventoryService = require('../services/inventoryService');
const fs = require('fs'); // Required for file system operations
const path = require('path'); // Required for path manipulation

let consumer = null;

const initializeConsumer = async () => {
    if (consumer) {
        logger.info('Kafka Consumer already initialized.');
        return consumer;
    }

    // try {
        // Retrieve brokers from config, which should come from process.env.KAFKA_BROKERS
        const brokers = config.kafka.brokers; 
        const saslUsername = process.env.KAFKA_USERNAME;
        const saslPassword = process.env.KAFKA_PASSWORD;
        // Determine if SSL should be used based on KAFKA_USE_SSL env variable
        const useSSL = process.env.KAFKA_USE_SSL === 'true'; 

        const kafkaConfig = {
            clientId: config.kafka.consumerClientId,
            brokers: brokers,
            ssl: useSSL, // Set SSL dynamically based on environment
        };

        logger.debug(`Final Kafka Consumer Config: ${JSON.stringify(kafkaConfig, null, 2)}`);

        // --- Conditional SSL/TLS Configuration with CA Certificate ---
        // This block is crucial for connecting to Aiven Kafka
        if (useSSL && process.env.KAFKA_CA_CERT_BASE64) {
            const caCertPath = path.join('/tmp', 'aiven-ca-consumer.pem'); // Use /tmp for Render deployment
            
            // Ensure the /tmp directory exists. Render provides it, but good practice for local testing if /tmp isn't default.
            // Note: On Render, /tmp is usually available and writeable without explicit mkdir.
            if (!fs.existsSync(path.dirname(caCertPath))) {
                fs.mkdirSync(path.dirname(caCertPath), { recursive: true });
            }

            // Write the base64-decoded CA certificate to a file
            fs.writeFileSync(caCertPath, Buffer.from(process.env.KAFKA_CA_CERT_BASE64, 'base64').toString('utf-8'));

            // Configure KafkaJS to use the custom CA certificate
            kafkaConfig.ssl = {
                ca: [fs.readFileSync(caCertPath, 'utf-8')], // Provide the trusted CA certificate content
                // IMPORTANT: Do NOT include rejectUnauthorized: false here.
                // Providing 'ca' tells Node.js to trust this specific CA,
                // and rejectUnauthorized: false would negate that trust, leading to errors.
            };
            logger.info('Kafka Consumer configured with SSL/TLS and custom CA certificate.');
        } else if (useSSL) {
            // This case means SSL is desired (useSSL is true) but no CA certificate is provided.
            // This will likely lead to "self-signed certificate" errors in production.
            logger.warn('KAFKA_CA_CERT_BASE64 not found for Consumer. Kafka client proceeding with basic SSL, which may cause certificate errors if broker uses untrusted CA.');
        } else {
            // This case means KAFKA_USE_SSL is false, typically for local development.
            logger.info('Kafka Consumer configured without SSL (KAFKA_USE_SSL is false or not set to "true").');
        }

        // --- Conditional SASL Authentication Configuration ---
        // This block is crucial for connecting to Aiven Kafka
        if (saslUsername && saslPassword) {
            kafkaConfig.sasl = {
                mechanism: 'scram-sha-256', // Aiven typically uses 'scram-sha-256' or 'scram-sha-512'
                username: saslUsername,
                password: saslPassword,
            };
            logger.info('Kafka Consumer configured with SASL authentication.');
        } else if (useSSL) { 
            // If SSL is enabled (i.e., production context), but SASL credentials are missing, warn.
            // Most managed Kafka services require SASL with SSL.
            logger.warn('Kafka Consumer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing) despite SSL being enabled. This may cause connection failures.');
        }
        // --- END OF DYNAMIC CONFIGURATION BLOCKS ---

        const kafka = new Kafka(kafkaConfig);

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
//     } catch (error) {
//         logger.error('Error connecting Kafka Consumer:', error);
//         throw error;
//     }
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
