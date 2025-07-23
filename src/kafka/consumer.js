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


 // src/kafka/consumer.js
// const { Kafka } = require('kafkajs');
// const config = require('../config');
// const logger = require('../utils/logger');
// const inventoryService = require('../services/inventoryService');
// const fs = require('fs');
// const path = require('path');

// let consumer = null;

// const initializeConsumer = async () => {
//     if (consumer) {
//         logger.info('Kafka Consumer already initialized.');
//         return consumer;
//     }

//     try {
//         const brokers = config.kafka.brokers; // Assuming config.kafka.brokers is correctly populated from KAFKA_BROKERS
//         const saslUsername = process.env.KAFKA_USERNAME;
//         const saslPassword = process.env.KAFKA_PASSWORD;
//         const useSSL = process.env.KAFKA_USE_SSL === 'true';

//         const kafkaConfig = {
//             clientId: config.kafka.consumerClientId,
//             brokers: brokers,
//             ssl: useSSL, // Initialize ssl based on env var
//         };

//         // --- Conditional SSL/TLS Configuration with CA Certificate ---
//         if (useSSL && process.env.KAFKA_CA_CERT_BASE64) {
//             const caCertPath = path.join('/tmp', 'aiven-ca-consumer.pem'); 
            
//             // Write the base64-decoded CA certificate to a file
//             fs.writeFileSync(caCertPath, Buffer.from(process.env.KAFKA_CA_CERT_BASE64, 'base64').toString('utf-8'));

//             // Configure KafkaJS to use the custom CA certificate
//             kafkaConfig.ssl = {
//                 ca: [fs.readFileSync(caCertPath, 'utf-8')], 
//                 // Do NOT include rejectUnauthorized: false here.
//                 // Providing 'ca' tells Node.js to trust this specific CA.
//             };
//             logger.info('Kafka Consumer configured with SSL/TLS and custom CA certificate.');
//         } else if (useSSL) {
//             logger.warn('KAFKA_CA_CERT_BASE64 not found for Consumer. Kafka client proceeding with basic SSL, which may cause certificate errors if broker uses untrusted CA.');
//         } else {
//             logger.info('Kafka Consumer configured without SSL (KAFKA_USE_SSL is false or not set to "true").');
//         }

//         // --- Conditional SASL Authentication Configuration ---
//         if (saslUsername && saslPassword) {
//             kafkaConfig.sasl = {
//                 mechanism: 'scram-sha-256', // Aiven typically uses 'scram-sha-256' or 'scram-sha-512'
//                 username: saslUsername,
//                 password: saslPassword,
//             };
//             logger.info('Kafka Consumer configured with SASL authentication.');
//         } else if (useSSL) { 
//             logger.warn('Kafka Consumer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing) despite SSL being enabled. This may cause connection failures.');
//         }

//         // >>> THIS IS THE CRUCIAL DEBUG LOG YOU NEED TO SEE IN RENDER LOGS <<<
//         logger.debug(`Final Kafka Consumer Config: ${JSON.stringify(kafkaConfig, null, 2)}`);

//         const kafka = new Kafka(kafkaConfig);

//         consumer = kafka.consumer({ groupId: config.kafka.consumerGroupId });

//         await consumer.connect();
//         await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });

//         await consumer.run({
//             eachMessage: async ({ topic, partition, message }) => {
//                 const event = JSON.parse(message.value.toString());
//                 logger.info(`Received event from Kafka: ${JSON.stringify(event)}`);

//                 try {
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

// src/kafka/consumer.js

// src/kafka/consumer.js
// const Kafka = require("node-rdkafka");
// const fs = require("fs");
// const config = require("../config"); // Import config

// let consumer = null;

// /**
//  * Initializes the Kafka consumer.
//  * @param {Function} messageHandler - Callback function to handle incoming messages.
//  * @returns {Promise<void>} A promise that resolves when the consumer is ready.
//  */
// const initializeConsumer = (messageHandler) => {
//     return new Promise((resolve, reject) => {
//         if (consumer) {
//             console.log("Kafka Consumer already initialized.");
//             return resolve();
//         }

//         // --- Read the CA Certificate ---
//         let caCert;
//         try {
//             caCert = fs.readFileSync(config.kafka.caCertPath, 'utf-8');
//             //console.log(`Successfully read CA certificate from: ${config.kafka.caCertPath}`);
//         } catch (error) {
//             console.error(`Error reading CA certificate from ${config.kafka.caCertPath}:`, error.message);
//             return reject(new Error(`Failed to initialize consumer: CA certificate read error - ${error.message}`));
//         }

//         consumer = new Kafka.KafkaConsumer(
//             {
//                 "metadata.broker.list": config.kafka.brokers.join(','),
//                 "group.id": config.kafka.consumerGroupId,
//                 "security.protocol": "sasl_ssl",
//                 "sasl.mechanism": config.kafka.saslMechanism,
//                 "sasl.username": config.kafka.username,
//                 "sasl.password": config.kafka.password,
//                 "ssl.ca.location": config.kafka.caCertPath, // Path to the CA certificate file
//                 "auto.offset.reset": "beginning", // Start consuming from the beginning if no offset is committed
//                 // Optional: set to false to manually commit offsets after processing
//                 // "enable.auto.commit": true,
//             },
//             {} // topic configuration (can be empty)
//         );

//         consumer.on("ready", () => {
//             console.log("Kafka Consumer is ready.");
//             // Subscribe here or in startConsumer, depending on desired flow
//             consumer.subscribe([config.kafka.topics.orderEvents]); // Example: subscribe to order events
//             consumer.consume(); // Start consuming messages
//             resolve();
//         });

//         consumer.on("data", (message) => {
//             console.log(`Received message from topic ${message.topic}: ${message.value.toString()}`);
//             if (messageHandler && typeof messageHandler === 'function') {
//                 messageHandler(message);
//             }
//             // If "enable.auto.commit": false, you would commit here:
//             // consumer.commitMessage(message);
//         });

//         consumer.on("event.log", function(log) {
//             // console.log("Consumer log:", log); // Uncomment for verbose librdkafka logs
//         });

//         consumer.on("event.error", (err) => {
//             console.error("Kafka Consumer error:", err);
//             // Consumer might try to reconnect; if not, you might need to re-initialize
//         });

//         consumer.on("disconnected", () => {
//             console.warn("Kafka Consumer disconnected.");
//             consumer = null;
//         });

//         consumer.connect();
//     });
// };

// /**
//  * Disconnects the Kafka consumer.
//  * @returns {Promise<void>} A promise that resolves when the consumer is disconnected.
//  */
// const disconnectConsumer = () => {
//     return new Promise((resolve) => {
//         if (consumer) {
//             console.log("Disconnecting Kafka Consumer...");
//             consumer.disconnect(() => {
//                 consumer = null;
//                 console.log("Kafka Consumer disconnected successfully.");
//                 resolve();
//             });
//         } else {
//             console.log("Kafka Consumer not active, no need to disconnect.");
//             resolve();
//         }
//     });
// };

// module.exports = {
//     initializeConsumer,
//     disconnectConsumer,
// };

// src/kafka/consumer.js
const Kafka = require("node-rdkafka");
const fs = require("fs");
const config = require("../config"); // Import config
const logger = require("../utils/logger"); // Use your logger

let consumer = null;
let isConsumerConnected = false;

/**
 * Initializes the Kafka consumer.
 * @param {Function} messageHandler - Callback function to handle incoming messages.
 * @returns {Promise<void>} A promise that resolves when the consumer is ready.
 */
const initializeConsumer = (messageHandler) => {
    return new Promise((resolve, reject) => {
        if (consumer && isConsumerConnected) {
            logger.info("Kafka Consumer already initialized and connected.");
            return resolve();
        }

        if (typeof messageHandler !== 'function') {
            logger.error("Consumer messageHandler must be a function.");
            return reject(new Error("Consumer messageHandler not provided or invalid."));
        }

        // --- Read the CA Certificate --- 
        let caCert;
        try {
            caCert = fs.readFileSync(config.kafka.caCertPath, 'utf-8');
            logger.info(`Successfully read CA certificate from: ${config.kafka.caCertPath}`);
        } catch (error) {
            logger.error(`Error reading CA certificate from ${config.kafka.caCertPath}:`, error.message);
            return reject(new Error(`Failed to initialize consumer: CA certificate read error - ${error.message}`));
        }
 
        consumer = new Kafka.KafkaConsumer(
            {
                "metadata.broker.list": config.kafka.brokers.join(','),
                "group.id": config.kafka.consumerGroupId,
                "security.protocol": "sasl_ssl",
                "sasl.mechanism": config.kafka.saslMechanism,
                "sasl.username": config.kafka.username,
                "sasl.password": config.kafka.password,
                "ssl.ca.location": config.kafka.caCertPath,
                "auto.offset.reset": "beginning",
                // "enable.auto.commit": false, // Set to false if you want to commit manually in messageHandler
            },
            {} // topic configuration (can be empty)
        );

        consumer.on("ready", () => {
            isConsumerConnected = true;
            logger.info("Kafka Consumer is ready.");
            consumer.subscribe([config.kafka.topics.orderEvents]);
            consumer.consume();
            resolve();
        });

        consumer.on("data", (message) => {
            // Pass the message to the provided handler function
            messageHandler(message);
            // If auto.commit.enable is false, commit after processing:
            // consumer.commitMessage(message);
        });

        consumer.on("event.log", function(log) {
            // logger.debug("Consumer log:", log); // Uncomment for verbose librdkafka logs
        });

        consumer.on("event.error", (err) => {
            isConsumerConnected = false;
            logger.error("Kafka Consumer error:", err);
            // Consumer might attempt to reconnect. For critical errors,
            // you might want to consider process.exit(1) or more advanced error recovery.
        });

        consumer.on("disconnected", () => {
            isConsumerConnected = false;
            logger.warn("Kafka Consumer disconnected.");
            consumer = null;
        });

        consumer.connect();
    });
};

/**
 * Disconnects the Kafka consumer.
 * @returns {Promise<void>} A promise that resolves when the consumer is disconnected.
 */
const disconnectConsumer = () => {
    return new Promise((resolve) => {
        if (consumer && isConsumerConnected) {
            logger.info("Disconnecting Kafka Consumer...");
            consumer.disconnect(() => {
                isConsumerConnected = false;
                consumer = null;
                logger.info("Kafka Consumer disconnected successfully.");
                resolve();
            });
        } else {
            logger.info("Kafka Consumer not active, no need to disconnect.");
            resolve();
        }
    });
};

module.exports = {
    initializeConsumer,
    disconnectConsumer,
};