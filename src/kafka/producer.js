// const { Kafka } = require("kafkajs");
// const config = require("../config");
// const logger = require("../utils/logger");
// const fs = require('fs');  
// const path = require('path');  

// let producer = null;

// const initializeProducer = async () => {
//   if (producer) {
//     logger.info("Kafka Producer already initialized.");
//     return producer;
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

//     // CA CERTIFICATE HANDLING ***
//     if (process.env.KAFKA_CA_CERT_BASE64) {
//         const caCertPath = path.join('/tmp', 'aiven-ca-producer.pem');  

//         if (!fs.existsSync(path.dirname(caCertPath))) {
//             fs.mkdirSync(path.dirname(caCertPath), { recursive: true });
//         }
//         fs.writeFileSync(caCertPath, Buffer.from(process.env.KAFKA_CA_CERT_BASE64, 'base64').toString('utf-8'));

//         kafkaConfig.ssl = {
//             ca: [fs.readFileSync(caCertPath, 'utf-8')], 
            
//         };
//         logger.info('Kafka Producer configured with SSL/TLS and custom CA certificate.');
//     } else {
//         logger.warn('KAFKA_CA_CERT_BASE64 not found for Producer. Kafka client proceeding with basic SSL, which may cause certificate errors.');
     
//         kafkaConfig.ssl = true;  
//     }

    
//     if (saslUsername && saslPassword) {
//       kafkaConfig.sasl = {
//         mechanism: "scram-sha-256",  
//         username: saslUsername,
//         password: saslPassword,
//       };
//       logger.info("Kafka Producer configured with SASL authentication.");
//     } else {
//       logger.warn("Kafka Producer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing).");
//     }
//     // *** END OF CA CERTIFICATE BLOCK ***

//     const kafka = new Kafka(kafkaConfig);

//     producer = kafka.producer();
//     await producer.connect();
//     logger.info("Kafka Producer connected!");
//     return producer;
//   } catch (error) {
//     logger.error(`Failed to connect Kafka Producer: ${error.message}`, error);
//     producer = null;  
//     throw error;
//   }
// };

// const sendKafkaMessage = async (topic, messages) => {
//   if (!producer) {
//     logger.error("Kafka Producer not initialized. Cannot send message.");
//     throw new Error("Kafka Producer is not connected.");
//   }
//   try {
//     await producer.send({
//       topic,
//       messages: Array.isArray(messages) ? messages : [messages],
//     });
//     logger.info(`Message sent to topic ${topic}: ${JSON.stringify(messages)}`);
//   } catch (error) {
//     logger.error(
//       `Error sending message to Kafka topic ${topic}: ${error.message}`,
//       error
//     );
//     throw error;
//   }
// };

// const disconnectProducer = async () => {
//   if (producer) {
//     try {
//       await producer.disconnect();
//       logger.info("Kafka Producer disconnected.");
//       producer = null;
//     } catch (error) {
//       logger.error(
//         `Error disconnecting Kafka Producer: ${error.message}`,
//         error
//       );
//     }
//   }
// };

// module.exports = {
//   initializeProducer,
//   sendKafkaMessage, // Export this for controllers to use
//   disconnectProducer,
// };


// src/kafka/producer.js
// const { Kafka } = require("kafkajs");
// const config = require("../config");
// const logger = require('../utils/logger');

// let producer = null; // Declare as mutable

// const initializeProducer = async () => {
//     if (producer) {
//         logger.info("Kafka Producer already initialized.");
//         return producer;
//     }

//     try {
//         const kafka = new Kafka({
//             clientId: config.kafka.clientId,
//             brokers: config.kafka.brokers, // Ensure this is an array from config.js
//         });

//         producer = kafka.producer();
//         await producer.connect();
//         logger.info("Kafka Producer connected!");
//         return producer;
//     } catch (error) {
//         logger.error(`Failed to connect Kafka Producer: ${error.message}`, error);
//         producer = null; // Reset on failure
//         throw error;
//     }
// };

// const sendKafkaMessage = async (topic, messages) => {
//     if (!producer) {
//         logger.error("Kafka Producer not initialized. Cannot send message.");
//         throw new Error("Kafka Producer is not connected.");
//     }
//     try {
//         await producer.send({
//             topic,
//             messages: Array.isArray(messages) ? messages : [messages],
//         });
//         logger.info(`Message sent to topic ${topic}: ${JSON.stringify(messages)}`);
//     } catch (error) {
//         logger.error(`Error sending message to Kafka topic ${topic}: ${error.message}`, error);
//         throw error;
//     }
// };

// const disconnectProducer = async () => {
//     if (producer) {
//         try {
//             await producer.disconnect();
//             logger.info("Kafka Producer disconnected.");
//             producer = null;
//         } catch (error) {
//             logger.error(`Error disconnecting Kafka Producer: ${error.message}`, error);
//         }
//     }
// };

// module.exports = {
//     initializeProducer,
//     sendKafkaMessage, // Export this for controllers to use
//     disconnectProducer,
// };


// const { Kafka } = require('kafkajs');
// const config = require('../config');
// const logger = require('../utils/logger');
// const fs = require('fs'); // <--- ADD THIS
// const path = require('path'); // <--- ADD THIS

// let producer = null; // Declare as mutable

// const initializeProducer = async () => {
//     if (producer) {
//         logger.info("Kafka Producer already initialized.");
//         return producer;
//     }

//     try {
//         // Retrieve brokers from config, which should come from process.env.KAFKA_BROKERS
//         const brokers = config.kafka.brokers;
//         const saslUsername = process.env.KAFKA_USERNAME;
//         const saslPassword = process.env.KAFKA_PASSWORD;
//         const useSSL = process.env.KAFKA_USE_SSL === 'true'; // Controlled by env var

//         const kafkaConfig = {
//             clientId: config.kafka.clientId,
//             brokers: brokers,
//             ssl: useSSL, // Conditional SSL based on KAFKA_USE_SSL
//         };

//         // --- PRODUCTION-SPECIFIC: SSL/TLS with CA Certificate ---
//         if (useSSL && process.env.KAFKA_CA_CERT_BASE64) {
//             const caCertPath = path.join('/tmp', 'aiven-ca-producer.pem'); // Unique name for producer
//             // Ensure /tmp exists (Render provides it)
//             fs.writeFileSync(caCertPath, Buffer.from(process.env.KAFKA_CA_CERT_BASE64, 'base64').toString('utf-8'));

//             kafkaConfig.ssl = {
//                 ca: [fs.readFileSync(caCertPath, 'utf-8')], // Provide the trusted CA certificate
//                 // Do NOT use rejectUnauthorized: false here for production!
//             };
//             logger.info('Kafka Producer configured with SSL/TLS and custom CA certificate.');
//         } else if (useSSL) {
//             logger.warn('KAFKA_CA_CERT_BASE64 not found for Producer. Kafka client proceeding with SSL, but may cause certificate errors.');
//             // This case might still lead to self-signed cert errors if CA is required and not provided.
//         } else {
//             logger.info('Kafka Producer configured without SSL (KAFKA_USE_SSL is false or not set to "true").');
//         }

//         // --- PRODUCTION-SPECIFIC: SASL Authentication ---
//         if (saslUsername && saslPassword) {
//             kafkaConfig.sasl = {
//                 mechanism: 'scram-sha-256', // Or 'scram-sha-512' - confirm with Aiven
//                 username: saslUsername,
//                 password: saslPassword,
//             };
//             logger.info('Kafka Producer configured with SASL authentication.');
//         } else if (useSSL) { // Only warn if SSL is enabled, as production usually requires SASL
//             logger.warn('Kafka Producer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing) despite SSL being enabled.');
//         }
//         // --- END PRODUCTION-SPECIFIC CONFIG ---

//         const kafka = new Kafka(kafkaConfig);

//         producer = kafka.producer();
//         await producer.connect();
//         logger.info("Kafka Producer connected!");
//         return producer;
//     } catch (error) {
//         logger.error(`Failed to connect Kafka Producer: ${error.message}`, error);
//         producer = null; // Reset on failure
//         throw error;
//     }
// };

// const sendKafkaMessage = async (topic, messages) => {
//     if (!producer) {
//         logger.error("Kafka Producer not initialized. Cannot send message.");
//         throw new Error("Kafka Producer is not connected.");
//     }
//     try {
//         await producer.send({
//             topic,
//             messages: Array.isArray(messages) ? messages : [messages],
//         });
//         logger.info(`Message sent to topic ${topic}: ${JSON.stringify(messages)}`);
//     } catch (error) {
//         logger.error(
//             `Error sending message to Kafka topic ${topic}: ${error.message}`,
//             error
//         );
//         throw error;
//     }
// };

// const disconnectProducer = async () => {
//     if (producer) {
//         try {
//             await producer.disconnect();
//             logger.info("Kafka Producer disconnected.");
//             producer = null;
//         } catch (error) {
//             logger.error(
//                 `Error disconnecting Kafka Producer: ${error.message}`,
//                 error
//             );
//         }
//     }
// };

// module.exports = {
//     initializeProducer,
//     sendKafkaMessage, // Export this for controllers to use
//     disconnectProducer,
// };

 
// src/kafka/producer.js
// const { Kafka } = require('kafkajs');
// const config = require('../config');
// const logger = require('../utils/logger');
// const fs = require('fs');
// const path = require('path');

// let producer = null;

// const initializeProducer = async () => {
//     if (producer) {
//         logger.info("Kafka Producer already initialized.");
//         return producer;
//     }

//     try {
//         const brokers = config.kafka.brokers; // Assuming config.kafka.brokers is correctly populated from KAFKA_BROKERS
//         const saslUsername = process.env.KAFKA_USERNAME;
//         const saslPassword = process.env.KAFKA_PASSWORD;
//         const useSSL = process.env.KAFKA_USE_SSL === 'true';

//         const kafkaConfig = {
//             clientId: config.kafka.clientId, // Note: clientId for producer
//             brokers: brokers,
//             ssl: useSSL, // Initialize ssl based on env var
//         }; 

//         // --- Conditional SSL/TLS Configuration with CA Certificate ---
//         if (useSSL && process.env.KAFKA_CA_CERT_BASE64) {
//             const caCertPath = path.join('/tmp', 'aiven-ca-producer.pem'); // Unique name for producer
            
//             // Write the base64-decoded CA certificate to a file
//             fs.writeFileSync(caCertPath, Buffer.from(process.env.KAFKA_CA_CERT_BASE64, 'base64').toString('utf-8'));

//             // Configure KafkaJS to use the custom CA certificate
//             kafkaConfig.ssl = {
//                 ca: [fs.readFileSync(caCertPath, 'utf-8')], 
//                 // Do NOT include rejectUnauthorized: false here.
//             };
//             logger.info('Kafka Producer configured with SSL/TLS and custom CA certificate.');
//         } else if (useSSL) {
//             logger.warn('KAFKA_CA_CERT_BASE64 not found for Producer. Kafka client proceeding with SSL, but may cause certificate errors.');
//         } else {
//             logger.info('Kafka Producer configured without SSL (KAFKA_USE_SSL is false or not set to "true").');
//         }

//         // --- Conditional SASL Authentication Configuration ---
//         if (saslUsername && saslPassword) {
//             kafkaConfig.sasl = {
//                 mechanism: 'scram-sha-256',
//                 username: saslUsername,
//                 password: saslPassword,
//             };
//             logger.info('Kafka Producer configured with SASL authentication.');
//         } else if (useSSL) { 
//             logger.warn('Kafka Producer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing) despite SSL being enabled.');
//         }

//         // >>> THIS IS THE CRUCIAL DEBUG LOG YOU NEED TO SEE IN RENDER LOGS <<<
//         logger.debug(`Final Kafka Producer Config: ${JSON.stringify(kafkaConfig, null, 2)}`);

//         const kafka = new Kafka(kafkaConfig);

//         producer = kafka.producer();
//         await producer.connect();
//         logger.info("Kafka Producer connected!");
//         return producer;
//     } catch (error) {
//         logger.error(`Failed to connect Kafka Producer: ${error.message}`, error);
//         producer = null;
//         throw error;
//     }
// };

// const sendKafkaMessage = async (topic, messages) => {
//     if (!producer) {
//         logger.error("Kafka Producer not initialized. Cannot send message.");
//         throw new Error("Kafka Producer is not connected.");
//     }
//     try {
//         await producer.send({
//             topic,
//             messages: Array.isArray(messages) ? messages : [messages],
//         });
//         logger.info(`Message sent to topic ${topic}: ${JSON.stringify(messages)}`);
//     } catch (error) {
//         logger.error(
//             `Error sending message to Kafka topic ${topic}: ${error.message}`,
//             error
//         );
//         throw error;
//     }
// };

// const disconnectProducer = async () => {
//     if (producer) {
//         try {
//             await producer.disconnect();
//             logger.info("Kafka Producer disconnected.");
//             producer = null;
//         } catch (error) {
//             logger.error(
//                 `Error disconnecting Kafka Producer: ${error.message}`,
//                 error
//             );
//         }
//     }
// };

// module.exports = {
//     initializeProducer,
//     sendKafkaMessage,
//     disconnectProducer,
// };

// src/kafka/producer.js
// const { Kafka } = require('kafkajs'); // Still need Kafka for its types and producer() method
// const config = require('../config');
// const logger = require('../utils/logger');
// const { createKafkaInstance } = require('./kafkaClientFactory'); // Import the factory

// let producer = null;

// const initializeProducer = async () => {
//     if (producer) {
//         logger.info("Kafka Producer already initialized.");
//         return producer;
//     }

//     try {
//         const kafka = createKafkaInstance({
//             clientId: config.kafka.clientId,
//             type: 'producer',
//         });

//         producer = kafka.producer();
//         await producer.connect();
//         logger.info("Kafka Producer connected!");
//         return producer;
//     } catch (error) {
//         logger.error(`Failed to connect Kafka Producer: ${error.message}`, error);
//         producer = null;
//         throw error;
//     }
// };

// const sendKafkaMessage = async (topic, messages) => {
//     if (!producer) {
//         logger.error("Kafka Producer not initialized. Cannot send message.");
//         throw new Error("Kafka Producer is not connected.");
//     }
//     try {
//         await producer.send({
//             topic,
//             messages: Array.isArray(messages) ? messages : [messages],
//         });
//         logger.info(`Message sent to topic ${topic}: ${JSON.stringify(messages)}`);
//     } catch (error) {
//         logger.error(
//             `Error sending message to Kafka topic ${topic}: ${error.message}`,
//             error
//         );
//         throw error;
//     }
// };

// const disconnectProducer = async () => {
//     if (producer) {
//         try {
//             await producer.disconnect();
//             logger.info("Kafka Producer disconnected.");
//             producer = null;
//         } catch (error) {
//             logger.error(
//                 `Error disconnecting Kafka Producer: ${error.message}`,
//                 error
//             );
//         }
//     }
// };

// module.exports = {
//     initializeProducer,
//     sendKafkaMessage,
//     disconnectProducer,
// };


// src/kafka/producer.js
const Kafka = require("node-rdkafka");
const fs = require("fs");
const config = require("../config"); // Import config

let producer = null; // Private producer instance
let isProducerConnected = false;

/**
 * Initializes the Kafka producer.
 * Must be called once before sending messages.
 * @returns {Promise<void>} A promise that resolves when the producer is ready.
 */
const initializeProducer = () => {
    return new Promise((resolve, reject) => {
        if (producer && isProducerConnected) {
            console.log("Kafka Producer already initialized and connected.");
            return resolve();
        }

        // --- Read the CA Certificate ---
        let caCert;
        try {
            caCert = fs.readFileSync(config.kafka.caCertPath, 'utf-8');
            //console.log(`Successfully read CA certificate from: ${config.kafka.caCertPath}`);
        } catch (error) {
            console.error(`Error reading CA certificate from ${config.kafka.caCertPath}:`, error.message);
            return reject(new Error(`Failed to initialize producer: CA certificate read error - ${error.message}`));
        }

        producer = new Kafka.Producer({
            "metadata.broker.list": config.kafka.brokers.join(','),
            "security.protocol": "sasl_ssl",
            "sasl.mechanism": config.kafka.saslMechanism,
            "sasl.username": config.kafka.username,
            "sasl.password": config.kafka.password,
            "ssl.ca.location": config.kafka.caCertPath, // Path to the CA certificate file
            "dr_cb": true // Delivery report callback
        });

        producer.on("ready", () => {
            isProducerConnected = true;
            console.log("Kafka Producer is ready.");
            resolve();
        });

        producer.on("delivery-report", (err, report) => {
            if (err) {
                console.error("Error sending message:", err);
                // Depending on your error handling, you might want to retry here
            } else {
                console.log(`Message delivered to topic ${report.topic} [${report.partition}] at offset ${report.offset}`);
            }
        });

        producer.on("event.log", function(log) {
            // console.log("Producer log:", log); // Uncomment for verbose librdkafka logs
        });

        producer.on("event.error", (err) => {
            isProducerConnected = false;
            console.error("Kafka Producer error:", err);
            // Don't reject the promise immediately on any error,
            // as producer might attempt to reconnect.
            // However, a critical error might warrant a manual disconnect/reconnect.
            // For initialization, we still reject if it errors before 'ready'.
            if (!isProducerConnected) { // If error occurs during initial connection
                reject(new Error(`Kafka Producer failed to connect: ${err.message}`));
            }
        });

        producer.on("disconnected", () => {
            isProducerConnected = false;
            console.warn("Kafka Producer disconnected.");
            producer = null; // Clear the instance
        });

        producer.connect();
    });
};

/**
 * Sends a message to a Kafka topic.
 * @param {string} topic - The Kafka topic to send the message to.
 * @param {object|string|Buffer} message - The message payload. Will be converted to Buffer.
 * @param {string} [key=null] - Optional message key.
 * @returns {Promise<void>} A promise that resolves when the message is queued.
 */
const sendKafkaMessage = (topic, message, key = null) => {
    return new Promise((resolve, reject) => {
        if (!producer || !isProducerConnected) {
            console.error("Kafka Producer is not initialized or not connected. Cannot send message.");
            return reject(new Error("Producer not ready."));
        }

        const messageBuffer = Buffer.isBuffer(message) ? message : Buffer.from(JSON.stringify(message));

        try {
            producer.produce(
                topic,
                null, // Partition (null for default)
                messageBuffer,
                key ? Buffer.from(key) : null,
                Date.now(),
                (err, offset) => { // This is the delivery report callback for this specific message
                    if (err) {
                        console.error(`Failed to produce message to topic ${topic}:`, err);
                        return reject(err);
                    }
                    console.log(`Message queued for topic ${topic} with offset ${offset}`);
                    resolve(offset);
                }
            );
        } catch (err) {
            console.error(`Failed to produce message synchronously to topic ${topic}:`, err);
            reject(err);
        }
    });
};

/**
 * Disconnects the Kafka producer.
 * @returns {Promise<void>} A promise that resolves when the producer is disconnected.
 */
const disconnectProducer = () => {
    return new Promise((resolve) => {
        if (producer && isProducerConnected) {
            console.log("Disconnecting Kafka Producer...");
            producer.disconnect(() => {
                isProducerConnected = false;
                producer = null;
                console.log("Kafka Producer disconnected successfully.");
                resolve();
            });
        } else {
            console.log("Kafka Producer not active, no need to disconnect.");
            resolve();
        }
    });
};

module.exports = {
    initializeProducer,
    sendKafkaMessage,
    disconnectProducer,
};