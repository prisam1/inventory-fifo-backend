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


const { Kafka } = require("kafkajs");
const config = require("../config");
const logger = require('../utils/logger');
const fs = require('fs'); // Required for file system operations
const path = require('path'); // Required for path manipulation

let producer = null; // Declare as mutable

const initializeProducer = async () => {
    if (producer) {
        logger.info("Kafka Producer already initialized.");
        return producer;
    }

    try {
        // Retrieve brokers from config, which should come from process.env.KAFKA_BROKERS
        const brokers = config.kafka.brokers;
        const saslUsername = process.env.KAFKA_USERNAME;
        const saslPassword = process.env.KAFKA_PASSWORD;
        // Determine if SSL should be used based on KAFKA_USE_SSL env variable
        const useSSL = process.env.KAFKA_USE_SSL === 'true';

        const kafkaConfig = {
            clientId: config.kafka.clientId,
            brokers: brokers, // Ensure this is an array from config.js
            ssl: useSSL, // Set SSL dynamically based on environment
        };

        // --- Conditional SSL/TLS Configuration with CA Certificate ---
        // This block is crucial for connecting to Aiven Kafka
        if (useSSL && process.env.KAFKA_CA_CERT_BASE64) {
            const caCertPath = path.join('/tmp', 'aiven-ca-producer.pem'); // Use /tmp for Render deployment
            
            // Ensure the /tmp directory exists. Render provides it, but good practice for local testing if /tmp isn't default.
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
            logger.info('Kafka Producer configured with SSL/TLS and custom CA certificate.');
        } else if (useSSL) {
            // This case means SSL is desired (useSSL is true) but no CA certificate is provided.
            // This will likely lead to "self-signed certificate" errors in production.
            logger.warn('KAFKA_CA_CERT_BASE64 not found for Producer. Kafka client proceeding with basic SSL, which may cause certificate errors if broker uses untrusted CA.');
        } else {
            // This case means KAFKA_USE_SSL is false, typically for local development.
            logger.info('Kafka Producer configured without SSL (KAFKA_USE_SSL is false or not set to "true").');
        }

        // --- Conditional SASL Authentication Configuration ---
        // This block is crucial for connecting to Aiven Kafka
        if (saslUsername && saslPassword) {
            kafkaConfig.sasl = {
                mechanism: 'scram-sha-256', // Aiven typically uses 'scram-sha-256' or 'scram-sha-512'
                username: saslUsername,
                password: saslPassword,
            };
            logger.info('Kafka Producer configured with SASL authentication.');
        } else if (useSSL) {
            // If SSL is enabled (i.e., production context), but SASL credentials are missing, warn.
            // Most managed Kafka services require SASL with SSL.
            logger.warn('Kafka Producer not configured with SASL authentication (KAFKA_USERNAME or KAFKA_PASSWORD missing) despite SSL being enabled. This may cause connection failures.');
        }
        // --- END OF DYNAMIC CONFIGURATION BLOCKS ---

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
        logger.error(`Error sending message to Kafka topic ${topic}: ${error.message}`, error);
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
            logger.error(`Error disconnecting Kafka Producer: ${error.message}`, error);
        }
    }
};

module.exports = {
    initializeProducer,
    sendKafkaMessage, // Export this for controllers to use
    disconnectProducer,
};