// src/config/index.js
require('dotenv').config(); // Load environment variables from .env file
const path = require('path');

const config = {
    // --- Server Configuration ---
    server: {
        port: parseInt(process.env.PORT || '10000', 10), // Use PORT from .env, default to 10000
    },

    // --- Authenticated URI for CORS ---
    authenticated: {
        uri: process.env.AUTHENTICATED_URI || 'http://localhost:3000', // Use AUTHENTICATED_URI for frontend URL
    },

    // --- Database Configuration ---
    db: {
        user: process.env.DB_USER || 'inventory_fifo_db_user',
        host: process.env.DB_HOST || 'localhost',
        name: process.env.DB_NAME || 'inventory_fifo_db', // Use DB_NAME consistently for database name
        password: process.env.DB_PASSWORD,
        port: parseInt(process.env.DB_PORT || '5432', 10),
    },

    // --- Kafka Configuration (for node-rdkafka) ---
    kafka: {
        // General Kafka client properties
        clientId: process.env.KAFKA_CLIENT_ID || 'inventory-app-node-rdkafka', // Client ID for broker identification
        brokers: process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : [],
        
        // SASL authentication
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        saslMechanism: process.env.KAFKA_SASL_MECHANISM || 'SCRAM-SHA-256',
        
        // SSL/TLS
        caCertPath: path.join(process.cwd(), process.env.KAFKA_CA_CERT_PATH || 'ca.pem'), // Path to CA certificate

        // Topics used by the application
        topics: {
            inventoryUpdates: process.env.KAFKA_TOPIC_INVENTORY_UPDATES || 'inventory-updates', // Producer topic
            orderEvents: process.env.KAFKA_TOPIC_ORDER_EVENTS || 'order-events', // Consumer topic
            // You can add more topics here as needed, e.g., KAFKA_TOPIC_SALES_REPORTS
        },
        
        // Consumer specific properties
        consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID || 'my-node-consumer-group',
        consumerClientId: process.env.KAFKA_CONSUMER_CLIENT_ID || 'inventory-consumer-client', // Specific client ID for consumer if needed
    },

    // --- JWT Configuration ---
    jwt: {
        secret: process.env.JWT_SECRET,
        expiresIn: process.env.JWT_EXPIRES_IN || '1h', // Default to 1 hour if not specified
    },

    // --- Logger Configuration ---
    logger: {
        level: process.env.LOG_LEVEL || 'info', // Default log level
    }
};

 
module.exports = config;