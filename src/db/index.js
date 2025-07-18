const { Pool } = require('pg');
const config = require('../config');
const logger = require('../utils/logger');

const db = new Pool({
    user: config.db.user,
    host: config.db.host,
    database: config.db.name,  
    password: config.db.password,
    port: config.db.port,
});

db.on('connect', () => {
    logger.info(`Connected to PostgreSQL database: ${config.db.name}`);
});

db.on('error', (err) => {
    logger.error(`PostgreSQL database error: ${err.message}`, err);
});

module.exports = db;

// src/db.js
// const { Pool } = require('pg');
// const config = require('../config'); // Assuming config is imported
// const fs = require('fs');
// const path = require('path');
// const logger = require('../utils/logger'); // Import your logger

// // Define the path to your PostgreSQL CA certificate
// // Adjust this path if your pg_ca.pem is not in the project root
// const PG_CA_CERT_PATH = path.resolve(__dirname, '../../ca.pem'); // Assumes pg_ca.pem is in the project root

// let pgSslConfig = false; // Default to no SSL

// try {
//     const pgCaCert = fs.readFileSync(PG_CA_CERT_PATH).toString();
//     pgSslConfig = {
//         rejectUnauthorized: true, // Always true for production. Ensures the server certificate is valid.
//         ca: pgCaCert,
//         // If Aiven provided client certificates and keys, you'd add them here:
//         // key: fs.readFileSync(path.resolve(__dirname, '../../pg_client_key.pem')).toString(),
//         // cert: fs.readFileSync(path.resolve(__dirname, '../../pg_client_cert.pem')).toString(),
//     };
//     logger.info(`Successfully loaded PostgreSQL CA certificate from: ${PG_CA_CERT_PATH}`);
// } catch (error) {
//     logger.error(`Could not load PostgreSQL CA certificate from ${PG_CA_CERT_PATH}. SSL connection might fail. Error: ${error.message}`);
//     // If SSL is mandatory, you might want to exit here:
//     // process.exit(1);
// }

// const pool = new Pool({
//     user: config.db.user,
//     host: config.db.host,
//     database: config.db.database,
//     password: config.db.password,
//     port: config.db.port,
//     ssl: pgSslConfig // Use the configured SSL settings
//     // If you're on Aiven, sslmode=require usually corresponds to rejectUnauthorized: true and providing CA.
// });

// pool.on('error', (err) => {
//     logger.error('Unexpected error on idle PostgreSQL client:', err);
//     // Do not exit process unless it's an unrecoverable error.
//     // pg-pool handles connection re-establishment usually.
// });

// module.exports = {
//     query: (text, params) => pool.query(text, params),
//     end: () => pool.end(), // For graceful shutdown
//     getPool: () => pool // Optionally expose the pool directly for advanced use
// };