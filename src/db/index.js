const { Pool } = require('pg');
const config = require('../config');
const logger = require('../utils/logger');

const db = new Pool({
    user: config.db.user,
    host: config.db.host,
    database: config.db.name,
    password: config.db.password,
    port: config.db.port,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    ssl: {
        rejectUnauthorized: false,
      },
});

db.on('connect', () => {
    logger.info(`Connected to PostgreSQL database: ${config.db.name}`);
});

db.on('error', (err) => {
    logger.error(`PostgreSQL database error: ${err.message}`, err);
});

module.exports = db; 

// src/db.js
// const { Pool } = require("pg");
// const config = require("../config");
// const fs = require("fs");
// const path = require("path");
// const logger = require("../utils/logger");

// // Determine the path to your PostgreSQL CA certificate
// // Option 1 (Recommended): Place pg_ca.pem in project root.
// // If src/db.js is at D:\Projects\inventory-fifo-system\src\db.js
// // and pg_ca.pem is at D:\Projects\inventory-fifo-system\pg_ca.pem
// // then path from db.js to pg_ca.pem is two levels up: ../../pg_ca.pem
// const PG_CA_CERT_PATH = path.resolve(__dirname, "../ca.pem");

// // Option 2: If you put pg_ca.pem in a specific 'certs' folder inside 'src'
// // e.g., D:\Projects\inventory-fifo-system\src\certs\pg_ca.pem
// // const PG_CA_CERT_PATH = path.resolve(__dirname, '../certs/pg_ca.pem');

// // Option 3: If you pass the path via config/environment variable (less common for certs)
// // const PG_CA_CERT_PATH = config.db.pgCaCertPath; // Only if you explicitly added it to config.db

// let pgSslConfig = false; // Default to no SSL

// try {
//   const pgCaCert = fs.readFileSync(PG_CA_CERT_PATH).toString();
//   pgSslConfig = {
//     rejectUnauthorized: true, // CRITICAL: This must be true for Aiven. It verifies the server's certificate.
//     ca: pgCaCert,
//     // If your Aiven service requires client certificates (less common for basic connections),
//     // you'd add 'key' and 'cert' properties here as well.
//     // key: fs.readFileSync(path.resolve(__dirname, '../../pg_client_key.pem')).toString(),
//     // cert: fs.readFileSync(path.resolve(__dirname, '../../pg_client_cert.pem')).toString(),
//   };
//   logger.info(
//     `Successfully loaded PostgreSQL CA certificate from: ${PG_CA_CERT_PATH}`
//   );
// } catch (error) {
//   // This error means the file isn't found or readable.
//   // It's a critical error for Aiven connections.
//   logger.error(
//     `Could not load PostgreSQL CA certificate from ${PG_CA_CERT_PATH}. SSL connection will likely fail. Error: ${error.message}`
//   );
//   // Consider crashing the app if this is vital, to prevent silent failures.
//   // process.exit(1);
// }

// const pool = new Pool({
//   user: config.db.user,
//   host: config.db.host,
//   database: config.db.database,
//   password: config.db.password,
//   port: config.db.port,
//   ssl: pgSslConfig, // This is where the configured SSL object is passed
// });

// pool.on("error", (err) => {
//   logger.error("Unexpected error on idle PostgreSQL client:", err);
//   // This handler catches errors that occur during query execution or idle connections.
//   // It doesn't necessarily mean the app should exit.
// });

// module.exports = {
//   query: (text, params) => pool.query(text, params),
//   end: () => pool.end(), // For graceful shutdown
//   getPool: () => pool, // Optionally expose the pool directly for advanced use
// };

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
