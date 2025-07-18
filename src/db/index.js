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