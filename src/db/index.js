const { Pool } = require("pg");
const config = require("../config");
const logger = require("../utils/logger");

const db = new Pool({
  connectionString: config.db.connectionString,
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

db.on("connect", () => {
  logger.info(`Connected to PostgreSQL database: ${config.db.name}`);
});

db.on("error", (err) => {
  logger.error(`PostgreSQL database error: ${err.message}`, err);
});

module.exports = db;
