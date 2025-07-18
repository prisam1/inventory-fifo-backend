// src/utils/logger.js
// A simple logger for demonstration. For production, consider Winston or Pino.
const LOG_LEVEL = process.env.LOG_LEVEL || 'info'; // 'info', 'warn', 'error', 'debug'

const levels = {
    debug: 0,
    info: 1,
    warn: 2,
    error: 3
};

const logger = {
    debug: (...args) => {
        if (levels[LOG_LEVEL] <= levels.debug) {
            console.log('[DEBUG]', ...args);
        }
    },
    info: (...args) => {
        if (levels[LOG_LEVEL] <= levels.info) {
            console.log('[INFO]', ...args);
        }
    },
    warn: (...args) => {
        if (levels[LOG_LEVEL] <= levels.warn) {
            console.warn('[WARN]', ...args);
        }
    },
    error: (...args) => {
        if (levels[LOG_LEVEL] <= levels.error) {
            console.error('[ERROR]', ...args);
        }
    }
};

module.exports = logger;