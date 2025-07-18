const { createLogger, format, transports } = require('winston');
const { combine, timestamp, printf, colorize } = format;

const customFormat = printf(({ level, message, timestamp, stack }) => {
    // Render logs already include a timestamp, so we'll simplify for console output
    return `${level}: ${message} ${stack ? '\n' + stack : ''}`;
});

// Determine the log level based on environment variable
const logLevel = process.env.LOG_LEVEL || 'info'; // Default to 'info' if not set

const logger = createLogger({
    level: logLevel, // Use the dynamically set log level
    format: combine(
        colorize(), // Add color for local readability
        timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }), // Include timestamp
        customFormat
    ),
    transports: [
        new transports.Console(),
    ],
    exceptionHandlers: [
        new transports.Console(),
    ],
    rejectionHandlers: [
        new transports.Console(),
    ],
});

module.exports = logger;