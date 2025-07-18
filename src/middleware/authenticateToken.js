// src/middleware/authenticateToken.js
const jwt = require('jsonwebtoken');
const config = require('../config');
const logger = require('../utils/logger');

const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (token == null) {
        logger.warn('Authentication attempt: No token provided');
        return res.status(401).json({ message: 'Authentication token required' });
    }

    jwt.verify(token, config.jwt.secret, (err, user) => {
        if (err) {
            logger.warn(`Authentication attempt: Invalid token - ${err.message}`);
            return res.status(403).json({ message: 'Invalid or expired token' });
        }
        req.user = user; // Attach user payload to request
        next();
    });
};

module.exports = authenticateToken;