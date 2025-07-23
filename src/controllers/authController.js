const jwt = require("jsonwebtoken");
const bcrypt = require("bcrypt");
const config = require("../config");
const db = require("../db");
const logger = require("../utils/logger");

const login = async (req, res) => {
  const { username, password } = req.body;
  try {
    const result = await db.query("SELECT * FROM users WHERE username = $1", [
      username,
    ]);
    const user = result.rows[0];

    if (!user) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const isMatch = await bcrypt.compare(password, user.password_hash);
    if (!isMatch) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const token = jwt.sign(
      { id: user.user_id, username: user.username },
      config.jwt.secret,
      { expiresIn: config.jwt.expiresIn }
    );
    logger.info(`User ${username} logged in successfully.`);
    res
      .status(200)
      .json({ token, user: { id: user.user_id, username: user.username } });
  } catch (error) {
    logger.error(`Login error for user ${username}: ${error.message}`, error);
    res.status(500).json({ message: "User does not exist" });
  } 
};

const register = async (req, res) => {
  const { username, password } = req.body;
  try {
    const hashedPassword = await bcrypt.hash(password, 10);
    const result = await db.query(
      "INSERT INTO users (username, password_hash) VALUES ($1, $2) RETURNING user_id, username",
      [username, hashedPassword]
    );
    logger.info(`User ${username} registered successfully.`);
    res.status(201).json(result.rows[0]);
  } catch (dbError) {
    logger.error("DB INSERT error in register():", dbError);
    throw dbError;
    // } catch (error) {
    //     if (error.code === '23505') {
    //         return res.status(409).json({ message: 'Username already exists' });
    //     }
    //     logger.error(`Registration error for user ${username}: ${error.message}`, error);
    //     res.status(500).json({ message: 'Server error during registration' });
  }
};

module.exports = {
  login, 
  register,
};
