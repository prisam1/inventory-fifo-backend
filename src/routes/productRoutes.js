// src/routes/productRoutes.js
const express = require("express");
const router = express.Router();
const {
  getProducts,
  getProductStockOverview,
  createProduct,
} = require("../controllers/productController");
const authenticateToken = require("../middleware/authenticateToken");

router.post("/", authenticateToken, createProduct);
router.get("/", authenticateToken, getProducts);
router.get("/stock-overview", authenticateToken, getProductStockOverview);

module.exports = router;
