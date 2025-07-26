const db = require("../db");
const logger = require("../utils/logger");

const getProducts = async (req, res) => {
  try {
    const result = await db.query(
      "SELECT product_id, product_name, created_at FROM products ORDER BY created_at DESC"
    );
    res.json(result.rows);
  } catch (err) {
    logger.error(`Error fetching products: ${err.message}`, err);
    res.status(500).json({ message: "Error fetching products" });
  }
}; 

const getProductStockOverview = async (req, res) => {
  try {
    const query = `
            SELECT
                p.product_id,
                p.product_name,
                COALESCE(SUM(ib.remaining_quantity), 0) AS current_quantity,
                COALESCE(SUM(ib.remaining_quantity * ib.unit_price), 0) AS total_inventory_cost,
                CASE
                    WHEN COALESCE(SUM(ib.remaining_quantity), 0) > 0 THEN
                        COALESCE(SUM(ib.remaining_quantity * ib.unit_price), 0) / SUM(ib.remaining_quantity)
                    ELSE 0
                END AS average_cost_per_unit
            FROM
                products p
            LEFT JOIN
                inventory_batches ib ON p.product_id = ib.product_id AND ib.remaining_quantity > 0
            GROUP BY
                p.product_id, p.product_name
            ORDER BY
                p.product_name;
        `;
    const { rows } = await db.query(query);
    res.json(rows);
  } catch (err) {
    logger.error(`Error fetching product stock overview: ${err.message}`, err);
    res.status(500).json({ message: "Error fetching product stock overview" });
  }
};

const createProduct = async (req, res) => {
  const { product_name } = req.body;
  if (!product_name) {
    return res.status(400).json({ message: "Product name is required" });
  }

  try {
    const { rows } = await db.query(
      `SELECT product_id FROM products 
             WHERE product_id LIKE 'PRD%' 
             ORDER BY product_id DESC LIMIT 1`
    );

    let newId;
    if (rows.length > 0) {
      // Extract number and increment
      const lastIdNum = parseInt(rows[0].product_id.replace("PRD", ""), 10);
      newId = `PRD${String(lastIdNum + 1).padStart(3, "0")}`;
    } else {
      newId = "PRD001"; // First product
    }

    const result = await db.query(
      "INSERT INTO products (product_id, product_name) VALUES ($1, $2) RETURNING *",
      [newId, product_name]
    );
    logger.info(
      `Product created: ${product_name} (${result.rows[0].product_id})`
    ); // Use the returned ID
    res.status(201).json(result.rows[0]);
  } catch (err) {
    logger.error(`Error creating product: ${err.message}`, err);
    // Check for unique violation on product_name if it's set unique
    if (
      err.code === "23505" &&
      err.constraint === "products_product_name_key"
    ) {
      // Assuming unique constraint name
      return res.status(409).json({ message: "Product name already exists." });
    }
    res.status(500).json({ message: "Error creating product" });
  }
};

module.exports = { 
  getProducts,
  getProductStockOverview,
  createProduct,
};
