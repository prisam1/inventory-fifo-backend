const db = require('../db');
const logger = require('../utils/logger');

const getProducts = async (req, res) => {
    try {
        const result = await db.query('SELECT product_id, product_name, created_at FROM products ORDER BY created_at DESC');
        res.json(result.rows);
    } catch (err) {
        logger.error(`Error fetching products: ${err.message}`, err);
        res.status(500).json({ message: 'Error fetching products' });
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
        res.status(500).json({ message: 'Error fetching product stock overview' });
    }
};

const createProduct = async (req, res) => {
    const { product_name } = req.body;
    if (!product_name) {
        return res.status(400).json({ message: 'Product name is required' });
    }
    
    try {
        // We no longer provide product_id; the database will generate it using the DEFAULT function
        const result = await db.query(
            'INSERT INTO products (product_name) VALUES ($1) RETURNING *', 
            [product_name]
        );
        logger.info(`Product created: ${product_name} (${result.rows[0].product_id})`); // Use the returned ID
        res.status(201).json(result.rows[0]);
    } catch (err) {
        logger.error(`Error creating product: ${err.message}`, err);
        // Check for unique violation on product_name if it's set unique
        if (err.code === '23505' && err.constraint === 'products_product_name_key') { // Assuming unique constraint name
            return res.status(409).json({ message: 'Product name already exists.' });
        }
        res.status(500).json({ message: 'Error creating product' });
    }
};
 
module.exports = {
    getProducts,
    getProductStockOverview,
    createProduct,
};