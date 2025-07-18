const logger = require("../utils/logger");
const db = require("../db");

const processInventoryEvent = async (event) => {
  const { eventId, product_id, event_type, quantity, unit_price, timestamp } =
    event;

  // Start a client for the transaction
  // Get a client from the pool
  const client = await db.connect();
  try {
    // Start a transaction
    await client.query("BEGIN");

    if (event_type === "purchase") {
      logger.debug(
        `[InventoryService] Attempting to insert purchase: ${JSON.stringify(
          event
        )}`
      );
      await client.query(
        // Use client.query for transaction
        `INSERT INTO inventory_batches (event_id, product_id, quantity, remaining_quantity, unit_price, purchase_timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6)`,
        [eventId, product_id, quantity, quantity, unit_price, timestamp]
      );
      logger.info(
        `[InventoryService] Recorded new purchase batch for product ${product_id}, qty ${quantity}, price ${unit_price}`
      );
    } else if (event_type === "sale") {
      logger.debug(
        `[InventoryService] Attempting to process sale: ${JSON.stringify(
          event
        )}`
      );
      let remainingSaleQuantity = quantity;
      let totalCostOfGoodsSold = 0;

      // Fetch available inventory batches (FIFO)
      const batchesResult = await client.query(
        // Use client.query for transaction
        `SELECT batch_id, remaining_quantity, unit_price
                 FROM inventory_batches
                 WHERE product_id = $1 AND remaining_quantity > 0
                 ORDER BY purchase_timestamp ASC, batch_id ASC`,
        [product_id]
      );

      if (batchesResult.rows.length === 0) {
        logger.warn(
          `[InventoryService] Sale of ${quantity} for product ${product_id} failed: No inventory found.`
        );
        await client.query("ROLLBACK"); // Rollback if no inventory
        return; // Exit early
      }

      for (const batch of batchesResult.rows) {
        if (remainingSaleQuantity <= 0) break;

        const quantityFromBatch = Math.min(
          remainingSaleQuantity,
          batch.remaining_quantity
        );
        const costForThisBatch =
          parseFloat(batch.unit_price) * quantityFromBatch;  
        totalCostOfGoodsSold += costForThisBatch;

        await client.query(
          // Use client.query for transaction
          `UPDATE inventory_batches
                     SET remaining_quantity = remaining_quantity - $1
                     WHERE batch_id = $2`,
          [quantityFromBatch, batch.batch_id]
        );
        logger.debug(
          `[InventoryService] Updated batch ${batch.batch_id} for sale, qty ${quantityFromBatch}`
        );
        remainingSaleQuantity -= quantityFromBatch;
      }

      if (remainingSaleQuantity > 0) {
        logger.warn(
          `[InventoryService] Sale of ${quantity} for product ${product_id} partially fulfilled. Remaining: ${remainingSaleQuantity}`
        );
      }

      // Record the sale
      await client.query(
        // Use client.query for transaction
        `INSERT INTO sales (event_id, product_id, quantity_sold, cost_of_goods_sold, sale_timestamp)
                 VALUES ($1, $2, $3, $4, $5)`,
        [eventId, product_id, quantity, totalCostOfGoodsSold, timestamp]
      );
      logger.info(
        `[InventoryService] Recorded sale for product ${product_id}, qty ${quantity}, COGS ${totalCostOfGoodsSold.toFixed(
          2
        )}`
      );
    } else {
      logger.warn(
        `[InventoryService] Unknown event type received: ${event_type} for event ID ${eventId}`
      );
    }

    await client.query("COMMIT"); // Commit the transaction
    logger.info(
      `[InventoryService] Transaction committed for event ${eventId}`
    );
  } catch (error) {
    await client.query("ROLLBACK"); // Rollback on any error
    logger.error(
      `[InventoryService] Failed to process event ${eventId} for product ${product_id} (ROLLBACK performed): ${error.message}`,
      error
    );
    throw error; // Re-throw to let the consumer's catch block handle logging/retries if needed
  } finally {
    client.release(); // Release the client back to the pool
  }
};

const getInventoryTransactions = async () => {
  try {
    const query = `
            SELECT
                'purchase' AS event_type,
                ib.product_id,
                p.product_name,
                ib.quantity AS quantity,
                ib.unit_price AS cost_or_price,
                ib.purchase_timestamp AS timestamp
            FROM
                inventory_batches ib
            JOIN
                products p ON ib.product_id = p.product_id
            WHERE
                ib.quantity > 0
            UNION ALL
            SELECT
                'sale' AS event_type,
                s.product_id,
                p.product_name,
                s.quantity_sold AS quantity,
                s.cost_of_goods_sold AS cost_or_price,
                s.sale_timestamp AS timestamp
            FROM
                sales s
            JOIN
                products p ON s.product_id = p.product_id
            ORDER BY
                timestamp DESC;
        `;

    const { rows } = await db.query(query); // use db.query for simple reads

    const processedRows = rows.map((row) => ({
      ...row,
      cost_or_price:
        typeof row.cost_or_price === "string"
          ? parseFloat(row.cost_or_price)
          : row.cost_or_price === null || row.cost_or_price === undefined
          ? 0
          : row.cost_or_price,  
    }));

    return processedRows;
  } catch (error) {
    logger.error(
      `[InventoryService] Error fetching inventory transactions: ${error.message}`,
      error
    );
    throw error;
  }
};

module.exports = {
  processInventoryEvent,
  getInventoryTransactions,
};
