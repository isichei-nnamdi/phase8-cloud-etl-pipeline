-- =====================================================================
-- 01  Verify the clean table after running src/etl_to_bigquery.py
-- Run each query in the BigQuery console (one at a time, or all together).
-- If you changed BQ_DATASET or BQ_TABLE, change the table name below.
-- =====================================================================

-- 1. Row count, total value and date range
--    Expected: 152 rows | 106,828,531.91 NGN | 2026-08-01 to 2027-01-07
SELECT
  COUNT(*)                          AS clean_rows,
  ROUND(SUM(order_value_ngn), 2)    AS total_value_ngn,
  MIN(order_date)                   AS first_order,
  MAX(order_date)                   AS last_order
FROM `phase8_training.supply_chain_orders`;

-- 2. No duplicate order IDs should remain
--    Expected: no rows returned
SELECT order_id, COUNT(*) AS copies
FROM `phase8_training.supply_chain_orders`
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 3. Required fields are complete and the cleaning rules were applied
--    Expected: every column = 0, except missing_delivery_days = 3
SELECT
  COUNTIF(quantity IS NULL)          AS missing_quantity,
  COUNTIF(quantity <= 0)             AS non_positive_quantity,
  COUNTIF(unit_cost_ngn < 0)         AS negative_cost,
  COUNTIF(supplier IS NULL)          AS missing_supplier,
  COUNTIF(delivery_days < 0)         AS negative_delivery_days,
  COUNTIF(delivery_days IS NULL)     AS missing_delivery_days
FROM `phase8_training.supply_chain_orders`;

-- 4. Orders by status
--    Expected: Delivered 107 | Delayed 24 | Pending 21
SELECT status, COUNT(*) AS orders
FROM `phase8_training.supply_chain_orders`
GROUP BY status
ORDER BY orders DESC;

-- 5. Suppliers that were blank in the raw file are now "Unknown"
--    Expected: Unknown = 5
SELECT supplier, COUNT(*) AS orders
FROM `phase8_training.supply_chain_orders`
GROUP BY supplier
ORDER BY orders DESC;
