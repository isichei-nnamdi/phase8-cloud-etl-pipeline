-- =====================================================================
-- 03  ELT version of the pipeline: clean the data INSIDE BigQuery
--
-- Prerequisite: the raw CSV is loaded unchanged into
--   phase8_training.supply_chain_orders_raw   (see README.md, Part 7)
--
-- This query applies the same rules as transform() in
-- src/etl_to_bigquery.py and writes the result to a separate table:
--   phase8_training.supply_chain_orders_elt
-- Expected result: 152 rows, the same as the Python pipeline.
--
-- To make it run automatically, save it as a scheduled query
-- (needs a billing account; see README.md, Part 7).
-- =====================================================================

CREATE OR REPLACE TABLE `phase8_training.supply_chain_orders_elt` AS

WITH deduplicated AS (
  -- Rule 1: remove exact duplicate rows
  SELECT DISTINCT * FROM `phase8_training.supply_chain_orders_raw`
),

typed AS (
  -- Rule 2: convert types (SAFE_CAST returns NULL instead of failing)
  -- Rule 3: trim spaces and use Title Case for text fields
  SELECT
    CAST(order_id AS STRING)                         AS order_id,
    SAFE_CAST(order_date AS DATE)                    AS order_date,
    INITCAP(TRIM(CAST(product AS STRING)))           AS product,
    INITCAP(TRIM(CAST(region AS STRING)))            AS region,
    -- Rule 4: a missing supplier becomes "Unknown"
    COALESCE(INITCAP(NULLIF(TRIM(CAST(supplier AS STRING)), '')), 'Unknown') AS supplier,
    SAFE_CAST(quantity AS FLOAT64)                   AS quantity,
    SAFE_CAST(unit_cost_ngn AS FLOAT64)              AS unit_cost_ngn,
    SAFE_CAST(delivery_days AS FLOAT64)              AS delivery_days,
    INITCAP(TRIM(CAST(status AS STRING)))            AS status
  FROM deduplicated
)

SELECT
  order_id,
  order_date,
  product,
  region,
  supplier,
  quantity,
  unit_cost_ngn,
  -- Rule 5: a negative delivery time is impossible, so it becomes missing
  IF(delivery_days < 0, NULL, delivery_days)        AS delivery_days,
  status,
  -- Derived fields
  quantity * unit_cost_ngn                          AS order_value_ngn,
  FORMAT_DATE('%Y-%m', order_date)                  AS order_month
FROM typed
-- Rule 6: drop rows missing a required field, and impossible values
WHERE order_id IS NOT NULL
  AND order_date IS NOT NULL
  AND product IS NOT NULL
  AND quantity IS NOT NULL
  AND unit_cost_ngn IS NOT NULL
  AND quantity > 0
  AND unit_cost_ngn >= 0;

-- Check: should return 152 and 106828531.91, the same as the Python pipeline
SELECT COUNT(*) AS clean_rows, ROUND(SUM(order_value_ngn), 2) AS total_value_ngn
FROM `phase8_training.supply_chain_orders_elt`;
