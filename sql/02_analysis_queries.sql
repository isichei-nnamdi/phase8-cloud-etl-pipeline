-- =====================================================================
-- 02  Business analysis on the clean table
-- A starting point for the "SQL queries" deliverable of Portfolio
-- Project 9. Add at least two queries of your own.
-- =====================================================================

-- Q1. Order value by region (feeds the dashboard bar chart)
SELECT region, ROUND(SUM(order_value_ngn), 2) AS order_value_ngn, COUNT(*) AS orders
FROM `phase8_training.supply_chain_orders`
GROUP BY region
ORDER BY order_value_ngn DESC;

-- Q2. Top five products by order value (feeds the dashboard table)
SELECT product, ROUND(SUM(order_value_ngn), 2) AS order_value_ngn
FROM `phase8_training.supply_chain_orders`
GROUP BY product
ORDER BY order_value_ngn DESC
LIMIT 5;

-- Q3. Monthly trend in orders and value
SELECT order_month, COUNT(*) AS orders, ROUND(SUM(order_value_ngn), 2) AS order_value_ngn
FROM `phase8_training.supply_chain_orders`
GROUP BY order_month
ORDER BY order_month;

-- Q4. Average delivery time by region (AVG ignores missing values)
SELECT region, ROUND(AVG(delivery_days), 2) AS avg_delivery_days
FROM `phase8_training.supply_chain_orders`
GROUP BY region
ORDER BY avg_delivery_days DESC;

-- Q5. Which suppliers have the highest share of delayed orders?
SELECT
  supplier,
  COUNT(*) AS orders,
  COUNTIF(status = 'Delayed') AS delayed_orders,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(status = 'Delayed'), COUNT(*)), 1) AS delayed_pct
FROM `phase8_training.supply_chain_orders`
GROUP BY supplier
ORDER BY delayed_pct DESC;

-- Q6. Each supplier's share of total order value
SELECT
  supplier,
  ROUND(SUM(order_value_ngn), 2) AS order_value_ngn,
  ROUND(100 * SUM(order_value_ngn) / SUM(SUM(order_value_ngn)) OVER (), 1) AS share_pct
FROM `phase8_training.supply_chain_orders`
GROUP BY supplier
ORDER BY order_value_ngn DESC;

-- Q7. Average order size and unit cost by product
SELECT
  product,
  ROUND(AVG(quantity), 1)       AS avg_quantity,
  ROUND(AVG(unit_cost_ngn), 2)  AS avg_unit_cost_ngn
FROM `phase8_training.supply_chain_orders`
GROUP BY product
ORDER BY avg_unit_cost_ngn DESC;

-- Q8. Region x status matrix (where are the delays and pending orders?)
SELECT
  region,
  COUNTIF(status = 'Delivered') AS delivered,
  COUNTIF(status = 'Delayed')   AS delayed,
  COUNTIF(status = 'Pending')   AS pending
FROM `phase8_training.supply_chain_orders`
GROUP BY region
ORDER BY region;
