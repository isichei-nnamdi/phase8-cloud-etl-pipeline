-- =====================================================================
-- 04  Recover a table you overwrote by mistake (BigQuery time travel)
--
-- WRITE_TRUNCATE replaces a table's contents. BigQuery keeps earlier
-- versions for a limited window (up to 7 days by default), so you can
-- copy an old version into a NEW table. Change the INTERVAL to a time
-- BEFORE the run that overwrote the table.
-- =====================================================================

-- 1. Look at the table as it was two hours ago
SELECT COUNT(*) AS rows_two_hours_ago
FROM `phase8_training.supply_chain_orders`
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR);

-- 2. Save that version as a new table
CREATE TABLE `phase8_training.supply_chain_orders_recovered` AS
SELECT *
FROM `phase8_training.supply_chain_orders`
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR);
