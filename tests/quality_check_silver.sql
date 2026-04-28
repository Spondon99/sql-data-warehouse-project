-- Check for Nulls or Duplicates in the Primary Key

SELECT
account,
COUNT(*)
FROM bronze.accounts
GROUP BY account
HAVING COUNT(*) > 1 OR account IS NULL;


SELECT
prod,
COUNT(*)
FROM bronze.products
GROUP BY prod
HAVING COUNT(*) > 1 OR prod IS NULL;


SELECT
opportunity_id,
COUNT(*)
FROM bronze.sales_pipeline
GROUP BY opportunity_id
HAVING COUNT(*) > 1 OR opportunity_id IS NULL;


SELECT
sales_agent,
COUNT(*)
FROM bronze.sales_teams
GROUP BY sales_agent
HAVING COUNT(*) > 1 OR sales_agent IS NULL;
