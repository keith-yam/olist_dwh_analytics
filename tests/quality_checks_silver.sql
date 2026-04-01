/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.products'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select *
from (
    select *, 
    count(*) over(partition by product_id) as cnt
    from silver.products
) t
where cnt > 1 or product_id is null

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    product_category_name 
FROM silver.products
WHERE product_category_name != TRIM(product_category_name);

-- Data Standardization & Consistency
SELECT DISTINCT 
    product_category_name
FROM silver.products;

-- ====================================================================
-- Checking 'silver.orders'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select * from 
(
    select order_id,
    count(*) over (partition by order_id) as cnt 
from silver.orders
) t
where cnt > 1 or order_id is null


-- Check for Invalid Date Orders (approved date > purchase date)
-- Expectation: No Results
select 
    * 
from silver.orders
where purchase_timestamp > order_approved_at;

-- ====================================================================
-- Checking 'silver.order_items'
-- ====================================================================
-- Check for null or negative price
-- Expectation: No Results
select 
    price 
from silver.order_items
where price < 0 OR price IS NULL;


-- Check for duplicate order 
-- Expectation: No Results
select * from (
  select *, row_number() over (partition by order_id order by order_id) as row_num
  from silver.order_items 
) t
where row_num > 1;


-- ====================================================================
-- Checking 'silver.customers'
-- ====================================================================
-- Check for duplicate in id
-- Expectation: No Results
select * from
( select id, count(id) as count from silver.customers
group by id ) t 
where count > 2


-- Check for city name
-- Expectation: No duplicate or misspelling 
select 
    distinct city
from silver.customers
order by city

-- ====================================================================
-- Checking 'silver.geolocation'
-- ====================================================================
-- Check for city name
-- Expectation: No duplicate or misspelling 
select 
    distinct city
from silver.geolocation
order by city

-- for zip code prefix length more or less than 6 
-- expection: no result
select * 
from  silver.geolocation
where len(zip_code_prefix) <> 5


-- ====================================================================
-- Checking 'silver.order_payments'
-- ====================================================================
-- Check for duplicate or null 
-- Expectation: No duplicate or null 
select distinct payment_type from silver.order_payments	


-- ====================================================================
-- Checking 'silver.order_reviews'
-- ====================================================================
-- Check for duplicate in review
-- Expectation: No result
select * from (
  select *, row_number() over (partition by order_id order by answer_timestamp desc) as row_num
  from silver.order_reviews 
) t
where row_num > 1;