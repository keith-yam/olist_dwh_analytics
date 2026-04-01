/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
-- =============================================================================
-- Create dimension : gold.dim_order_reviews 
-- =============================================================================

if object_id('gold.dim_order_reviews','v') is not null
    drop view gold.dim_order_reviews
go

create view gold.dim_order_reviews as 
select 
    row_number() over (order by score desc) as reviews_key
    , review_id 
    , order_id 
    , score 
    , answer_timestamp
from silver.order_reviews
go

-- =============================================================================
-- Create dimension: gold.dim_order_payments 
-- =============================================================================
if object_id('gold.dim_order_payments','v') is not null
    drop view gold.dim_order_payments
go

create view gold.dim_order_payments as 
select 
    row_number() over (order by payment_type) as payments_key
    , order_id as order_id
    , payment_sequential 
    , payment_type 
    , payment_installments as installments
    , payment_value 
from silver.order_payments
go



-- =============================================================================
-- Create dimension: gold.dim_sellers 
-- =============================================================================
if object_id('gold.dim_sellers','v') is not null
    drop view gold.dim_sellers
go

create view gold.dim_sellers as 
select 
    row_number() over (order by zip_code_prefix) as sellers_key
    , seller_id
    , zip_code_prefix
    , city
    , state
    , latitude
    , longitude
from
(
select     
    seller_id			            as seller_id 			
    , seller_zip_code_prefix		as zip_code_prefix
    , seller_city					as city
    , seller_state					as state
    , g.latitude
    , g.longitude
    , ROW_NUMBER() OVER (PARTITION BY s.seller_id ORDER BY s.seller_id) as row_num
from silver.sellers as s 
inner join silver.geolocation as g
on s.seller_zip_code_prefix = g.zip_code_prefix
) t 
where  row_num = 1
go



-- =============================================================================
-- Create dimension: gold.dim_customers 
-- =============================================================================
if object_id('gold.dim_customers','v') is not null
    drop view gold.dim_customers
go

create view gold.dim_customers as 
select 
    row_number() over (order by zip_code_prefix) as customer_key
    , zip_code_prefix
    , customer_id
    , unique_id
    , city
    , state
    , latitude
    , longitude
from
(
select     
    c.zip_code_prefix
    , c.id as customer_id					
    , c.unique_id			
    , c.city				
    , c.state	
    , g.latitude
    , g.longitude
    , ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.id) as row_num
from silver.customers as c 
inner join silver.geolocation as g
on c.zip_code_prefix = g.zip_code_prefix
) t 
where  row_num = 1
go



-- =============================================================================
-- Create dimension: gold.dim_products 
-- =============================================================================
if object_id('gold.dim_products','v') is not null
    drop view gold.dim_products
go

create view gold.dim_products as 
select 
   row_number() over (order by product_category_name) as product_key
   , product_id                    as product_id
   , product_category_name			as category
   , product_name_length			as name_length	
   , product_description_length	    as description_length
   , product_photos_qty			    as photos_quantity
   , product_weight_g				as weight_g	
   , product_length_cm				as length_cm	
   , product_height_cm				as height_cm
   , product_width_cm				as width_cm
from silver.products 
go



-- =============================================================================
-- Create Fact: gold.fact_orders
-- =============================================================================
if object_id('gold.fact_orders','v') is not null
    drop view gold.fact_orders
go

create view gold.fact_orders as 
select
    p.product_key
    , c.customer_key 
    , op.payments_key
    , r.reviews_key 
    , s.sellers_key
    , o.order_id
    , o.order_status
    , o.purchase_timestamp
    , o.order_approved_at
    , o.shipping_limit_date
    , o.price
    , o.freight_value
    , o.delivered_carrier_date
    , o.delivered_customer_date
    , o.estimated_delivery_date
from
(
    select 
        o.order_id 
        , o.customer_id
        , oi.product_id
        , oi.seller_id
        , o.order_status
        , o.purchase_timestamp
        , o.order_approved_at
        , oi.shipping_limit_date
        , oi.price
        , oi.freight_value
        , o.delivered_carrier_date
        , o.delivered_customer_date
        , o.estimated_delivery_date
    from silver.orders as o
    left join silver.order_items as oi 
    on o.order_id = oi.order_id
) o
left join gold.dim_products as p
on o.product_id = p.product_id
left join gold.dim_customers as c 
on o.customer_id = c.customer_id
left join gold.dim_order_payments as op
on o.order_id = op.order_id
left join gold.dim_order_reviews as r
on o.order_id = r.order_id
left join gold.dim_sellers as s
on o.seller_id = s.seller_id
go
