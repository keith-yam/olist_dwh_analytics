/*
===============================================================================
Product Category Report
===============================================================================
Purpose:
    - This report consolidates key product category metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product category, average size, weight and price.
    2. Segments product category by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates category-level metrics:
       - total orders
       - total sales
       - total customers (unique)
       - lifespan (in months)
       - average 
    4. Calculates valuable KPIs:
       - average order revenue 
       - average monthly revenue
===============================================================================
*/ 

-- category aggregations: summarizes key metrics at the category level
with category_aggregation as
(
    select
          p.category
          , avg(p.length_cm * p.height_cm * p.width_cm) as avg_vol
          , avg(p.weight_g) as avg_weight
          , avg(o.price) as avg_price
          , count(distinct o.order_id) as total_orders
          , sum(o.price) as total_sales
          , count(distinct c.unique_id) as total_customer 
          , cast( avg(cast(r.score as decimal(10,2))) as decimal(10,2)) as avg_review_score
          , cast(max(o.purchase_timestamp) as date) as last_order
          , datediff(month, min(o.purchase_timestamp), max(o.purchase_timestamp)) as lifespan
          , cast(round(avg(o.freight_value),2) as decimal(10,2)) as avg_freight_cost
          , avg( datediff(day, o.order_approved_at, o.delivered_customer_date )  ) as avg_actual_delivery_period
          , avg( datediff(day, o.order_approved_at, o.estimated_delivery_date )  ) as avg_estimated_delivery_period
    from gold.fact_orders as o
    left join gold.dim_products as p
    on o.products_key = p.product_key
    left join gold.dim_order_reviews as r
    on o.reviews_key = r.reviews_key
    join gold.dim_customers as c 
    on o.customers_key = c.customer_key
    where o.order_id is not null
    group by p.category
),
category_sales_2017 as (
    select
        p.category as product_category,
        sum(o.price) as sales_amount
    from gold.fact_orders o 
    left join gold.dim_products p
        on o.products_key = p.product_key
    where o.purchase_timestamp is not null and year(o.purchase_timestamp) = 2017
    group by 
        p.category
),
category_sales_2018 as (
    select
        p.category as product_category,
        sum(o.price) as sales_amount
    from gold.fact_orders o 
    left join gold.dim_products p
        on o.products_key = p.product_key
    where o.purchase_timestamp is not null and year(o.purchase_timestamp) = 2018
    group by 
        p.category
)
-- final query: combines all product results into one output
select
    category
    , avg_vol as 'average size (m3)'
    , avg_weight as ' average weight (g)' 
    , avg_price as 'average price' 
    , total_orders 'total orders' 
    , total_sales 'total sales'  
    , t2.sales_amount as 'total sales in 2017' 
    , t3.sales_amount as 'total sales in 2018' 
    , concat( cast((t3.sales_amount - t2.sales_amount)/t2.sales_amount*100 as varchar),'%') as 'YoY change'
    , total_customer 'total customer' 
    , avg_review_score 'avg review score'
    , case
		when total_sales > 500000 then 'high-performer'
		when total_sales >= 100000 then 'mid-range'
		else 'low-performer'
	  end as product_segment    
	 , case 
		when total_orders = 0 then 0
		else total_sales / total_orders
	  end as 'average order revenue'  -- average order revenue (aor)
  	, case
		when lifespan = 0 then total_sales
		else total_sales / lifespan 
	  end as 'average monthly revenue'  -- average monthly revenue
    , lifespan
    , last_order 'last order'
    , avg_freight_cost 'average freight cost' 
    , avg_actual_delivery_period 'average actual delivery period' 
    , avg_estimated_delivery_period 'average estimated delivery period'

from category_aggregation as t1
join category_sales_2017 as t2
on t1.category = t2.product_category
join category_sales_2018 as t3
on t1.category = t3.product_category

order by [total sales] desc