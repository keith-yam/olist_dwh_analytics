/*
===============================================================================
City Performance Report
===============================================================================
Purpose:
    - This report consolidates key city metrics

Report Format:
    1. city
	2. number of customer
	3. percentage of total customer
	4. number of purchases
	5. percentage of total purchases
	6. number of sellers
	7. percentage of total sellers
	8. avg number of orders per month 
	9. top category 
	10. avg number of orders for top category 
===============================================================================
*/

-- top category in each city
with top_category_per_city as
(
	select * from
	(
		SELECT c.city
		, p.category
		, (count(distinct o.order_id)) AS total_sold
		, row_number() over (partition by c.city order by count(distinct o.order_id) desc) as rank
		from gold.fact_orders as o 
		left join gold.dim_products as p
		on o.products_key = p.product_key
		left join gold.dim_customers as c
		on o.customers_key = c.customer_key
		where p.category is not null
		GROUP BY c.city, p.category
	--	ORDER BY total_sold desc
	)  t
	where rank = 1 
--	ORDER BY total_sold desc
),
city_report as
(
	select
		c.city
		, count(distinct c.unique_id) as customer_per_city
		, (select count(distinct unique_id) from gold.dim_customers) as total_customer
		, count(distinct o.order_id) as purchase_per_city
		, (select count(distinct order_id) from gold.fact_orders) as total_purchase
		, cast( cast( count(distinct  o.order_id) as numeric) * 100 / 
			sum( cast( count(distinct o.order_id) as numeric) ) 
			over() as decimal(10,2)) 
			as percentage_of_purchase 
		, count(distinct s.seller_id) as sellers_per_city
		, (select count(distinct seller_id) from gold.dim_sellers) as total_sellers
		, datediff(month, min(o.purchase_timestamp), max(o.purchase_timestamp)) as lifespan 
	from gold.fact_orders as o 
	left join gold.dim_customers as c
	on o.customers_key = c.customer_key
	left join gold.dim_sellers as s
	on o.sellers_key = s.sellers_key
	group by c.city
	-- order by customer_per_city desc
)


-- Final Query: formatting 
select
	r.city
	, customer_per_city as 'number of customers'
	, cast(cast(customer_per_city as numeric) / total_customer * 100 as decimal(10,2)) as '% of total customer'
	, purchase_per_city as 'number of orders'
	, cast(cast( purchase_per_city as numeric) / total_purchase * 100 as decimal(10,2)) '% of total purchase'
	, sellers_per_city as 'number of sellers' 
	, cast(cast( sellers_per_city as numeric) / total_sellers * 100 as decimal(10,2)) '% of total sellers'
	, coalesce( purchase_per_city / nullif(lifespan,0), 1) as 'avg orders per month'
	, c.category as 'top category'
	, coalesce( c.total_sold / nullif(lifespan,0), 1) as 'avg orders per month for top category'
from city_report as r
join top_category_per_city as c
on r.city = c.city
order by customer_per_city desc 
