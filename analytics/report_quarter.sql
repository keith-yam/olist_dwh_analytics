/*
===============================================================================
Product Category Report
===============================================================================
Purpose:
    - This report consolidates key product category quarterly sales metric

Format:
    1. Product Category
    2. Year and Quarter
    3. Annual Sales
    4. Quarterly Sales
    5. Runing Total of Sales
    6. QoQ change
===============================================================================
*/ 

with quarterly_product_sales as (
    select
        year(o.purchase_timestamp) as purchase_year,
        datepart(quarter,o.purchase_timestamp) as quarter,
        p.category as product_category,
        sum(o.price) as sales_amount
    from gold.fact_orders o 
    left join gold.dim_products p
        on o.products_key = p.product_key
    where o.purchase_timestamp is not null
    group by 
        year(o.purchase_timestamp),
        datepart(quarter,o.purchase_timestamp),
        p.category
), 
sales_with_running_total as
(
    select
        product_category,
        purchase_year,
        quarter,
        sum(sales_amount) over (partition by product_category, purchase_year)  as annual_sales,
        sum(sales_amount) over (partition by product_category, quarter, purchase_year)  as quarterly_sales,
        sum(sales_amount) over (partition by product_category order by quarter, purchase_year) as running_total_quarterly_sales,
        sales_amount - avg(sales_amount) over (partition by product_category) as diff_by_avg,
    --    case 
    --        when sales_amount - avg(sales_amount) over (partition by product_category) > 0 then 'above avg'
    --        when sales_amount - avg(sales_amount) over (partition by product_category) < 0 then 'below avg'
    --        else 'avg'
    --    end as avg_change,
        -- quarter over quarter analysis
        --lag(sales_amount) over (partition by product_category order by quarter) as py_sales,
        (sales_amount - lag(sales_amount) over (partition by product_category order by quarter) ) * 100/ 
                     lag(sales_amount) over (partition by product_category order by quarter)
                      as quarterly_change_percentage
  --      case 
  --          when sales_amount - lag(sales_amount) over (partition by product_category order by quarter) > 0 then 'increase'
  --          when sales_amount - lag(sales_amount) over (partition by product_category order by quarter) < 0 then 'decrease'
  --          else 'no change'
  --      end as quarterly_change
    from quarterly_product_sales
 --   order by product_category, purchase_year, quarter
)

-- final query: formatting

select
    product_category as 'Product Category' 
    , concat(cast(purchase_year as int),'-Q',quarter) 'Year-Quarter'
    , annual_sales as 'Annual Sales'
    , quarterly_sales as 'Quarterly Sales'
    , running_total_quarterly_sales as 'Running Total of Quarterly Sales'
    , concat( cast(  COALESCE(quarterly_change_percentage,'0') as varchar),'%') as 'QoQ change'
from
    sales_with_running_total
where product_category is not null