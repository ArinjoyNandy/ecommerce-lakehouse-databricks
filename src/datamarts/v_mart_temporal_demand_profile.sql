CREATE OR REPLACE VIEW ecommerce_dev.gold.v_mart_temporal_demand_profile
AS
SELECT 
    order_purchase_year_month,
    order_purchase_day_of_week,
    is_weekend_purchase_flag,
    COUNT(DISTINCT order_id) AS total_orders_placed,
    COUNT(order_item_id) AS total_items_ordered,
    ROUND(SUM(total_item_value), 2) AS gross_ordered_value
FROM ecommerce_dev.gold.datablock_sales_order_lineage
GROUP BY 
    order_purchase_year_month,
    order_purchase_day_of_week,
    is_weekend_purchase_flag;