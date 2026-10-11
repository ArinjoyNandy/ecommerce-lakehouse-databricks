CREATE OR REPLACE VIEW ecommerce_dev.gold.v_mart_freight_leakage_analysis
AS
SELECT 
    product_price_tier,
    COUNT(order_item_id) AS total_items_sold,
    SUM(is_freight_exceeding_price_flag) AS total_freight_exceeding_price_items,
    ROUND(SUM(item_price), 2) AS total_merchandise_revenue,
    ROUND(SUM(item_freight_value), 2) AS total_freight_cost,
    ROUND(SUM(total_item_value), 2) AS total_line_item_value,
    ROUND(AVG(freight_to_total_ratio), 4) AS avg_freight_burden_ratio
FROM ecommerce_dev.gold.datablock_sales_order_lineage
WHERE is_cancelled_flag = 0
GROUP BY 
    product_price_tier;