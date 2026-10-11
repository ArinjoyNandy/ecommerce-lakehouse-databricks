CREATE OR REPLACE VIEW ecommerce_dev.gold.v_mart_daily_executive_sales
AS
SELECT 
    order_purchase_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(order_item_id) AS total_line_items_sold,
    SUM(is_delivered_flag) AS total_delivered_items,
    SUM(is_cancelled_flag) AS total_cancelled_items,
    ROUND(SUM(item_price), 2) AS total_merchandise_revenue,
    ROUND(SUM(item_freight_value), 2) AS total_freight_revenue,
    ROUND(SUM(total_item_value), 2) AS gross_transaction_value
FROM ecommerce_dev.gold.datablock_sales_order_lineage
GROUP BY 
    order_purchase_date;