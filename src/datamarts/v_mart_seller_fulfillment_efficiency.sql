CREATE OR REPLACE VIEW ecommerce_dev.gold.v_mart_seller_fulfillment_efficiency
AS
SELECT 
    seller_id,
    COUNT(DISTINCT order_id) AS total_orders_fulfilled,
    COUNT(order_item_id) AS total_items_sold,
    ROUND(SUM(item_price), 2) AS total_seller_revenue,
    SUM(is_on_time_flag) AS total_on_time_items,
    SUM(is_delayed_flag) AS total_delayed_items,
    ROUND(AVG(carrier_transit_days), 1) AS avg_carrier_transit_days,
    ROUND(AVG(delivery_lead_days), 1) AS avg_delivery_lead_days
FROM ecommerce_dev.gold.datablock_sales_order_lineage
WHERE is_delivered_flag = 1
GROUP BY 
    seller_id;