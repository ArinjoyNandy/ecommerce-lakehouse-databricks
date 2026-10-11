CREATE OR REPLACE VIEW ecommerce_dev.gold.v_mart_logistics_regional_sla
AS
SELECT 
    customer_macro_region,
    customer_state,
    order_purchase_year_month,
    COUNT(order_item_id) AS total_items_shipped,
    SUM(is_on_time_flag) AS total_on_time_items,
    SUM(is_delayed_flag) AS total_delayed_items,
    ROUND(AVG(delivery_lead_days), 1) AS avg_delivery_lead_days,
    ROUND(AVG(carrier_transit_days), 1) AS avg_carrier_transit_days,
    ROUND(AVG(delivery_delay_days), 1) AS avg_delay_days
FROM ecommerce_dev.gold.datablock_sales_order_lineage
WHERE is_delivered_flag = 1
GROUP BY 
    customer_macro_region,
    customer_state,
    order_purchase_year_month;