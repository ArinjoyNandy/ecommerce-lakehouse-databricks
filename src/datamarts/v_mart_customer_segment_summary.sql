CREATE OR REPLACE VIEW ecommerce_dev.gold.v_mart_customer_segment_summary
AS
SELECT 
    customer_macro_region,
    rfm_frequency_tier,
    rfm_monetary_tier,
    rfm_recency_tier,
    COUNT(customer_unique_id) AS total_customers,
    SUM(lifetime_orders_count) AS cohort_lifetime_orders,
    ROUND(SUM(lifetime_spend_merchandise), 2) AS cohort_merchandise_spend,
    ROUND(SUM(lifetime_spend_freight), 2) AS cohort_freight_spend,
    ROUND(SUM(lifetime_spend_gross), 2) AS cohort_gross_spend,
    ROUND(AVG(recency_days), 1) AS avg_cohort_recency_days,
    ROUND(AVG(avg_historical_order_value), 2) AS avg_cohort_aov
FROM ecommerce_dev.gold.datablock_customer_360
GROUP BY 
    customer_macro_region,
    rfm_frequency_tier,
    rfm_monetary_tier,
    rfm_recency_tier;