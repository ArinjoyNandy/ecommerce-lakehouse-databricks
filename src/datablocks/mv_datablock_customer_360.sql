CREATE OR REPLACE MATERIALIZED VIEW ecommerce_dev.gold.mv_datablock_customer_360
    COMMENT 'Curated customer dimension joining orders and items to pre-compute RFM segmentation and lifetime spend'
AS
    WITH customer_base AS (
        SELECT 
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) AS rank_c
        FROM ecommerce_dev.silver.customers
    ),
    order_base AS (
        SELECT 
            order_id,
            customer_id,
            order_status,
            CAST(order_purchase_timestamp AS DATE) AS order_date,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_purchase_timestamp DESC) AS rank_o
        FROM ecommerce_dev.silver.orders
    ),
    item_base AS (
        SELECT 
            order_id,
            price,
            freight_value,
            ROW_NUMBER() OVER (PARTITION BY order_id, order_item_id ORDER BY _ingested_at DESC) AS rank_i
        FROM ecommerce_dev.silver.order_items
    ),
    customer_aggregations AS (
        SELECT 
            c.customer_unique_id,
            c.customer_city,
            c.customer_state,
            COUNT(DISTINCT o.order_id) AS lifetime_orders_count,
            COALESCE(SUM(i.price), 0.0) AS lifetime_spend_merchandise,
            COALESCE(SUM(i.freight_value), 0.0) AS lifetime_spend_freight,
            COALESCE(SUM(i.price + i.freight_value), 0.0) AS lifetime_spend_gross,
            MIN(o.order_date) AS first_order_date,
            MAX(o.order_date) AS latest_order_date
        FROM customer_base c
        JOIN order_base o 
            ON c.customer_id = o.customer_id AND c.rank_c = 1 AND o.rank_o = 1
        LEFT JOIN item_base i 
            ON o.order_id = i.order_id AND i.rank_i = 1
        WHERE o.order_status NOT IN ('canceled', 'unavailable')
        GROUP BY 
            c.customer_unique_id,
            c.customer_city,
            c.customer_state
    ),
    reference_snapshot AS (
        SELECT MAX(order_date) AS current_snapshot_date FROM order_base
    )
SELECT 
    ca.customer_unique_id,
    ca.customer_city,
    ca.customer_state,

    -- Macro Region
    CASE 
        WHEN ca.customer_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
        WHEN ca.customer_state IN ('PR', 'SC', 'RS') THEN 'South'
        WHEN ca.customer_state IN ('BA', 'PE', 'CE', 'MA', 'PB', 'RN', 'AL', 'SE', 'PI') THEN 'Northeast'
        WHEN ca.customer_state IN ('MT', 'MS', 'GO', 'DF') THEN 'Central-West'
        WHEN ca.customer_state IN ('AM', 'PA', 'RO', 'TO', 'AC', 'AP', 'RR') THEN 'North'
        ELSE 'Unknown'
    END AS customer_macro_region,

    -- Financial Totals
    ca.lifetime_orders_count,
    ROUND(ca.lifetime_spend_merchandise, 2) AS lifetime_spend_merchandise,
    ROUND(ca.lifetime_spend_freight, 2) AS lifetime_spend_freight,
    ROUND(ca.lifetime_spend_gross, 2) AS lifetime_spend_gross,
    ROUND(ca.lifetime_spend_gross / NULLIF(ca.lifetime_orders_count, 0), 2) AS avg_historical_order_value,

    -- Recency
    ca.first_order_date,
    ca.latest_order_date,
    DATEDIFF(ref.current_snapshot_date, ca.latest_order_date) AS recency_days,

    -- RFM Classifications
    CASE 
        WHEN ca.lifetime_orders_count >= 4 THEN 'CHAMPION'
        WHEN ca.lifetime_orders_count BETWEEN 2 AND 3 THEN 'REPEAT_BUYER'
        ELSE 'ONE_TIME_BUYER'
    END AS rfm_frequency_tier,
    CASE 
        WHEN ca.lifetime_spend_gross > 500.0 THEN 'HIGH_VALUE'
        WHEN ca.lifetime_spend_gross BETWEEN 150.0 AND 500.0 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END AS rfm_monetary_tier,
    CASE 
        WHEN DATEDIFF(ref.current_snapshot_date, ca.latest_order_date) <= 90 THEN 'ACTIVE'
        WHEN DATEDIFF(ref.current_snapshot_date, ca.latest_order_date) BETWEEN 91 AND 270 THEN 'WARM'
        ELSE 'CHURNED'
    END AS rfm_recency_tier,

    CURRENT_TIMESTAMP() AS _datablock_refreshed_at
    
    FROM customer_aggregations ca
    CROSS JOIN reference_snapshot ref;