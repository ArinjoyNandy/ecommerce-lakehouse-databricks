CREATE OR REPLACE TABLE ecommerce_dev.gold.datablock_sales_order_lineage
USING DELTA
    COMMENT 'Enriched order line-item lineage joining orders, items, and customers with pre-derived SLA, financial, and geographical attributes'
AS
    WITH dedup_orders AS(
        SELECT 
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            ROW_NUMBER() OVER(
                PARTITION BY order_id
                ORDER BY order_purchase_timestamp DESC, _ingested_at DESC
            ) AS rank_order
        FROM ecommerce_dev.silver.orders
        WHERE order_id IS NOT NULL
    ),
    dedup_items AS(
        SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            price,
            freight_value,
            ROW_NUMBER() OVER(
                PARTITION BY order_id, order_item_id
                ORDER BY _ingested_at DESC
            ) AS rank_item
        FROM ecommerce_dev.silver.order_items
        WHERE order_id IS NOT NULL
    ),
    dedup_custs AS(
        SELECT
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state,
            ROW_NUMBER() OVER(
                PARTITION BY customer_id
                ORDER BY _ingested_at DESC
            ) AS rank_cust
        FROM ecommerce_dev.silver.customers
        WHERE customer_id IS NOT NULL
    )

SELECT 
    -- Primary & Foreign Keys
    i.order_id,
    i.order_item_id,
    o.customer_id,
    c.customer_unique_id,
    i.product_id,
    i.seller_id,

    -- Temporal Dimensions
    o.order_purchase_timestamp,
    CAST(o.order_purchase_timestamp AS DATE) AS order_purchase_date,
    DATE_FORMAT(o.order_purchase_timestamp, 'yyyy-MM') AS order_purchase_year_month,
    DAYOFWEEK(o.order_purchase_timestamp) AS order_purchase_day_of_week,
    CASE
        WHEN DAYOFWEEK(o.order_purchase_timestamp) IN (1,7) THEN 1
        ELSE 0
    END AS is_weekend_purchase_flag,

    -- Customer Geography Derivations
    c.customer_city,
    c.customer_state,
    CASE
        WHEN c.customer_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
        WHEN c.customer_state IN ('PR', 'SC', 'RS') THEN 'South'
        WHEN c.customer_state IN ('BA', 'PE', 'CE', 'MA', 'PB', 'RN', 'AL', 'SE', 'PI') THEN 'Northeast'
        WHEN c.customer_state IN ('MT', 'MS', 'GO', 'DF') THEN 'Central-West'
        WHEN c.customer_state IN ('AM', 'PA', 'RO', 'TO', 'AC', 'AP', 'RR') THEN 'North'
        ELSE 'Unknown'
    END AS customer_macro_region,

    -- Financial Values & Derived Tiers
    i.price AS item_price,
    i.freight_value AS item_freight_value,
    ROUND(i.price + i.freight_value, 2) AS total_item_value,
    ROUND(i.freight_value / NULLIF(i.price + i.freight_value, 0), 4) AS freight_to_total_ratio,
    CASE 
        WHEN i.price < 50.0 THEN 'BUDGET'
        WHEN i.price BETWEEN 50.0 AND 150.0 THEN 'MID_TIER'
        WHEN i.price BETWEEN 150.01 AND 500.0 THEN 'PREMIUM'
        ELSE 'LUXURY'
    END AS product_price_tier,
    CASE 
        WHEN i.freight_value > i.price THEN 1 
        ELSE 0 
    END AS is_freight_exceeding_price_flag,

    -- Logistics & Delivery SLA Derivations
    o.order_status,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_lead_days,
    DATEDIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date) AS carrier_transit_days,
    GREATEST(DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date), 0) AS delivery_delay_days,
    CASE 
        WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 'ON_TIME'
        WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 'DELAYED'
        WHEN o.order_status IN ('shipped', 'approved', 'invoiced', 'processing') THEN 'IN_TRANSIT'
        WHEN o.order_status IN ('canceled', 'unavailable') THEN 'CANCELLED'
        ELSE 'OTHER'
    END AS delivery_sla_status,

    -- Pre-calculated Metric Flags
    CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END AS is_delivered_flag,
    CASE WHEN o.order_status IN ('canceled', 'unavailable') THEN 1 ELSE 0 END AS is_cancelled_flag,
    CASE WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1 ELSE 0 END AS is_on_time_flag,
    CASE WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END AS is_delayed_flag,

    CURRENT_TIMESTAMP() AS _datablock_refreshed_at

    FROM dedup_items as i
    JOIN dedup_orders as o
        ON i.order_id = o.order_id AND o.rank_order = 1
    JOIN dedup_custs as c
        ON o.customer_id = c.customer_id AND c.rank_cust = 1
    WHERE i.rank_item = 1