-- 1. FILE COMPACTION & MULTIDIMENSIONAL CLUSTERING (Z-ORDER)
-- Optimize Silver Core Tables
OPTIMIZE ecommerce_dev.silver.orders
ZORDER BY (order_purchase_timestamp, order_status);

OPTIMIZE ecommerce_dev.silver.order_items
ZORDER BY (order_id, product_id);

OPTIMIZE ecommerce_dev.silver.customers
ZORDER BY (customer_id, customer_state);

-- Optimize Gold Curated DataBlocks
OPTIMIZE ecommerce_dev.gold.datablock_sales_order_lineage
ZORDER BY (order_purchase_date, customer_state, product_price_tier);

OPTIMIZE ecommerce_dev.gold.datablock_customer_360
ZORDER BY (customer_macro_region, rfm_frequency_tier, rfm_monetary_tier);


-- 2. COST-BASED OPTIMIZER (CBO) STATISTICS
ANALYZE TABLE ecommerce_dev.gold.datablock_sales_order_lineage 
COMPUTE STATISTICS FOR ALL COLUMNS;

ANALYZE TABLE ecommerce_dev.gold.datablock_customer_360 
COMPUTE STATISTICS FOR ALL COLUMNS;

-- 3. STORAGE PRUNING (Remove obsolete Delta versions older than 7 days)
VACUUM ecommerce_dev.silver.orders RETAIN 168 HOURS;
VACUUM ecommerce_dev.silver.order_items RETAIN 168 HOURS;
VACUUM ecommerce_dev.silver.customers RETAIN 168 HOURS;
VACUUM ecommerce_dev.gold.datablock_sales_order_lineage RETAIN 168 HOURS;
VACUUM ecommerce_dev.gold.datablock_customer_360 RETAIN 168 HOURS;
