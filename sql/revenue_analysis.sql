-- Query 1: Revenue by Country (Regional Imbalance)
SELECT 
    country,
    COUNT(DISTINCT invoice)          AS total_orders,
    SUM(quantity * price)            AS total_revenue,
    ROUND(AVG(quantity * price), 2)  AS avg_order_value
FROM transactions
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 2: Monthly Revenue Trend
SELECT
    TO_CHAR(invoice_date::timestamp, 'YYYY-MM')  AS month,
    COUNT(DISTINCT invoice)                       AS total_orders,
    ROUND(SUM(quantity * price)::numeric, 2)      AS monthly_revenue
FROM transactions
WHERE quantity > 0 AND price > 0
GROUP BY month
ORDER BY month;


-- Query 3: Running Revenue Total (Window Function + CTE)
WITH monthly_revenue AS (
    SELECT
        TO_CHAR(invoice_date::timestamp, 'YYYY-MM') AS month,
        ROUND(SUM(quantity * price)::numeric, 2)    AS monthly_revenue
    FROM transactions
    WHERE quantity > 0 AND price > 0
    GROUP BY month
)
SELECT
    month,
    monthly_revenue,
    ROUND(SUM(monthly_revenue) OVER (ORDER BY month)::numeric, 2)        AS running_total,
    ROUND(AVG(monthly_revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)::numeric, 2) AS moving_avg_3m
FROM monthly_revenue
ORDER BY month;


-- Query 4: Customer Lifetime Value (CLV)
WITH customer_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT invoice)                  AS total_orders,
        ROUND(SUM(quantity * price)::numeric, 2) AS total_spent,
        MIN(invoice_date::timestamp)             AS first_purchase,
        MAX(invoice_date::timestamp)             AS last_purchase
    FROM transactions
    WHERE quantity > 0 AND price > 0
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    total_spent,
    ROUND((total_spent / total_orders)::numeric, 2)          AS avg_order_value,
    DATE_PART('day', last_purchase - first_purchase)         AS customer_lifespan_days,
    RANK() OVER (ORDER BY total_spent DESC)                  AS revenue_rank
FROM customer_stats
ORDER BY total_spent DESC
LIMIT 20;



-- Query 5: Top 10 Products by Revenue
SELECT
    stock_code,
    description,
    SUM(quantity)                            AS total_units_sold,
    ROUND(SUM(quantity * price)::numeric, 2) AS total_revenue,
    ROUND(AVG(price)::numeric, 2)            AS avg_price,
    RANK() OVER (ORDER BY SUM(quantity * price) DESC) AS revenue_rank
FROM transactions
WHERE quantity > 0 AND price > 0
    AND description IS NOT NULL
GROUP BY stock_code, description
ORDER BY total_revenue DESC
LIMIT 10;



-- Query 6: Customer Segmentation (RFM-style using CTEs)
WITH customer_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT invoice)                  AS frequency,
        ROUND(SUM(quantity * price)::numeric, 2) AS monetary,
        MAX(invoice_date::timestamp)             AS last_purchase
    FROM transactions
    WHERE quantity > 0 AND price > 0
    GROUP BY customer_id
),
segmented AS (
    SELECT
        customer_id,
        frequency,
        monetary,
        CASE
            WHEN monetary >= 10000 THEN 'VIP'
            WHEN monetary >= 1000  THEN 'Loyal'
            WHEN monetary >= 100   THEN 'Regular'
            ELSE 'Low Value'
        END AS segment
    FROM customer_stats
)
SELECT
    segment,
    COUNT(*)                             AS total_customers,
    ROUND(AVG(monetary)::numeric, 2)     AS avg_revenue,
    ROUND(SUM(monetary)::numeric, 2)     AS total_revenue
FROM segmented
GROUP BY segment
ORDER BY total_revenue DESC;