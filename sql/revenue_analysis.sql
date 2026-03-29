Query 1: Revenue by Country (Regional Imbalance)
SELECT 
    country,
    COUNT(DISTINCT invoice)          AS total_orders,
    SUM(quantity * price)            AS total_revenue,
    ROUND(AVG(quantity * price), 2)  AS avg_order_value
FROM transactions
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 10;