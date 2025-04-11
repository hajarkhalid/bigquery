-- Declare regex pattern for better filtering
DECLARE pattern STRING DEFAULT r'(?i)\b(promo|offer|discount|deal|sale|coupon)\b';

-- Track users who visited promo-related pages in the last 30 days
WITH PromoPages AS (
    SELECT 
        user_pseudo_id,
        session_id,
        event_timestamp,
        page_location,
        traffic_source.source AS traffic_source,
        traffic_source.medium AS traffic_medium,
        device.category AS device_type,
        geo.country AS user_country
    FROM `your_project.your_dataset.events_*`
    WHERE 
        event_name = 'page_view' 
        AND REGEXP_CONTAINS(page_location, pattern) 
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) 
                             AND FORMAT_DATE('%Y%m%d', CURRENT_DATE()) 
),

UserEngagement AS (
    -- Calculate engagement metrics for promo visitors
    SELECT
        user_pseudo_id,
        COUNT(DISTINCT session_id) AS promo_sessions,  
        COUNT(*) AS promo_page_views,  
        COUNTIF(event_name = 'purchase') AS total_purchases,  
        SUM(CAST(event_bundle.product_revenue AS FLOAT64)) AS total_revenue  
    FROM `your_project.your_dataset.events_*`
    WHERE user_pseudo_id IN (SELECT DISTINCT user_pseudo_id FROM PromoPages)
    GROUP BY user_pseudo_id
),

TrafficBreakdown AS (
    -- Analyze traffic sources for promo visitors
    SELECT
        traffic_source.source AS traffic_source,
        COUNT(DISTINCT user_pseudo_id) AS total_users,
        COUNT(DISTINCT session_id) AS total_sessions
    FROM `your_project.your_dataset.events_*`
    WHERE REGEXP_CONTAINS(page_location, pattern)
    GROUP BY traffic_source
),

PromoPerformance AS (
    -- Compare promo vs. non-promo visitors
    SELECT
        REGEXP_CONTAINS(page_location, pattern) AS is_promo_visit,
        COUNT(DISTINCT user_pseudo_id) AS total_users,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNTIF(event_name = 'purchase') AS total_purchases,
        SUM(CAST(event_bundle.product_revenue AS FLOAT64)) AS total_revenue
    FROM `your_project.your_dataset.events_*`
    GROUP BY is_promo_visit
),

UserJourney AS (
    -- Track user journey from promo visit to purchase
    SELECT 
        user_pseudo_id,
        MIN(IF(REGEXP_CONTAINS(page_location, pattern), event_timestamp, NULL)) AS first_promo_visit,
        MIN(IF(event_name = 'purchase', event_timestamp, NULL)) AS first_purchase
    FROM `your_project.your_dataset.events_*`
    GROUP BY user_pseudo_id
)

-- Final Output: Promo Performance Summary
SELECT 
    p.user_pseudo_id,
    p.page_location,
    p.traffic_source,
    p.traffic_medium,
    p.device_type,
    p.user_country,
    e.promo_sessions,
    e.promo_page_views,
    e.total_purchases,
    e.total_revenue,
    ROUND(SAFE_DIVIDE(e.total_purchases, e.promo_sessions) * 100, 2) AS conversion_rate
FROM PromoPages p
LEFT JOIN UserEngagement e 
ON p.user_pseudo_id = e.user_pseudo_id
ORDER BY e.total_revenue DESC; -- Prioritize high-revenue users
