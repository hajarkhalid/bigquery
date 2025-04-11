-- Declare parameters for flexibility
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';
DECLARE promo_pattern STRING DEFAULT r'(?i)\b(promo|offer|discount|deal|sale|coupon)\b';

-- Step 1: Identify Promo Interactions
WITH PromoPages AS (
    SELECT 
        user_pseudo_id,
        session_id,
        event_timestamp,
        page_location,
        traffic_source.source AS source,
        traffic_source.medium AS medium,
        device.category AS device_type,
        geo.country AS user_country,
        COUNT(*) OVER(PARTITION BY user_pseudo_id) AS total_promo_pageviews
    FROM `your_project.your_dataset.events_*`
    WHERE event_name = 'page_view' 
        AND REGEXP_CONTAINS(page_location, promo_pattern)
        AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) 
                             AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
),

-- Step 2: Calculate Promo Engagement Metrics
UserEngagement AS (
    SELECT
        user_pseudo_id,
        COUNT(DISTINCT session_id) AS promo_sessions,
        COUNT(*) AS promo_page_views,
        COUNTIF(event_name = 'purchase') AS purchases,
        SUM(purchase_revenue) AS revenue_generated,
        SAFE_DIVIDE(COUNTIF(event_name = 'purchase'), COUNT(DISTINCT session_id)) AS session_conversion_rate
    FROM `your_project.your_dataset.events_*`
    WHERE user_pseudo_id IN (SELECT DISTINCT user_pseudo_id FROM PromoPages)
    GROUP BY user_pseudo_id
),

-- Step 3: Multi-Touch Attribution Models
Attribution AS (
    SELECT
        user_pseudo_id,
        MIN(IF(REGEXP_CONTAINS(page_location, promo_pattern), event_timestamp, NULL)) AS first_touch,
        MAX(IF(REGEXP_CONTAINS(page_location, promo_pattern), event_timestamp, NULL)) AS last_touch,
        COUNTIF(REGEXP_CONTAINS(page_location, promo_pattern)) / COUNT(*) AS linear_attribution_score
    FROM `your_project.your_dataset.events_*`
    GROUP BY user_pseudo_id
),

-- Step 4: Promo CTR Analysis
PromoCTR AS (
    SELECT
        user_pseudo_id,
        COUNTIF(event_name = 'click' AND REGEXP_CONTAINS(page_location, promo_pattern)) AS promo_clicks,
        COUNTIF(event_name = 'page_view' AND REGEXP_CONTAINS(page_location, promo_pattern)) AS promo_views,
        SAFE_DIVIDE(COUNTIF(event_name = 'click' AND REGEXP_CONTAINS(page_location, promo_pattern)),
                    COUNTIF(event_name = 'page_view' AND REGEXP_CONTAINS(page_location, promo_pattern))) AS promo_ctr
    FROM `your_project.your_dataset.events_*`
    GROUP BY user_pseudo_id
),

-- Step 5: Bounce Rate Calculation
BounceRate AS (
    SELECT
        user_pseudo_id,
        COUNTIF(session_engaged = FALSE) AS bounced_sessions,
        COUNT(DISTINCT session_id) AS total_sessions,
        SAFE_DIVIDE(COUNTIF(session_engaged = FALSE), COUNT(DISTINCT session_id)) AS bounce_rate
    FROM `your_project.your_dataset.events_*`
    WHERE user_pseudo_id IN (SELECT DISTINCT user_pseudo_id FROM PromoPages)
    GROUP BY user_pseudo_id
),

-- Step 6: Customer Lifetime Value (CLV) Estimation
CLV AS (
    SELECT 
        user_pseudo_id,
        SUM(purchase_revenue) AS total_revenue,
        COUNT(DISTINCT session_id) AS total_sessions,
        SAFE_DIVIDE(SUM(purchase_revenue), COUNT(DISTINCT session_id)) AS avg_revenue_per_session,
        SAFE_DIVIDE(SUM(purchase_revenue), COUNT(DISTINCT user_pseudo_id)) AS estimated_clv
    FROM `your_project.your_dataset.events_*`
    WHERE event_name = 'purchase'
    GROUP BY user_pseudo_id
),

-- Step 7: Retention Analysis for Promo Visitors
Retention AS (
    SELECT
        user_pseudo_id,
        MIN(event_timestamp) AS first_interaction,
        MAX(event_timestamp) AS last_interaction,
        DATE_DIFF(DATE(MAX(event_timestamp)), DATE(MIN(event_timestamp)), DAY) AS retention_days,
        COUNT(DISTINCT session_id) AS session_count
    FROM `your_project.your_dataset.events_*`
    WHERE user_pseudo_id IN (SELECT DISTINCT user_pseudo_id FROM PromoPages)
    GROUP BY user_pseudo_id
),

-- Step 8: Search Appearance Metrics (AMP, Video, Shopping, etc.)
SearchAppearance AS (
    SELECT  
        SUM(COALESCE(CAST(is_amp_top_stories AS INT64), 0)) AS is_amp_top_stories,
        SUM(COALESCE(CAST(is_amp_blue_link AS INT64), 0)) AS is_amp_blue_link,
        SUM(COALESCE(CAST(is_job_listing AS INT64), 0)) AS is_job_listing,
        SUM(COALESCE(CAST(is_video AS INT64), 0)) AS is_video_results,
        SUM(COALESCE(CAST(is_organic_shopping AS INT64), 0)) AS is_organic_shopping,
        SUM(COALESCE(CAST(is_review_snippet AS INT64), 0)) AS is_review_snippet
    FROM `bigquery-public-data.google_search_data.searchdata_url_impression`
    WHERE DATE(data_date) BETWEEN start_date AND end_date
)

-- Final Output: Comprehensive Promo Analysis
SELECT 
    p.user_pseudo_id,
    p.page_location,
    p.source,
    p.medium,
    p.device_type,
    p.user_country,
    e.promo_sessions,
    e.promo_page_views,
    e.purchases,
    e.revenue_generated,
    e.session_conversion_rate,
    a.first_touch,
    a.last_touch,
    a.linear_attribution_score,
    ctr.promo_ctr,
    br.bounce_rate,
    clv.estimated_clv,
    r.retention_days,
    r.session_count,
    sa.is_amp_top_stories,
    sa.is_amp_blue_link,
    sa.is_job_listing,
    sa.is_video_results,
    sa.is_review_snippet
FROM PromoPages p
LEFT JOIN UserEngagement e ON p.user_pseudo_id = e.user_pseudo_id
LEFT JOIN Attribution a ON p.user_pseudo_id = a.user_pseudo_id
LEFT JOIN PromoCTR ctr ON p.user_pseudo_id = ctr.user_pseudo_id
LEFT JOIN BounceRate br ON p.user_pseudo_id = br.user_pseudo_id
LEFT JOIN CLV clv ON p.user_pseudo_id = clv.user_pseudo_id
LEFT JOIN Retention r ON p.user_pseudo_id = r.user_pseudo_id
CROSS JOIN SearchAppearance sa -- One row, so no need for join condition
ORDER BY e.revenue_generated DESC;
