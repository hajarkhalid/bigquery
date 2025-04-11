-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH user_events AS (
  -- Flatten event data for consistent structure
  SELECT
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name,
    user_pseudo_id,
    CONCAT(collected_traffic_source.manual_source, ' / ', collected_traffic_source.manual_medium) AS traffic_source,
    collected_traffic_source.gclid,
    CONCAT(user_pseudo_id, '-', (
      SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'
    )) AS session_id,
    SAFE_CAST((
      SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged'
    ) AS INT64) AS session_engaged,
    SAFE_CAST((
      SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec'
    ) AS FLOAT64) / 1000 AS engagement_time_sec,  -- Converted to seconds
    SAFE_CAST(ecommerce.purchase_revenue AS FLOAT64) AS purchase_revenue
  FROM `project.dataset.events_*`
  WHERE _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
),

session_data AS (
  -- Aggregate session-level data
  SELECT
    EXTRACT(DATE FROM event_timestamp) AS date,
    user_pseudo_id,
    session_id,
    IF(gclid IS NOT NULL, 'google / cpc', traffic_source) AS traffic_source, 
    MIN(event_timestamp) AS session_started_at,
    MAX(event_timestamp) AS session_ended_at,
    MAX(session_engaged) AS session_engaged,
    SUM(engagement_time_sec) AS total_engagement_time_sec,
    COUNT(*) AS event_count,
    SUM(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) AS pageview_count,
    SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    SUM(CASE WHEN event_name = 'signup' THEN 1 ELSE 0 END) AS signup_count,
    IFNULL(SUM(purchase_revenue), 0) AS total_revenue
  FROM user_events
  GROUP BY user_pseudo_id, session_id, traffic_source
)

-- Final aggregation and session metrics
SELECT
  IFNULL(traffic_source, 'direct') AS traffic_source,
  COUNT(DISTINCT user_pseudo_id) AS total_users,
  COUNT(DISTINCT session_id) AS total_sessions,
  COUNT(DISTINCT CASE WHEN session_engaged = 1 THEN session_id END) AS engaged_sessions,
  ROUND(AVG(total_engagement_time_sec), 2) AS avg_engagement_time_sec,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN session_engaged = 1 THEN session_id END), COUNT(DISTINCT session_id)), 3) AS session_engagement_rate,
  SUM(pageview_count) AS total_pageviews,
  ROUND(SAFE_DIVIDE(SUM(pageview_count), COUNT(DISTINCT session_id)), 2) AS avg_pageviews_per_session,
  SUM(signup_count) AS total_signups,
  SUM(purchase_count) AS total_transactions,
  ROUND(IFNULL(SUM(total_revenue), 0), 2) AS total_revenue,
  ROUND(SAFE_DIVIDE(SUM(purchase_count), COUNT(DISTINCT session_id)), 3) AS session_conversion_rate
FROM session_data
GROUP BY traffic_source
ORDER BY total_users DESC;
