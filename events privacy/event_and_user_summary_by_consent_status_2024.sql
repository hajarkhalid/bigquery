-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

-- Query for consent summary: Total events, users, and sessions by consent status
SELECT
  privacy_info.analytics_storage AS analytics_storage_status,
  privacy_info.ads_storage AS ads_storage_status,
  device.category AS device_type,
  geo.country AS user_country,
  
  -- Total Events and Users
  COUNT(*) AS total_events,
  ROUND(100 * SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()), 2) AS event_percentage,
  COUNT(DISTINCT user_pseudo_id) AS total_users,
  ROUND(100 * SAFE_DIVIDE(COUNT(DISTINCT user_pseudo_id), SUM(COUNT(DISTINCT user_pseudo_id)) OVER ()), 2) AS user_percentage,
  
  -- Sessions
  COUNT(DISTINCT CONCAT(user_pseudo_id, '-', 
    (SELECT value.int_value 
     FROM UNNEST(event_params) 
     WHERE key = 'ga_session_id'))) AS total_sessions,
  ROUND(100 * SAFE_DIVIDE(COUNT(DISTINCT CONCAT(user_pseudo_id, '-', 
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'))), 
    SUM(COUNT(DISTINCT CONCAT(user_pseudo_id, '-', 
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')))) OVER ()), 2) AS session_percentage,

  -- Engagement Metrics
  AVG((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') / 1000) AS avg_engagement_time_sec,
  COUNT(DISTINCT CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 THEN user_pseudo_id END) AS new_users,

  -- Conversion Data
  SUM(CASE WHEN event_name = 'purchase' THEN 
      (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') ELSE 0 END) AS total_revenue,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) AS converting_users
  
FROM
  `project.dataset.events_*` -- Replace with your actual dataset
WHERE
  _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
GROUP BY
  analytics_storage_status, ads_storage_status, device_type, user_country
ORDER BY
  total_events DESC; -- Order by total event count
