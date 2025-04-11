DECLARE start_date STRING DEFAULT '2024-01-01'; -- Start date for filtering
DECLARE end_date STRING DEFAULT '2024-12-31';   -- End date for filtering

WITH conversion_data AS (
  SELECT
    user_pseudo_id,
    event_name,
    -- Attribution based on the last click (session-level)
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'traffic_source') AS last_click_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS last_click_campaign,
    COUNT(DISTINCT session_id) AS conversion_sessions, -- Count of sessions with conversions
    SUM(event_revenue) AS total_revenue -- Total revenue for the conversion sessions
  FROM
    `your-project-id.dataset_name.events_*` -- Replace with your actual project and dataset
  WHERE
    event_name = 'purchase' -- Filter for purchase events
    AND DATE(event_timestamp) BETWEEN DATE(start_date) AND DATE(end_date) -- Filter by date range
  GROUP BY
    user_pseudo_id, last_click_source, last_click_campaign
),

attribution_result AS (
  SELECT
    last_click_source,
    last_click_campaign,
    SUM(conversion_sessions) AS total_conversions,
    SUM(total_revenue) AS total_revenue
  FROM
    conversion_data
  GROUP BY
    last_click_source, last_click_campaign
)

-- Final output: Attributed conversions and revenue by traffic source and campaign
SELECT
  last_click_source AS traffic_source,
  last_click_campaign AS campaign,
  total_conversions,
  total_revenue,
  ROUND((total_revenue / total_conversions), 2) AS average_revenue_per_conversion
FROM
  attribution_result
ORDER BY
  total_revenue DESC;
