DECLARE start_date STRING DEFAULT '2024-01-01'; -- Start date for filtering
DECLARE end_date STRING DEFAULT '2024-12-31'; -- End date for filtering

WITH attribution_data AS (
  SELECT
    user_pseudo_id, -- Unique user identifier
    event_name,     -- Event name (e.g., 'purchase', 'view')
    event_timestamp, -- Timestamp of the event
    traffic_source,  -- Traffic source (e.g., 'google', 'facebook')
    revenue -- Event revenue (if applicable)
  FROM
    `your-project-id.analytics_1234567890.events_*` -- Replace with your actual project and dataset
  WHERE
    DATE(event_timestamp) BETWEEN DATE(start_date) AND DATE(end_date) -- Filter by date range
),

-- Get the first and last touch events per user
first_last_touch AS (
  SELECT
    user_pseudo_id,
    MIN(event_timestamp) AS first_touch_time, -- First touch time
    MAX(event_timestamp) AS last_touch_time, -- Last touch time
    COUNT(DISTINCT traffic_source) AS touch_points -- Number of touch points per user
  FROM
    attribution_data
  GROUP BY
    user_pseudo_id
),

-- Calculate the attribution score for each touch point (Linear Attribution)
linear_attribution AS (
  SELECT
    a.user_pseudo_id,
    a.traffic_source,
    COUNT(a.traffic_source) / f.touch_points AS attribution_score, -- Equal attribution across all touch points
    SUM(a.revenue) AS total_revenue
  FROM
    attribution_data a
  JOIN
    first_last_touch f
  ON
    a.user_pseudo_id = f.user_pseudo_id
  WHERE
    a.event_timestamp BETWEEN f.first_touch_time AND f.last_touch_time
  GROUP BY
    a.user_pseudo_id, a.traffic_source, f.touch_points
)

SELECT
  traffic_source,
  SUM(attribution_score) AS total_attribution_score,
  SUM(total_revenue) AS total_revenue
FROM
  linear_attribution
GROUP BY
  traffic_source
ORDER BY
  total_attribution_score DESC; -- Order by total attribution score
