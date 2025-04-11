-- Declaration of date range for analysis
DECLARE start_date STRING DEFAULT "2024-01-01"; -- Start date for filtering
DECLARE end_date STRING DEFAULT "2024-12-31"; -- End date for filtering

-- First-click attribution model
WITH first_click AS (
  SELECT
    user_pseudo_id, -- Unique user identifier
    MIN(event_timestamp) AS first_click_timestamp, -- Timestamp of first click event
    -- Extracting the source, medium, campaign, etc. from the first event
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'utm_source') AS first_utm_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'utm_medium') AS first_utm_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'utm_campaign') AS first_utm_campaign
  FROM
    `your-project-id.analytics_1234567890.events_*` -- Replace with your project ID
  WHERE
    event_name = 'session_start' -- Considering first click to be the session start
    AND DATE(event_timestamp) BETWEEN DATE(start_date) AND DATE(end_date)
  GROUP BY
    user_pseudo_id
),

-- Filtering conversions that occurred after the first click
conversions AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT IFNULL(value.int_value, 0) FROM UNNEST(event_params) WHERE KEY = 'value') +
    (SELECT IFNULL(value.float_value, 0.0) FROM UNNEST(event_params) WHERE KEY = 'value') +
    (SELECT IFNULL(value.double_value, 0.0) FROM UNNEST(event_params) WHERE KEY = 'value') AS purchase_value
  FROM
    `your-project-id.analytics_1234567890.events_*` -- Replace with your project ID
  WHERE
    event_name = 'purchase' -- Considering purchase as the conversion event
    AND DATE(event_timestamp) BETWEEN DATE(start_date) AND DATE(end_date)
),

-- Joining first-click attribution with conversions
attribution AS (
  SELECT
    fc.user_pseudo_id,
    fc.first_utm_source,
    fc.first_utm_medium,
    fc.first_utm_campaign,
    c.purchase_value
  FROM
    first_click fc
  LEFT JOIN
    conversions c
  ON
    fc.user_pseudo_id = c.user_pseudo_id
  WHERE
    c.purchase_value > 0 -- Only considering users with a purchase value
)

-- Final aggregation to get the total purchase value by first-touch attribution
SELECT
  first_utm_source,
  first_utm_medium,
  first_utm_campaign,
  SUM(purchase_value) AS total_purchase_value
FROM
  attribution
GROUP BY
  first_utm_source,
  first_utm_medium,
  first_utm_campaign
ORDER BY
  total_purchase_value DESC; -- Sorting by total purchase value in descending order
