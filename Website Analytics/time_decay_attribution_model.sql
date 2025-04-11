-- Declaration of date range for analysis
DECLARE start_date STRING DEFAULT "2024-01-01"; -- Start date for filtering
DECLARE end_date STRING DEFAULT "2024-12-31"; -- End date for filtering

-- Time decay attribution model
WITH interactions AS (
  SELECT
    user_pseudo_id, -- Unique user identifier
    event_timestamp, -- Timestamp of interaction event
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'utm_source') AS utm_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'utm_medium') AS utm_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'utm_campaign') AS utm_campaign
  FROM
    `your-project-id.analytics_1234567890.events_*` -- Replace with your project ID
  WHERE
    event_name IN ('session_start', 'page_view', 'click') -- Interactions to track
    AND DATE(event_timestamp) BETWEEN DATE(start_date) AND DATE(end_date)
),

-- Conversion events (e.g., purchase, transaction)
conversions AS (
  SELECT
    user_pseudo_id, -- Unique user identifier
    event_timestamp AS conversion_timestamp, -- Timestamp of conversion event
    (SELECT IFNULL(value.int_value, 0) FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT IFNULL(value.int_value, 0) FROM UNNEST(event_params) WHERE key = 'value') +
    (SELECT IFNULL(value.float_value, 0.0) FROM UNNEST(event_params) WHERE key = 'value') +
    (SELECT IFNULL(value.double_value, 0.0) FROM UNNEST(event_params) WHERE key = 'value') AS purchase_value
  FROM
    `your-project-id.analytics_1234567890.events_*` -- Replace with your project ID
  WHERE
    event_name = 'purchase' -- Considering purchase as the conversion event
    AND DATE(event_timestamp) BETWEEN DATE(start_date) AND DATE(end_date)
),

-- Apply Time Decay to interactions based on their timestamp relative to the conversion timestamp
attribution AS (
  SELECT
    i.user_pseudo_id,
    i.utm_source,
    i.utm_medium,
    i.utm_campaign,
    i.event_timestamp AS interaction_timestamp,
    c.conversion_timestamp,
    -- Time difference in hours between the interaction and the conversion
    TIMESTAMP_DIFF(c.conversion_timestamp, i.event_timestamp, HOUR) AS hours_to_conversion,
    -- Decay function: decay = (1 / (1 + decay_factor * hours_to_conversion))
    POWER(0.5, TIMESTAMP_DIFF(c.conversion_timestamp, i.event_timestamp, HOUR) / 24) AS decay_factor,
    -- Credit assignment based on time decay
    (purchase_value * POWER(0.5, TIMESTAMP_DIFF(c.conversion_timestamp, i.event_timestamp, HOUR) / 24)) AS attributed_value
  FROM
    interactions i
  JOIN
    conversions c
  ON
    i.user_pseudo_id = c.user_pseudo_id -- Matching user ID
  WHERE
    c.purchase_value > 0 -- Only considering users who made a purchase
)

-- Final aggregation of attributed values by each touchpoint
SELECT
  utm_source,
  utm_medium,
  utm_campaign,
  SUM(attributed_value) AS total_attributed_value
FROM
  attribution
GROUP BY
  utm_source,
  utm_medium,
  utm_campaign
ORDER BY
  total_attributed_value DESC; -- Sorting by total attributed value in descending order
