-- File Name: measurement_protocol_event_analysis.sql
-- Description: This query analyzes suspected measurement protocol events in GA4 data.
-- Note: Replace `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` with your desired dataset.

-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH event_data AS (
  -- Extract essential fields for analysis
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date, -- Convert event_date to DATE format
    user_pseudo_id,
    device.language,
    device.operating_system,
    device.category
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` -- Public dataset
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
),

measurement_protocol_analysis AS (
  -- Calculate the ratio of suspected measurement protocol events
  SELECT
    event_date,
    -- Calculate the ratio of events with null device details and desktop category
    ROUND(
      SAFE_DIVIDE(
        COUNTIF(
          language IS NULL 
          AND operating_system IS NULL 
          AND category = "desktop"
        ),
        COUNT(*)
      ),
      4
    ) AS measurement_protocol_event_ratio
  FROM 
    event_data
  GROUP BY 
    event_date
)

-- Final result: Output daily measurement protocol event ratios
SELECT
  event_date,
  measurement_protocol_event_ratio
FROM 
  measurement_protocol_analysis
ORDER BY 
  event_date ASC;
