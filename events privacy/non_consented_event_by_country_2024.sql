-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';  -- Start date for the query
DECLARE end_date STRING DEFAULT '2024-12-31';    -- End date for the query

-- Query to count events from non-consented users and identify countries with the most events
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS date_formatted,  -- Convert event_date to a proper date format
  geo.country AS country,  -- Get the country of the event
  COUNT(*) AS total_events  -- Count the total number of events
FROM
  `project.dataset.events_*`  -- Replace with your actual project and dataset
WHERE 
  privacy_info.analytics_storage = 'No'  -- Filter for users who did not consent to analytics storage
  AND privacy_info.ads_storage = 'No'  -- Filter for users who did not consent to ads storage
  AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')  -- Filter by the specified date range
GROUP BY 
  date_formatted, country  -- Group by date and country
ORDER BY 
  total_events DESC;  -- Order by total events in descending order for easier analysis
