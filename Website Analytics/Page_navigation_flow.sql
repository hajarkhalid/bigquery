-- Declare date range variables for filtering
DECLARE start_date DATE DEFAULT DATE '2024-01-01';
DECLARE end_date DATE DEFAULT DATE '2024-12-31';

WITH page_view_data AS (
  -- Extract page view data with previous and next page navigation details
  SELECT
    -- Unique session identifier combining user ID and session ID
    CONCAT(
      user_pseudo_id, '-', 
      (SELECT value.int_value 
       FROM UNNEST(event_params) 
       WHERE key = 'ga_session_id') 
    ) AS session_id,
    user_pseudo_id,
    -- Extract normalized page path (remove domain and query parameters)
    REGEXP_REPLACE(
      (SELECT value.string_value 
       FROM UNNEST(event_params) 
       WHERE key = 'page_location'),
      r'^https?://[^/]+|[\?].*', '' -- Remove domain and query parameters in one step
    ) AS page_path,
    event_timestamp
  FROM
    `project.dataset.events_*`
  WHERE
    event_name = 'page_view'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
), 

page_navigation AS (
  -- Use LAG and LEAD functions to derive navigation flow
  SELECT
    session_id,
    user_pseudo_id,
    -- Previous page
    LAG(page_path) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) AS previous_page,
    -- Current page
    page_path AS current_page,
    -- Next page
    LEAD(page_path) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) AS next_page
  FROM 
    page_view_data
)

-- Aggregate navigation patterns and count occurrences
SELECT 
  COALESCE(previous_page, '(entrance)') AS previous_page, 
  current_page, 
  COALESCE(next_page, '(exit)') AS next_page, 
  COUNT(DISTINCT session_id) AS session_count
FROM 
  page_navigation
GROUP BY 
  previous_page, current_page, next_page
ORDER BY 
  session_count DESC;
