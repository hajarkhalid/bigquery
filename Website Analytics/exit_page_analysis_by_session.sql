-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH sessions_pages AS (
  -- Extract page view data for each session
  SELECT
    user_pseudo_id AS cid, 
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page,
    event_timestamp
  -- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE event_name = 'page_view'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
),

exit_pages AS (
  -- Identify the exit page for each session by ordering events by timestamp
  SELECT
    cid,
    session_id,
    FIRST_VALUE(page) OVER (PARTITION BY cid, session_id ORDER BY event_timestamp DESC) AS exit_page
  FROM sessions_pages
),

exit_page_aggregates AS (
  -- Aggregate the occurrences of each exit page by session count
  SELECT
    exit_page,
    COUNT(DISTINCT CONCAT(cid, session_id)) AS session_count
  FROM exit_pages
  WHERE exit_page IS NOT NULL -- Exclude sessions without a valid exit page
  GROUP BY exit_page
)

-- Final output: Display the exit page with the number of sessions
SELECT
  exit_page,
  session_count
FROM exit_page_aggregates
ORDER BY session_count DESC;
