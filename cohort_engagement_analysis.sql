-- Declare date range variables for filtering
DECLARE start_date DATE DEFAULT '2024-01-01';
DECLARE end_date DATE DEFAULT '2024-12-31';

WITH session_data AS (
  -- Extract session-related data and calculate engagement metrics
  SELECT
    -- Unique session identifier combining user ID and session ID
    CONCAT(user_pseudo_id, '-', 
           (SELECT value.int_value FROM event_params WHERE key = 'ga_session_id' LIMIT 1)) AS session_id,
    user_pseudo_id,
    -- Session number
    (SELECT value.int_value FROM event_params WHERE key = 'ga_session_number' LIMIT 1) AS session_number,
    -- Maximum engagement time in milliseconds per session
    MAX((SELECT value.int_value FROM event_params WHERE key = 'engagement_time_msec' LIMIT 1)) AS engagement_time_msec,
    -- Convert event_date to DATE format
    PARSE_DATE('%Y%m%d', event_date) AS session_date,
    -- First session date per user
    FIRST_VALUE(PARSE_DATE('%Y%m%d', event_date)) 
      OVER (PARTITION BY user_pseudo_id ORDER BY event_date) AS first_session_date
  FROM
    `project.dataset.events_*` -- Replace with your GA4 project and dataset
  WHERE
    -- Efficient filtering using _TABLE_SUFFIX (ensures BigQuery scans only relevant tables)
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) 
                        AND FORMAT_DATE('%Y%m%d', end_date)
  GROUP BY
    user_pseudo_id, session_id, session_number, event_date
)

-- Calculate week-by-week engagement and retention rate
SELECT
  -- Year-week of first session (Cohort grouping)
  CONCAT(EXTRACT(ISOYEAR FROM first_session_date), '-', FORMAT('%02d', EXTRACT(ISOWEEK FROM first_session_date))) AS year_week,
  
  -- Total users in cohort
  COUNT(DISTINCT user_pseudo_id) AS cohort_size,

  -- Weekly retention (users who return in each subsequent week)
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 0 
    THEN user_pseudo_id END) AS week_0,

  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 1 
    THEN user_pseudo_id END) AS week_1,
  
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 2 
    THEN user_pseudo_id END) AS week_2,

  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 3 
    THEN user_pseudo_id END) AS week_3,

  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 4 
    THEN user_pseudo_id END) AS week_4,

  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 5 
    THEN user_pseudo_id END) AS week_5,

  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 6 
    THEN user_pseudo_id END) AS week_6,

  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 7 
    THEN user_pseudo_id END) AS week_7,

  -- Retention rate as percentage
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 1 THEN user_pseudo_id END), 
              COUNT(DISTINCT user_pseudo_id)) * 100 AS week_1_retention_rate,

  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 2 THEN user_pseudo_id END), 
              COUNT(DISTINCT user_pseudo_id)) * 100 AS week_2_retention_rate,

  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 3 THEN user_pseudo_id END), 
              COUNT(DISTINCT user_pseudo_id)) * 100 AS week_3_retention_rate,

  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 4 THEN user_pseudo_id END), 
              COUNT(DISTINCT user_pseudo_id)) * 100 AS week_4_retention_rate

FROM session_data
GROUP BY year_week
ORDER BY year_week;
