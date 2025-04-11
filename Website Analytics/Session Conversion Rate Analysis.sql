-- Declare date range variables for filtering
DECLARE start_date DATE DEFAULT '2024-01-01';
DECLARE end_date DATE DEFAULT '2024-12-31';

WITH session_data AS (
  -- Extract session ID and event date
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    CONCAT(user_pseudo_id, '-', 
        CAST((SELECT value.int_value FROM event_params WHERE key = 'ga_session_id' LIMIT 1) AS STRING)) AS session_id
  FROM 
    `project.dataset.events_*` -- Replace with your project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) 
                        AND FORMAT_DATE('%Y%m%d', end_date) -- Optimized partition pruning
),

session_counts AS (
  -- Count total sessions per event date
  SELECT
    event_date,
    COUNT(DISTINCT session_id) AS total_sessions
  FROM 
    session_data
  GROUP BY 
    event_date
),

converted_sessions AS (
  -- Count converted sessions (purchase event) per event date
  SELECT
    event_date,
    COUNT(DISTINCT session_id) AS converted_sessions
  FROM 
    session_data
  WHERE 
    EXISTS (SELECT 1 FROM `project.dataset.events_*` e 
            WHERE e.event_name = 'purchase' 
            AND e.event_date = FORMAT_DATE('%Y%m%d', session_data.event_date)
            AND e.user_pseudo_id = SPLIT(session_id, '-')[SAFE_OFFSET(0)])
  GROUP BY 
    event_date
)

-- Calculate session conversion rate by date
SELECT
  sc.event_date,
  sc.total_sessions,
  COALESCE(cs.converted_sessions, 0) AS converted_sessions,
  ROUND(SAFE_DIVIDE(cs.converted_sessions, sc.total_sessions) * 100, 3) AS session_conversion_rate
FROM 
  session_counts sc
LEFT JOIN 
  converted_sessions cs
ON 
  sc.event_date = cs.event_date
ORDER BY 
  sc.event_date ASC;
