-- Declare date range variables to filter data for the year 2024
DECLARE start_date DATE DEFAULT DATE '2024-01-01';
DECLARE end_date DATE DEFAULT DATE '2024-12-31';

WITH purchase_sessions AS (
    -- Extract session IDs for purchase events
    SELECT 
        event_timestamp AS purchase_timestamp,
        CONCAT(user_pseudo_id, '-', 
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
        ) AS session_id
    FROM `project.dataset.events_*`
    WHERE event_name = 'purchase'
      AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
),

first_pageview_in_session AS (
    -- Identify the first page view per session
    SELECT DISTINCT
        session_id,
        page_location AS landing_page
    FROM (
        SELECT 
            CONCAT(user_pseudo_id, '-', 
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
            ) AS session_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
            event_timestamp,
            ROW_NUMBER() OVER (PARTITION BY CONCAT(user_pseudo_id, '-', 
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) 
                ORDER BY event_timestamp ASC) AS rn
        FROM `project.dataset.events_*`
        WHERE event_name = 'page_view'
          AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
    )
    WHERE rn = 1 -- Keep only the first page view per session
)

-- Aggregate purchases by landing page
SELECT
    fp.landing_page,
    COUNT(ps.session_id) AS total_purchases
FROM purchase_sessions ps
JOIN first_pageview_in_session fp
  ON ps.session_id = fp.session_id
WHERE fp.landing_page IS NOT NULL
GROUP BY fp.landing_page
ORDER BY total_purchases DESC;
