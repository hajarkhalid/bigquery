-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';  -- Start of 2024
DECLARE end_date STRING DEFAULT '2024-12-31';    -- End of 2024

WITH ConsentFactors AS (
    -- Define the adjustment factors for consented and non-consented users
    SELECT 
        2.0 AS factor_value, 'Granted' AS consent_state -- Consented users (estimated lower event frequency)
    UNION ALL
    SELECT 
        3.0 AS factor_value, 'Denied' AS consent_state  -- Non-consented users (estimated higher event frequency due to repeated events)
),

EventData AS (
    -- Extract relevant event data, filter by 'page_view' event and the specified date range
    SELECT
        -- Determine consent state based on user_pseudo_id (can be adjusted based on actual consent info)
        IF(e.user_pseudo_id IS NOT NULL, 'Granted', 'Denied') AS consent_state,
        COUNT(1) AS total_events, -- Count of total events for the 'page_view' event
        COUNT(DISTINCT e.user_pseudo_id) AS distinct_users -- Distinct count of users
    FROM 
        `project.dataset.events_*` AS e  -- Replace with your actual project and dataset
    WHERE
        e.event_name = 'page_view'  -- Focus on 'page_view' events for analysis
        AND e.event_date BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')  -- Filter by the 2024 date range
    GROUP BY 
        consent_state
),

ConsentAnalysis AS (
    -- Join event data with consent factors to calculate estimated user count based on the event factor
    SELECT
        ed.consent_state,
        ed.total_events,
        ed.distinct_users,
        CAST(ROUND(ed.total_events / cf.factor_value) AS INT64) AS estimated_users -- Estimate user count by dividing events by factor
    FROM 
        EventData ed
    LEFT JOIN 
        ConsentFactors cf 
    ON 
        ed.consent_state = cf.consent_state  -- Join on consent state
)

-- Final output: Show total events, distinct users, estimated users, and share of events by consent state
SELECT
    consent_state,
    total_events,
    distinct_users,
    estimated_users,
    ROUND(SAFE_DIVIDE(total_events, SUM(total_events) OVER()), 4) AS event_share -- Calculate the share of events per consent state
FROM 
    ConsentAnalysis
ORDER BY 
    total_events DESC;  -- Order by total events for clearer insight
