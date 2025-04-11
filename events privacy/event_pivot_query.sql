-- File Name: event_pivot_query.sql
-- Description: Query to pivot event data and count occurrences for specified event types by user
-- Author: [Your Name]
-- Date: [Insert Date]

-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

SELECT
    *
FROM (
    SELECT
        user_pseudo_id, -- Unique identifier for users
        event_name      -- Name of the event
    FROM 
        `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` -- Using a public dataset for testing
    WHERE 
        _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Filter by date range
)
PIVOT (
    COUNT(*) -- Count occurrences of each event type
    FOR event_name IN (
        'session_start', 
        'page_view', 
        'signup_success', 
        'login_success', 
        'whitelabel_final_step', 
        'upgrade', 
        'cancellation', 
        'purchase', 
        'contact_telegram'
    )
)
ORDER BY 
    user_pseudo_id; -- Sort results by user ID
