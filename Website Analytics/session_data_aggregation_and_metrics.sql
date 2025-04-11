-- Declare date range variables for filtering data
DECLARE start_date STRING DEFAULT '2024-01-01';  -- Replace with desired start date
DECLARE end_date STRING DEFAULT '2024-12-31';    -- Replace with desired end date

WITH LandingPageData AS (
    -- Extract relevant data for landing pages
    SELECT
        -- Clean and extract the page path without query parameters or fragments
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                (SELECT ep.value.string_value 
                 FROM UNNEST(e.event_params) AS ep 
                 WHERE ep.key = 'page_location'),
                r'\?.*$', '' -- Remove query parameters
            ),
            r'#.*$', '' 
        ) AS landing_page,
        e.user_pseudo_id,
        (SELECT ep.value.int_value FROM UNNEST(e.event_params) AS ep WHERE ep.key = 'entrances') AS is_landing_page,
        (SELECT ep.value.int_value FROM UNNEST(e.event_params) AS ep WHERE ep.key = 'engagement_time_msec') AS engagement_time_msec,
        (SELECT ep.value.string_value FROM UNNEST(e.event_params) AS ep WHERE ep.key = 'source') AS traffic_source,
        (SELECT ep.value.string_value FROM UNNEST(e.event_params) AS ep WHERE ep.key = 'medium') AS traffic_medium,
        (SELECT ep.value.int_value FROM UNNEST(e.event_params) AS ep WHERE ep.key = 'bounce') AS is_bounce
    FROM 
        `project.dataset.events_*` AS e  -- Replace with your project and dataset
    WHERE 
        e.event_name = 'page_view'
        AND e.user_pseudo_id IS NOT NULL
        AND e.event_date BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')  -- Filter by date range
),

AggregatedLandingPages AS (
    -- Aggregate landing page data
    SELECT
        landing_page,
        traffic_source,
        traffic_medium,
        COUNT(DISTINCT user_pseudo_id) AS unique_users, -- Count of unique users
        COUNT(*) AS page_views, -- Total page views
        SUM(is_landing_page) AS entrances, -- Total number of entrances
        ROUND(SAFE_DIVIDE(SUM(is_landing_page), COUNT(*)), 2) AS entrance_rate, -- Entrance rate
        ROUND(SAFE_DIVIDE(SUM(is_bounce), SUM(is_landing_page)), 2) AS bounce_rate, -- Bounce rate
        ROUND(AVG(engagement_time_msec) / 1000, 2) AS avg_engagement_time_sec -- Average engagement time in seconds
    FROM 
        LandingPageData
    WHERE 
        landing_page IS NOT NULL -- Exclude null paths
    GROUP BY 
        landing_page, traffic_source, traffic_medium
)

-- Retrieve the top 10 most popular landing pages
SELECT
    landing_page,
    traffic_source,
    traffic_medium,
    unique_users,
    page_views,
    entrances,
    entrance_rate,
    bounce_rate,
    avg_engagement_time_sec
FROM 
    AggregatedLandingPages
ORDER BY 
    unique_users DESC -- Rank by the number of unique users
LIMIT 10;
