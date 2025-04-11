SELECT
-- group by traffic sources
    user_pseudo_id,
    traffic_source.source,
    CASE 
        WHEN traffic_source.source IN ('google', 'bing', 'yahoo') THEN 'Search'
        WHEN traffic_source.source IN ('facebook', 'instagram', 'twitter') THEN 'Social'
        WHEN traffic_source.source = 'direct' THEN 'Direct'
        ELSE 'Other'
    END AS traffic_category
FROM `your_project.dataset.events_*`

SELECT 
-- group by engagement

    user_pseudo_id,
    session_id,
    CASE 
        WHEN session_engaged = '1' THEN 'Engaged'
        WHEN session_engaged = '0' THEN 'Not Engaged'
        ELSE 'Unknown'
    END AS session_status
FROM `your_project.dataset.events_*`
