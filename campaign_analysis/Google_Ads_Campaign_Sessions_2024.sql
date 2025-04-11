DECLARE start_date DATE DEFAULT '2024-01-01';  -- Start date for filtering
DECLARE end_date DATE DEFAULT '2024-12-31';    -- End date for filtering

WITH SessionData AS (
    -- Calculate total sessions for each Google Ads campaign and account
    SELECT
        session_traffic_source_last_click.google_ads_campaign.account_name AS google_ads_account_name,  -- Google Ads account name
        session_traffic_source_last_click.google_ads_campaign.campaign_name AS google_ads_campaign_name, -- Campaign name
        COUNT(DISTINCT CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'))) AS session_count -- Total sessions
    FROM
        `your-project-id.analytics_1234567890.events_*` -- Replace with your project ID
    WHERE
        session_traffic_source_last_click.google_ads_campaign.account_name IS NOT NULL -- Filter valid account names
        AND session_traffic_source_last_click.google_ads_campaign.campaign_name IS NOT NULL -- Filter valid campaign names
        AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Date range filter
    GROUP BY
        google_ads_account_name, google_ads_campaign_name
)
ConversionData AS (
    -- Calculate total conversions and revenue per campaign and device
    SELECT
        session_traffic_source_last_click.google_ads_campaign.account_name AS google_ads_account_name,
        session_traffic_source_last_click.google_ads_campaign.campaign_name AS google_ads_campaign_name,
        device.category AS device_type,
        COUNTIF(event_name = 'purchase') AS total_purchases, -- Number of purchases
        SUM((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'purchase_revenue')) AS total_revenue -- Total revenue from purchases
    FROM 
        `your-project-id.analytics_1234567890.events_*`
    WHERE 
        session_traffic_source_last_click.google_ads_campaign.account_name IS NOT NULL 
        AND session_traffic_source_last_click.google_ads_campaign.campaign_name IS NOT NULL 
        AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') 
    GROUP BY 
        google_ads_account_name, google_ads_campaign_name, device_type)

-- Final Output: Sessions, Conversions, and Revenue per Campaign and Device Type
SELECT 
    s.google_ads_account_name,
    s.google_ads_campaign_name,
    s.device_type,
    s.session_count,
    COALESCE(c.total_purchases, 0) AS total_purchases,
    COALESCE(c.total_revenue, 0) AS total_revenue
FROM 
    SessionData s
LEFT JOIN 
    ConversionData c 
ON 
    s.google_ads_account_name = c.google_ads_account_name
    AND s.google_ads_campaign_name = c.google_ads_campaign_name
    AND s.device_type = c.device_type
ORDER BY 
    s.session_count DESC; -- Sort by highest session count
