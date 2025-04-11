DECLARE start_date DATE DEFAULT '2024-01-01'; -- Start date for filtering
DECLARE end_date DATE DEFAULT '2024-12-31'; -- End date for filtering

WITH CampaignData AS (
  SELECT
    session_traffic_source_last_click.google_ads_campaign.campaign_name AS google_ads_campaign, -- Google Ads Campaign Name
    session_traffic_source_last_click.cross_channel_campaign.primary_channel_group AS primary_channel, -- Primary Channel Group
    COUNT(DISTINCT user_pseudo_id) AS unique_users, -- Count of Unique Users
    SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue -- Total Revenue from Purchases
  FROM
    `your-project-id.analytics_1234567890.events_*` -- Replace with your actual project and dataset
  WHERE
    session_traffic_source_last_click.google_ads_campaign.campaign_name IS NOT NULL -- Ensure campaign name is valid
    AND ecommerce.purchase_revenue_in_usd IS NOT NULL -- Filter out records with no revenue
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Date filtering
  GROUP BY
    google_ads_campaign, primary_channel
)

SELECT
  google_ads_campaign,
  primary_channel,
  unique_users,
  total_revenue,
  SAFE_DIVIDE(total_revenue, unique_users) AS revenue_per_user -- Calculate revenue per unique user
FROM
  CampaignData
ORDER BY
  total_revenue DESC;  -- Order by total revenue in descending order
