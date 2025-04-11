-- Declare date range variables for dynamic filtering
DECLARE start_date DATE DEFAULT '2024-01-01';
DECLARE end_date DATE DEFAULT '2024-12-31';

WITH traffic_data AS ( 
  -- Extract session-level attributes
  SELECT
    -- Unique session identifier combining user ID and session ID
    CONCAT(user_pseudo_id, '-', 
        (SELECT value.int_value FROM event_params WHERE key = 'ga_session_id' LIMIT 1)) AS session_id,
    -- Traffic source attributes
    collected_traffic_source.manual_source AS session_source,
    collected_traffic_source.manual_medium AS session_medium,
    collected_traffic_source.manual_campaign_name AS session_campaign_name,
    collected_traffic_source.gclid
  FROM
    `project.dataset.events_*` -- Replace with your project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) 
                        AND FORMAT_DATE('%Y%m%d', end_date) -- Efficient partition pruning
),

channel_grouping AS (
  -- Assign channel groupings based on session attributes
  SELECT
    session_id,
    CASE
      WHEN session_source IS NULL OR session_medium IS NULL THEN "Direct"
      WHEN LOWER(session_campaign_name) LIKE "%cross-network%" THEN "Cross-network"
      
      -- Paid Shopping
      WHEN REGEXP_CONTAINS(LOWER(session_source), r"(alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart)")
           AND REGEXP_CONTAINS(LOWER(session_medium), r"(cp.*|ppc|paid.*)") THEN "Paid Shopping"

      -- Paid Search
      WHEN gclid IS NOT NULL 
           OR (REGEXP_CONTAINS(LOWER(session_source), r"(baidu|bing|duckduckgo|ecosia|google|yahoo|yandex)") 
           AND REGEXP_CONTAINS(LOWER(session_medium), r"(cp.*|ppc|paid.*)")) THEN "Paid Search"

      -- Paid Social
      WHEN REGEXP_CONTAINS(LOWER(session_source), r"(badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)")
           AND REGEXP_CONTAINS(LOWER(session_medium), r"(cp.*|ppc|paid.*)") THEN "Paid Social"

      -- Paid Video
      WHEN REGEXP_CONTAINS(LOWER(session_source), r"(dailymotion|disneyplus|netflix|youtube|vimeo|twitch)") 
           AND REGEXP_CONTAINS(LOWER(session_medium), r"(cp.*|ppc|paid.*)") THEN "Paid Video"

      -- Display
      WHEN LOWER(session_medium) IN ("display", "banner", "expandable", "interstitial", "cpm") THEN "Display"

      -- Organic Shopping
      WHEN REGEXP_CONTAINS(LOWER(session_source), r"(alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart)") THEN "Organic Shopping"

      -- Organic Social
      WHEN REGEXP_CONTAINS(LOWER(session_source), r"(badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)")
           OR LOWER(session_medium) IN ("social", "social-network", "social-media", "sm", "social network", "social media") THEN "Organic Social"

      -- Organic Video
      WHEN REGEXP_CONTAINS(LOWER(session_source), r"(dailymotion|disneyplus|netflix|youtube|vimeo|twitch)")
           OR LOWER(session_medium) LIKE "%video%" THEN "Organic Video"

      -- Organic Search
      WHEN REGEXP_CONTAINS(LOWER(session_source), r"(baidu|bing|duckduckgo|ecosia|google|yahoo|yandex)") 
           OR LOWER(session_medium) = "organic" THEN "Organic Search"

      -- Email
      WHEN REGEXP_CONTAINS(LOWER(session_source), r"(email|e-mail|e_mail|e mail)")
           OR REGEXP_CONTAINS(LOWER(session_medium), r"(email|e-mail|e_mail|e mail)") THEN "Email"

      -- Other Channels
      WHEN LOWER(session_medium) = "affiliate" THEN "Affiliates"
      WHEN LOWER(session_medium) = "referral" THEN "Referral"
      WHEN LOWER(session_medium) = "audio" THEN "Audio"
      WHEN LOWER(session_medium) = "sms" THEN "SMS"

      -- Push Notifications (Consolidated)
      WHEN LOWER(session_medium) LIKE "%push%" 
           OR REGEXP_CONTAINS(LOWER(session_medium), r"(mobile|notification)") THEN "Push Notifications"

      ELSE "Unassigned"
    END AS channel_grouping
  FROM
    traffic_data
)

SELECT
  channel_grouping,
  COUNT(DISTINCT session_id) AS session_count, -- Count unique sessions per channel grouping
  ROUND(100 * COUNT(DISTINCT session_id) / SUM(COUNT(DISTINCT session_id)) OVER (), 2) AS session_percentage -- % of total
FROM
  channel_grouping
GROUP BY
  channel_grouping
ORDER BY
  session_count DESC;
