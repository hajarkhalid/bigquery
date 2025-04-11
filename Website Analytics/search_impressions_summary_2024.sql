-- Declare date range variables for filtering
DECLARE start_date DATE DEFAULT '2024-01-01';  -- Set desired start date
DECLARE end_date DATE DEFAULT '2024-12-31';    -- Set desired end date

-- Boolean metric aggregation for search impressions
SELECT  
  SUM(COALESCE(SAFE_CAST(is_amp_top_stories AS INT64), 0)) AS is_amp_top_stories, -- AMP Top Stories
  SUM(COALESCE(SAFE_CAST(is_amp_blue_link AS INT64), 0)) AS is_amp_blue_link, -- AMP Blue Link
  SUM(COALESCE(SAFE_CAST(is_job_listing AS INT64), 0)) AS is_job_listing, -- Job Listings
  SUM(COALESCE(SAFE_CAST(is_job_details AS INT64), 0)) AS is_job_details, -- Job Details
  SUM(COALESCE(SAFE_CAST(is_tpf_qa AS INT64), 0)) AS is_tpf_qa, -- Third-party Q&A
  SUM(COALESCE(SAFE_CAST(is_tpf_faq AS INT64), 0)) AS is_tpf_faq, -- Third-party FAQ
  SUM(COALESCE(SAFE_CAST(is_tpf_howto AS INT64), 0)) AS is_tpf_howto, -- Third-party How-To
  SUM(COALESCE(SAFE_CAST(is_weblite AS INT64), 0)) AS is_weblite, -- Web Lite
  SUM(COALESCE(SAFE_CAST(is_action AS INT64), 0)) AS is_action, -- Action Items
  SUM(COALESCE(SAFE_CAST(is_events_listing AS INT64), 0)) AS is_events_listing, -- Event Listings
  SUM(COALESCE(SAFE_CAST(is_events_details AS INT64), 0)) AS is_events_details, -- Event Details
  SUM(COALESCE(SAFE_CAST(is_search_appearance_android_app AS INT64), 0)) AS is_search_appearance_android_app, -- Android App Search Appearance
  SUM(COALESCE(SAFE_CAST(is_amp_story AS INT64), 0)) AS is_amp_story, -- AMP Story
  SUM(COALESCE(SAFE_CAST(is_amp_image_result AS INT64), 0)) AS is_amp_image_result, -- AMP Image Results
  SUM(COALESCE(SAFE_CAST(is_video AS INT64), 0)) AS is_video, -- Video Results
  SUM(COALESCE(SAFE_CAST(is_organic_shopping AS INT64), 0)) AS is_organic_shopping, -- Organic Shopping
  SUM(COALESCE(SAFE_CAST(is_review_snippet AS INT64), 0)) AS is_review_snippet, -- Review Snippets
  SUM(COALESCE(SAFE_CAST(is_special_announcement AS INT64), 0)) AS is_special_announcement, -- Special Announcements
  SUM(COALESCE(SAFE_CAST(is_recipe_feature AS INT64), 0)) AS is_recipe_feature, -- Recipe Features
  SUM(COALESCE(SAFE_CAST(is_recipe_rich_snippet AS INT64), 0)) AS is_recipe_rich_snippet, -- Recipe Rich Snippets
  SUM(COALESCE(SAFE_CAST(is_subscribed_content AS INT64), 0)) AS is_subscribed_content, -- Subscribed Content
  SUM(COALESCE(SAFE_CAST(is_page_experience AS INT64), 0)) AS is_page_experience, -- Page Experience
  SUM(COALESCE(SAFE_CAST(is_practice_problems AS INT64), 0)) AS is_practice_problems, -- Practice Problems
  SUM(COALESCE(SAFE_CAST(is_math_solvers AS INT64), 0)) AS is_math_solvers, -- Math Solvers
  SUM(COALESCE(SAFE_CAST(is_translated_result AS INT64), 0)) AS is_translated_result, -- Translated Results
  SUM(COALESCE(SAFE_CAST(is_edu_q_and_a AS INT64), 0)) AS is_edu_q_and_a, -- Educational Q&A
  SUM(COALESCE(SAFE_CAST(is_product_snippets AS INT64), 0)) AS is_product_snippets, -- Product Snippets
  SUM(COALESCE(SAFE_CAST(is_merchant_listings AS INT64), 0)) AS is_merchant_listings, -- Merchant Listings
  SUM(COALESCE(SAFE_CAST(is_learning_videos AS INT64), 0)) AS is_learning_videos -- Learning Videos
FROM 
  `bigquery-public-data.google_search_data.searchdata_url_impression` -- Replace with your dataset
WHERE 
  DATE(data_date) BETWEEN start_date AND end_date; -- Filter based on declared date range
-- Declare date range variables
DECLARE start_date DATE DEFAULT '2024-01-01';
DECLARE end_date DATE DEFAULT '2024-12-31';

WITH 
Device_Country AS (
  -- Aggregate impressions by device type and country
  SELECT  
    'Device_Country' AS report_type,
    device_category AS category, 
    country AS segment,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(clicks, 0)) AS total_clicks,
    ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr -- CTR calculation
  FROM 
    `bigquery-public-data.google_search_data.searchdata_url_impression`
  WHERE 
    DATE(data_date) BETWEEN start_date AND end_date
  GROUP BY 
    device_category, country
),

Traffic_Source AS (
  -- Search performance by traffic source
  SELECT  
    'Traffic_Source' AS report_type,
    traffic_source AS category,
    NULL AS segment,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(clicks, 0)) AS total_clicks,
    ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr
  FROM 
    `bigquery-public-data.google_search_data.searchdata_url_impression`
  WHERE 
    DATE(data_date) BETWEEN start_date AND end_date
  GROUP BY 
    traffic_source
),

Daily_Trends AS (
  -- Daily trend analysis
  SELECT  
    'Daily_Trends' AS report_type,
    CAST(DATE(data_date) AS STRING) AS category, 
    NULL AS segment,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(clicks, 0)) AS total_clicks,
    ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr
  FROM 
    `bigquery-public-data.google_search_data.searchdata_url_impression`
  WHERE 
    DATE(data_date) BETWEEN start_date AND end_date
  GROUP BY 
    date
),

Search_Query_Performance AS (
  -- Search performance by query
  SELECT  
    'Search_Query_Performance' AS report_type,
    search_query AS category,
    NULL AS segment,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(clicks, 0)) AS total_clicks,
    ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr
  FROM 
    `bigquery-public-data.google_search_data.searchdata_url_impression`
  WHERE 
    DATE(data_date) BETWEEN start_date AND end_date
  GROUP BY 
    search_query
),

AMP_Performance AS (
  -- AMP & Rich Snippet Performance
  SELECT  
    'AMP_Performance' AS report_type,
    CASE 
      WHEN is_amp_top_stories THEN 'AMP Top Stories'
      WHEN is_amp_blue_link THEN 'AMP Blue Link'
      WHEN is_amp_story THEN 'AMP Story'
      WHEN is_amp_image_result THEN 'AMP Image Result'
      ELSE 'Non-AMP'
    END AS category,
    NULL AS segment,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(clicks, 0)) AS total_clicks,
    ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr
  FROM 
    `bigquery-public-data.google_search_data.searchdata_url_impression`
  WHERE 
    DATE(data_date) BETWEEN start_date AND end_date
  GROUP BY 
    category
),

Keyword_Performance AS (
  -- Keyword-level performance for promo-related terms
  SELECT  
    'Keyword_Performance' AS report_type,
    search_query AS category,
    NULL AS segment,
    SUM(COALESCE(impressions, 0)) AS total_impressions,
    SUM(COALESCE(clicks, 0)) AS total_clicks,
    ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 2) AS ctr
  FROM 
    `bigquery-public-data.google_search_data.searchdata_url_impression`
  WHERE 
    DATE(data_date) BETWEEN start_date AND end_date
    AND REGEXP_CONTAINS(LOWER(search_query), r'(?i)\b(promo|offer|sale|discount|deal|coupon)\b')
  GROUP BY 
    search_query
)

-- Final Combined Output
SELECT * FROM Device_Country
UNION ALL
SELECT * FROM Traffic_Source
UNION ALL
SELECT * FROM Daily_Trends
UNION ALL
SELECT * FROM Search_Query_Performance
UNION ALL
SELECT * FROM AMP_Performance
UNION ALL
SELECT * FROM Keyword_Performance
ORDER BY report_type, total_impressions DESC;
