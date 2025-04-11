WITH user_loyalty AS (
  SELECT 
    user_pseudo_id, 
    MAX(CASE 
        WHEN event_name = 'purchase' THEN event_bundle_sequence_id 
        ELSE NULL 
    END) AS last_purchase_event,
    MAX(CASE 
        WHEN event_name = 'loyalty_tier_update' THEN event_params.value.string_value 
        ELSE NULL 
    END) AS membership_tier
  FROM `your_project_id.analytics_YYYYMMDD`  -- Replace with your GA4 table name
  WHERE event_name IN ('purchase', 'loyalty_tier_update')
  GROUP BY user_pseudo_id
)

SELECT 
  u.membership_tier,
  COUNT(DISTINCT u.user_pseudo_id) AS total_members,
  COUNT(purchase_events.user_pseudo_id) AS total_purchases,
  ROUND(COUNT(purchase_events.user_pseudo_id) / COUNT(DISTINCT u.user_pseudo_id), 2) AS avg_purchases_per_user
FROM user_loyalty u
LEFT JOIN (
  SELECT user_pseudo_id FROM `your_project_id.analytics_YYYYMMDD`
  WHERE event_name = 'purchase'
) purchase_events 
ON u.user_pseudo_id = purchase_events.user_pseudo_id
GROUP BY u.membership_tier
ORDER BY total_purchases DESC;
