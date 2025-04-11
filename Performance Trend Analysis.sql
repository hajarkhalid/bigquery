WITH DailyMetrics AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        COUNT(DISTINCT CONCAT(user_pseudo_id, '-', 
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'))) AS total_sessions,
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) AS purchasing_users,
        SUM(CASE WHEN event_name = 'purchase' THEN (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') END) AS total_revenue
    FROM 
        `project.dataset.events_*`
    WHERE 
        _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', '20240101') AND FORMAT_DATE('%Y%m%d', '20241231')
    GROUP BY event_date
),

TrendAnalysis AS (
    SELECT
        event_date,
        total_sessions,
        purchasing_users,
        total_revenue,

        -- Previous day's sessions for trend comparison
        LAG(total_sessions) OVER (ORDER BY event_date) AS previous_sessions,

        -- 7-day moving average for smoothing trends
        ROUND(AVG(total_sessions) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS avg_sessions_7d,

        -- Trend indicator: Increase, Decrease, or Stable
        CASE 
            WHEN total_sessions > LAG(total_sessions) OVER (ORDER BY event_date) THEN 'Increase'
            WHEN total_sessions < LAG(total_sessions) OVER (ORDER BY event_date) THEN 'Decrease'
            ELSE 'Stable'
        END AS session_trend
    FROM DailyMetrics
)

-- Final Output
SELECT 
    event_date,
    total_sessions,
    previous_sessions,
    avg_sessions_7d,
    session_trend,
    purchasing_users,
    total_revenue
FROM TrendAnalysis
ORDER BY event_date;
