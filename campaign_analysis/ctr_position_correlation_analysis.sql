DECLARE start_date DATE DEFAULT '2024-01-01'; -- Set start date
DECLARE end_date DATE DEFAULT '2024-12-31'; -- Set end date

WITH DateRange AS (
  -- Generate all dates within the date range to handle missing dates
  SELECT DATE_ADD(start_date, INTERVAL x DAY) AS date
  FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS x
),

DailyMetrics AS (
  -- Calculate CTR and average position for each query and day
  SELECT
    data_date,
    query,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr, -- Compute CTR safely
    SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) + 1.0 AS avg_position -- Calculate average position
  FROM 
    `your-project-id.your-dataset-id.searchdata_site_impression` -- Replace with your project and dataset
  WHERE
    impressions > 0 
    AND sum_top_position < 20 -- Only consider valid top positions
    AND data_date BETWEEN start_date AND end_date -- Apply date filter
  GROUP BY 
    data_date, query
),

DailyCorrelation AS (
  -- Calculate correlation between CTR and average position for each day
  SELECT
    data_date,
    CORR(ctr, avg_position) AS ctr_position_correlation -- Correlation between CTR and avg position
  FROM
    DailyMetrics
  GROUP BY
    data_date
),

-- Handle missing dates by joining with the DateRange
FilledDailyCorrelation AS (
  SELECT
    d.date AS data_date,
    IFNULL(dc.ctr_position_correlation, 0) AS ctr_position_correlation -- Fill missing dates with 0
  FROM 
    DateRange d
  LEFT JOIN 
    DailyCorrelation dc 
  ON d.date = dc.data_date
),

-- Calculate 7-day moving average of correlation
MovingAverage AS (
  SELECT 
    data_date,
    ROUND(ctr_position_correlation, 4) AS ctr_position_correlation,
    -- 7-day moving average
    ROUND(AVG(ctr_position_correlation) 
          OVER (ORDER BY data_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 4) 
          AS moving_avg_7d,
    -- 14-day moving average
    ROUND(AVG(ctr_position_correlation) 
          OVER (ORDER BY data_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 4) 
          AS moving_avg_14d,
    -- 30-day moving average
    ROUND(AVG(ctr_position_correlation) 
          OVER (ORDER BY data_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 4) 
          AS moving_avg_30d
  FROM 
    FilledDailyCorrelation
)


-- Aggregate monthly average correlation
MonthlyCorrelation AS (
  SELECT 
    EXTRACT(MONTH FROM data_date) AS month,
    ROUND(AVG(ctr_position_correlation), 4) AS avg_monthly_corr
  FROM 
    FilledDailyCorrelation
  GROUP BY 
    month
)

-- Final output: Daily correlation, moving average, and monthly averages
SELECT 
  m.data_date,
  m.ctr_position_correlation,
  m.moving_avg_7d,
  m.moving_avg_14d,
  m.moving_avg_30d,
  mc.avg_monthly_corr
FROM 
  MovingAverage m
LEFT JOIN 
  MonthlyCorrelation mc 
ON EXTRACT(MONTH FROM m.data_date) = mc.month
ORDER BY 
  m.data_date;
