DECLARE start_date DATE DEFAULT '2024-01-01'; -- Set the start date
DECLARE end_date DATE DEFAULT '2024-12-31'; -- Set the end date

WITH DailyMetrics AS (
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
)

-- Final output: Display date and correlation value
SELECT 
  data_date,
  ROUND(ctr_position_correlation, 4) AS ctr_position_correlation -- Rounded to 4 decimal places for precision
FROM 
  DailyCorrelation
ORDER BY 
  data_date; -- Order by date for chronological output
