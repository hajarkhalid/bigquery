DECLARE project_id STRING DEFAULT 'your_project';
DECLARE dataset_id STRING DEFAULT 'your_dataset';
DECLARE table_prefix STRING DEFAULT 'web_analytics_';
DECLARE table_schema STRING DEFAULT '''
  user_id INT64,
  session_id STRING,
  event_name STRING,
  event_date DATE,
  event_timestamp TIMESTAMP
''';

-- Get the last 12 months (formatted as YYYYMM)
FOR month_offset IN (SELECT * FROM UNNEST(GENERATE_ARRAY(0, 11))) DO
  DECLARE table_suffix STRING;
  SET table_suffix = FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL month_offset MONTH));

  -- Construct the table name dynamically
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `%s.%s.%s%s`
    (
      %s
    )
    PARTITION BY event_date;
  """, project_id, dataset_id, table_prefix, table_suffix, table_schema);
END FOR;
