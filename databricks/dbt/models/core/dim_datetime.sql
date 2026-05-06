{{ config(materialized = 'table') }}

-- Original used BigQuery's GENERATE_TIMESTAMP_ARRAY + UNNEST.
-- Databricks SQL uses sequence() to generate an array of timestamps,
-- then explode() to flatten it into rows.

WITH date_series AS
(
SELECT
  explode(
    sequence(
      CAST('2018-10-01' AS TIMESTAMP),
      CAST('2023-01-01' AS TIMESTAMP),
      INTERVAL 1 HOUR
    )
  ) AS date
)
SELECT
    UNIX_TIMESTAMP(date) AS dateKey,
    date,
    DAYOFWEEK(date) AS dayOfWeek,
    DAY(date) AS dayOfMonth,
    WEEKOFYEAR(date) AS weekOfYear,
    MONTH(date) AS month,
    YEAR(date) AS year,
    CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN True ELSE False END AS weekendFlag
FROM date_series
