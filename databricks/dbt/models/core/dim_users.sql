{{ config(materialized = 'table') }}

-- SCD Type 2 dimension for users.
-- The level column (free / paid) is tracked as a slowly changing dimension.
-- Original BigQuery syntax replaced with Databricks SQL equivalents:
--   - CAST(... AS DATE) is the same
--   - DATE '9999-12-31' literal is the same
--   - LEAD / RANK window functions are ANSI SQL and work identically

SELECT {{ dbt_utils.generate_surrogate_key(['userId', 'rowActivationDate', 'level']) }} as userKey, *
FROM
(
SELECT CAST(userId AS BIGINT) as userId, firstName, lastName, gender, level, CAST(registration as BIGINT) as registration, minDate as rowActivationDate,
LEAD(minDate, 1, CAST('9999-12-31' AS DATE)) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped) as rowExpirationDate,
CASE WHEN RANK() OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped desc) = 1 THEN 1 ELSE 0 END AS currentRow
FROM
(
SELECT userId, firstName, lastName, gender, registration, level, grouped, cast(min(date) as date) as minDate
FROM
(SELECT *, SUM(lagged) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date) as grouped
FROM
(SELECT *, CASE WHEN LAG(level, 1, 'NA') OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date) <> level THEN 1 ELSE 0 END AS lagged
from
(SELECT  distinct userId
       ,firstName
       ,lastName
       ,gender
       ,registration
       ,level
       ,ts AS date
FROM {{ source('staging', 'listen_events') }}
WHERE userId <> 0
)
)
)
GROUP BY userId, firstName, lastName, gender, registration, level, grouped
)

UNION ALL

SELECT CAST(userId as BIGINT) as userKey, firstName, lastName, gender, level, CAST(registration as BIGINT) as registration, CAST(min(ts) as date) as rowActivationDate, CAST('9999-12-31' AS DATE) as rowExpirationDate, 1 as currentRow
FROM {{ source('staging', 'listen_events') }}
WHERE userId = 0 or userId = 1
GROUP BY userId, firstName, lastName, gender, level, registration

)
