{{ config(
  materialized = 'table'
  ) }}

-- Original BigQuery version used partition_by on timestamp with hourly
-- granularity. Delta Lake does not support declarative partitioning in
-- the same way; instead we rely on Delta's data-skipping and Z-ORDER
-- optimisations. See MIGRATION_PLAN.md for the rationale.
--
-- BigQuery's date_trunc(ts, HOUR) → Databricks' date_trunc('HOUR', ts)

SELECT
    dim_users.userKey AS userKey,
    dim_artists.artistKey AS artistKey,
    dim_songs.songKey AS songKey ,
    dim_datetime.dateKey AS dateKey,
    dim_location.locationKey AS locationKey,
    listen_events.ts AS ts
 FROM {{ source('staging', 'listen_events') }}
  LEFT JOIN {{ ref('dim_users') }}
    ON listen_events.userId = dim_users.userId AND CAST(listen_events.ts AS DATE) >= dim_users.rowActivationDate AND CAST(listen_events.ts AS DATE) < dim_users.RowExpirationDate
  LEFT JOIN {{ ref('dim_artists') }}
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_artists.name
  LEFT JOIN {{ ref('dim_songs') }}
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_songs.artistName AND listen_events.song = dim_songs.title
  LEFT JOIN {{ ref('dim_location') }}
    ON listen_events.city = dim_location.city AND listen_events.state = dim_location.stateCode AND listen_events.lat = dim_location.latitude AND listen_events.lon = dim_location.longitude
  LEFT JOIN {{ ref('dim_datetime') }}
    ON dim_datetime.date = date_trunc('HOUR', listen_events.ts)
