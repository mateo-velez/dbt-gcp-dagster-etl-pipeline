{{ config(
  materialized = 'incremental',
  partition_by ={ 'field' :'ingestion_timestamp',
  'data_type' :'timestamp',
  'granularity' :'day' },
  incremental_strategy = 'insert_overwrite',
  post_hook=[
    "
      CREATE MATERIALIZED VIEW IF NOT EXISTS
      {{ '`' ~ [this.database, this.schema,'helper' ~ '__' ~ this.identifier]| join('.') ~ '`'}}
    OPTIONS (enable_refresh=true)
    AS (
      SELECT
        MAX(ingestion_timestamp) AS ingestion_timestamp
      FROM
        {{ this }}
      WHERE
        ingestion_timestamp IS NOT NULL );",
      
      "CALL BQ.REFRESH_MATERIALIZED_VIEW({{ \"'\" ~ [this.database, this.schema,'helper' ~ '__' ~ this.identifier]| join('.') ~ \"'\"}});"
    ]
) }}

WITH api_market_price AS (

  SELECT
    ingestion_timestamp,
    TIMESTAMP AS event_timestamp,
    item_id,
    quality,
    CAST(
      location AS STRING
    ) AS location,
    item_count AS item_amount,
    item_count * avg_price AS silver_count,
  FROM
    {{ source (
      'staging',
      'inc_albion_api_market_price'
    ) }}
),
dump_market_price AS (
  SELECT
    TIMESTAMP("2020-08-29T11:11:11Z") AS ingestion_timestamp,
    TIMESTAMP AS event_timestamp,
    item_id,
    quality,
    CAST(
      location AS STRING
    ) AS location,
    item_amount,
    silver_amount
  FROM
    {{ source (
      'staging',
      'full_albion_dump_market_price'
    ) }}
  WHERE
    aggregation = 6
  GROUP BY
    item_amount,
    silver_amount,
    item_id,
    location,
    quality,
    TIMESTAMP
) (
  SELECT
    *
  FROM
    api_market_price

{% if is_incremental () %}
WHERE
  ingestion_timestamp > _dbt_max_partition
{% endif %}
)
UNION ALL(
    SELECT
      *
    FROM
      dump_market_price

{% if is_incremental () %}
LIMIT
  0
{% endif %}
)
