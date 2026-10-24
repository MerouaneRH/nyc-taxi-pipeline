{#
  Creates Glue database nyc_taxi_raw + external tables over the raw S3 data.
  Run once (idempotent):  dbt run-operation create_raw_tables

  Types follow the 2024 TLC parquet (int32 -> int, int64 -> bigint, double).
  Athena reads parquet by column name; month files whose physical types differ
  (older years have different passenger_count etc.) can fail on read, so the
  project vars min_year/max_year cap which partitions staging touches.
#}
{% macro create_raw_tables(bucket='merouane-nyc-taxi-raw-euw3-2026') %}
  {% set db = var('raw_database') %}
  {% set stmts = [] %}

  {% do stmts.append('create database if not exists ' ~ db) %}

  {% do stmts.append("
create external table if not exists " ~ db ~ ".yellow_trips (
  VendorID int,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  passenger_count bigint,
  trip_distance double,
  RatecodeID bigint,
  store_and_fwd_flag string,
  PULocationID int,
  DOLocationID int,
  payment_type bigint,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  improvement_surcharge double,
  total_amount double,
  congestion_surcharge double,
  Airport_fee double
)
partitioned by (year int, month int)
stored as parquet
location 's3://" ~ bucket ~ "/raw/yellow/'
tblproperties (
  'projection.enabled' = 'true',
  'projection.year.type' = 'integer',
  'projection.year.range' = '2009,2030',
  'projection.month.type' = 'integer',
  'projection.month.range' = '1,12',
  'projection.month.digits' = '2',
  'storage.location.template' = 's3://" ~ bucket ~ "/raw/yellow/year=${year}/month=${month}/'
)") %}

  {% do stmts.append("
create external table if not exists " ~ db ~ ".taxi_zone_lookup (
  LocationID string,
  Borough string,
  Zone string,
  service_zone string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties ('separatorChar' = ',', 'quoteChar' = '\"')
stored as textfile
location 's3://" ~ bucket ~ "/raw/zones/'
tblproperties ('skip.header.line.count' = '1')") %}

  {% for s in stmts %}
    {% do log('Running: ' ~ (s | trim | replace('\n', ' ') | truncate(90)), info=True) %}
    {% do run_query(s) %}
  {% endfor %}
{% endmacro %}
