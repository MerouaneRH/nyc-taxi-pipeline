{#
  SCD2 vendor dimension. dbt-athena only supports snapshots on Iceberg tables
  (needs MERGE), hence table_type='iceberg'. Check strategy on vendor_name.
#}
{% snapshot dim_vendor %}
{{
  config(
    unique_key='vendor_id',
    strategy='check',
    check_cols=['vendor_name'],
    table_type='iceberg'
  )
}}
select vendor_id, vendor_name from {{ ref('vendors') }}
{% endsnapshot %}
