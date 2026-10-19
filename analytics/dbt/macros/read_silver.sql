-- Reads every partition of a silver dataset from the S3-compatible store.
-- On MotherDuck, s3:// scans run in the cloud by default (MD_RUN = AUTO), where
-- the local SeaweedFS endpoint is unreachable: MD_RUN = LOCAL keeps the scan in
-- the local DuckDB client and only the results are uploaded.
{% macro read_silver(dataset_path) -%}
read_parquet('s3://silver/{{ dataset_path }}/**/*.parquet', hive_partitioning = true
    {%- if target.name == 'motherduck' %}, MD_RUN = LOCAL{% endif %})
{%- endmacro %}
