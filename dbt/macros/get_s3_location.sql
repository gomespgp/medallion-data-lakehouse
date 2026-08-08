{% macro get_s3_location(layer, schema_name) %}
  {% set model_name = model.name %}
  {% set partition_path = var('partition_path') %}
  {% set bucket_name = var(layer ~ '_bucket') %}
  {{ return('s3://' ~ bucket_name ~ '/' ~ schema_name ~ '/' ~ model_name ~ '/' ~ partition_path ~ '/' ~ model_name ~ '.parquet') }}
{% endmacro %}