{% macro create_clickstream_table() %}
  {% set query %}
    create table if not exists softcart_db.raw.clickstream_events (
        raw_event VARIANT,
        loaded_at TIMESTAMP_NTZ default current_timestamp()
    )
  {% endset %}
  {% do run_query(query) %}
  {{ log("clickstream_events table created", info=true) }}
{% endmacro %}
