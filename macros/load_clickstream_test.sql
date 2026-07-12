{% macro load_clickstream_test() %}
  {% set query %}
    copy into softcart_db.raw.clickstream_events (raw_event)
    from @softcart_db.raw.bronze_stage/clickstream/
    file_format = softcart_db.raw.json_format
    on_error = 'CONTINUE'
  {% endset %}
  {% set results = run_query(query) %}
  {{ log(results, info=true) }}
{% endmacro %}
