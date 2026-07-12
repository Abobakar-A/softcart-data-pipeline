{% macro setup_json_format() %}
  {% set query %}
    create file format if not exists softcart_db.raw.json_format type = json strip_outer_array = false
  {% endset %}
  {% do run_query(query) %}
  {{ log("JSON file format created", info=true) }}
{% endmacro %}
