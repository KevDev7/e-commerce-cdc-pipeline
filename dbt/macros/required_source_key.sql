{% test required_source_key(model, column_name) %}
select * from {{ model }}
where {{ column_name }} is null or {{ column_name }} = ''
{% endtest %}
