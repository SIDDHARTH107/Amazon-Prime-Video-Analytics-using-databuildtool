-- Here, I want to make sure no nulls are present in the columns of the table.
-- This is a macro that can be used in dbt to check for null values in specified
{% macro no_nulls_in_columns(model) %}
    SELECT *
    FROM {{ model }}
    WHERE {% for col in adapter.get_columns_in_relation(model) %}
        AND {{ col.column }} IS NULL OR
    {% endfor %}
    FALSE
{% endmacro %}