SELECT
    {{ to_date }} AS to_date,
    {% if replace_offset_value == 0 %} {{ project_date_column }} {% else %} ({{ project_date_column }} + INTERVAL '{{offset_value}}' HOUR) {% endif %} AS order_date,
    sales_order_schedule,
    delivery_country_cod,
    COUNT(*) orders,
    SUM(sales_order_qty) total_sales
FROM {{ database }}.order_events {{ joins }}
WHERE
{{ filter_date_column }} >= (
        '{{start_date}}' + INTERVAL '{{offset_value}}' HOUR
)
AND {{ filter_date_column }} < (
        '{{ end_date}}' + INTERVAL '{{offset_value}}' HOUR
)
AND order_date_header IS NOT NULL
GROUP BY ALL