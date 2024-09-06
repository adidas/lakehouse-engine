SELECT
    {% if replace_offset_value == 0 %} {{ project_date_column }}
    {% else %} ({{ project_date_column }} + interval '{{offset_value}}' hour)
    {% endif %} AS order_date,
    {{ to_date }} AS to_date,
    b.category_name,
    COUNT(a.article_id) qty_articles,
    SUM(amount) total_amount
FROM
  `{{ database }}`.`dummy_sales_kpi` a {{ joins }}
  LEFT JOIN article_categories b
    ON a.article_id = b.article_id
WHERE
  TO_DATE({{ filter_date_column }}, 'yyyyMMdd') >= (
          '{{start_date}}' + interval '{{offset_value}}' hour
  )
  AND TO_DATE({{ filter_date_column }}, 'yyyyMMdd') < (
          '{{ end_date}}' + interval '{{offset_value}}' hour
  )
GROUP BY
  1,2,3



