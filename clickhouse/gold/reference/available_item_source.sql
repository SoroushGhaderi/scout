SELECT match_id, '{{item_id}}' AS {{item_column}}
FROM {{database}}.{{table_name}} FINAL
WHERE match_id > 0
GROUP BY match_id, {{item_column}}
