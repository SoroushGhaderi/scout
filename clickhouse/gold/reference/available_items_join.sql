LEFT JOIN (
    SELECT
        match_id,
        groupUniqArray({{item_column}}) AS {{available_column}}
    FROM (
        {{available_item_sources}}
    )
    GROUP BY match_id
) AS available ON br.match_id = available.match_id
