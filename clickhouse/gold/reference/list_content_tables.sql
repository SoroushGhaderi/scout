SELECT t.name
FROM system.tables AS t
INNER JOIN system.columns AS c
    ON t.database = c.database
   AND t.name = c.table
WHERE t.database = %(database)s
  AND startsWith(t.name, %(table_prefix)s)
  AND c.name = 'match_id'
ORDER BY t.name
