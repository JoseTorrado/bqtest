SELECT country, COUNT(*) as user_count
FROM ${TABLE}
GROUP BY country
ORDER BY country
