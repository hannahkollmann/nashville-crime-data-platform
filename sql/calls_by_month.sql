SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) AS call_count
FROM fact_calls_for_service_v3 f
JOIN dim_date d
  ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;