SELECT
    t.hour,
    COUNT(*) AS call_count
FROM fact_calls_for_service_v3 f
JOIN dim_time t
  ON f.time_key = t.time_key
GROUP BY t.hour
ORDER BY t.hour;