SELECT
    d.day_of_week_name,
    t.hour,
    COUNT(*) AS violent_incident_count
FROM fact_incidents_v2 f
JOIN dim_date d
    ON f.date_key = d.date_key
JOIN dim_time t
    ON f.time_key = t.time_key
WHERE d.year = 2025
  AND f.offense_nibrs IN ('09A','13A','120','11A')
GROUP BY
    d.day_of_week_name,
    t.hour
ORDER BY
    d.day_of_week_name,
    t.hour;