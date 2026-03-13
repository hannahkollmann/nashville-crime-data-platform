WITH monthly_calls AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        COUNT(*) AS total_calls
    FROM fact_calls_for_service_v3 f
    JOIN dim_date d
        ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
),
monthly_violent_incidents AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        COUNT(*) AS violent_incidents
    FROM fact_incidents_v2 f
    JOIN dim_date d
        ON f.date_key = d.date_key
    WHERE f.offense_nibrs IN (
        '09A', -- homicide
        '13A', -- aggravated assault
        '120', -- robbery
        '11A'  -- rape
    )
    GROUP BY d.year, d.month, d.month_name
)
SELECT
    c.year,
    c.month,
    c.month_name,
    c.total_calls,
    COALESCE(v.violent_incidents, 0) AS violent_incidents,
    ROUND(
        CAST(COALESCE(v.violent_incidents, 0) AS DOUBLE) / NULLIF(c.total_calls, 0) * 100,
        2
    ) AS violent_incident_rate_pct
FROM monthly_calls c
LEFT JOIN monthly_violent_incidents v
    ON c.year = v.year
   AND c.month = v.month
ORDER BY c.year, c.month;