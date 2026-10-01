-- How far each day's average temperature deviates from that city's own
-- average over the loaded forecast window (a simple, self-relative
-- "temperature anomaly", since this pipeline has no long-term climate
-- normal to compare against).
WITH city_baseline AS (
    SELECT
        city,
        AVG(temp_avg) AS city_avg_temp
    FROM {{ ref('stg_weather_forecast') }}
    GROUP BY city
)
SELECT
    s.city,
    s.forecast_date,
    s.temp_avg,
    b.city_avg_temp,
    s.temp_avg - b.city_avg_temp AS temp_anomaly
FROM {{ ref('stg_weather_forecast') }} s
JOIN city_baseline b
    ON s.city = b.city
