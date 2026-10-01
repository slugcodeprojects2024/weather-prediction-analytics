-- 7-day moving average of daily temperature, per city.
-- Smooths day-to-day noise so a forecast trend is easier to read / plot.
SELECT
    city,
    forecast_date,
    temp_avg,
    temp_max,
    temp_min,
    AVG(temp_avg) OVER (
        PARTITION BY city
        ORDER BY forecast_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS temp_avg_7day_moving_avg,
    AVG(temp_max) OVER (
        PARTITION BY city
        ORDER BY forecast_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS temp_max_7day_moving_avg,
    AVG(temp_min) OVER (
        PARTITION BY city
        ORDER BY forecast_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS temp_min_7day_moving_avg
FROM {{ ref('stg_weather_forecast') }}
