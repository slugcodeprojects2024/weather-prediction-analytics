-- Rolling 7-day cumulative rainfall per city, useful for spotting wet
-- stretches that a single day's precipitation_sum would hide.
SELECT
    city,
    forecast_date,
    precipitation_sum,
    SUM(precipitation_sum) OVER (
        PARTITION BY city
        ORDER BY forecast_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_rainfall
FROM {{ ref('stg_weather_forecast') }}
