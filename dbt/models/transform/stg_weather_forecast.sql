-- Ephemeral: compiles as a CTE inside the analytics models below, no
-- standalone table/view is created for this one (see dbt_project.yml,
-- models.build_mau.transform.+materialized: ephemeral).
SELECT
    city,
    latitude,
    longitude,
    forecast_date,
    temp_max,
    temp_min,
    (temp_max + temp_min) / 2 AS temp_avg,
    precipitation_sum,
    weather_code
FROM {{ source('raw', 'weather_forecast') }}
