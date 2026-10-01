-- Length of each consecutive run of dry days (precipitation_sum = 0) per
-- city, using the classic "gaps and islands" trick: the difference between
-- two row-number sequences is constant within one unbroken streak and
-- changes at every streak boundary.
WITH flagged AS (
    SELECT
        city,
        forecast_date,
        precipitation_sum,
        CASE WHEN precipitation_sum = 0 THEN 1 ELSE 0 END AS is_dry
    FROM {{ ref('stg_weather_forecast') }}
),
grouped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY forecast_date)
            - ROW_NUMBER() OVER (PARTITION BY city, is_dry ORDER BY forecast_date)
            AS streak_group
    FROM flagged
)
SELECT
    city,
    MIN(forecast_date) AS spell_start,
    MAX(forecast_date) AS spell_end,
    COUNT(*) AS dry_spell_length
FROM grouped
WHERE is_dry = 1
GROUP BY city, streak_group
ORDER BY city, spell_start
