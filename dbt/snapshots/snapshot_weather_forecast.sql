-- Tracks forecast revisions over time: Open-Meteo's forecast for a given
-- (city, forecast_date) can change each day as the model gets closer to
-- that date. The check strategy records a new snapshot row whenever
-- temp_max, temp_min, precipitation_sum, or weather_code changes for the
-- same (city, forecast_date), so you can see how the forecast evolved.
{% snapshot snapshot_weather_forecast %}

{{
    config(
        target_schema='snapshots',
        unique_key="city || '-' || forecast_date",
        strategy='check',
        check_cols=['temp_max', 'temp_min', 'precipitation_sum', 'weather_code'],
    )
}}

SELECT *
FROM {{ source('raw', 'weather_forecast') }}

{% endsnapshot %}
