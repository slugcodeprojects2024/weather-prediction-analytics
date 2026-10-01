# Weather Prediction Analytics

An Airflow + dbt + Snowflake + Tableau pipeline that forecasts weather for
two cities (San Jose and Santa Cruz) and turns it into moving averages,
temperature anomalies, rolling rainfall, and dry-spell metrics.

See `docs/architecture.png` for the full data-flow diagram, and the
submitted report for the complete writeup: problem statement, requirements,
table structures, future work, conclusion, and references.

## What it does

1. **Extract**: an Airflow DAG (`dags/weather_etl_dag.py`) calls the
   [Open-Meteo forecast API](https://api.open-meteo.com/v1/forecast) for
   every city listed in the `weather_locations` Airflow Variable and loads
   a 16-day daily forecast into Snowflake's `RAW.WEATHER_FORECAST` table,
   idempotently (DELETE+INSERT inside one explicit transaction, with
   try/except/raise rollback on any failure).
2. **Transform**: on a successful load, that DAG triggers a second DAG
   (`dags/weather_dbt_dag.py`), which runs `dbt run`, `dbt test`, and
   `dbt snapshot` against the dbt project in `dbt/`, producing four
   analytics marts plus a snapshot of how forecasts change over time.
3. **Visualize**: a Tableau dashboard connects to the Snowflake
   `ANALYTICS` schema and charts the four marts, with city and date
   filters.

## Repository layout

```
.
├── dags/
│   ├── weather_etl_dag.py      # Extract + load (Open-Meteo -> Snowflake RAW)
│   └── weather_dbt_dag.py      # dbt run / test / snapshot, triggered by the ETL DAG
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── source.yml                      # raw.weather_forecast source definition
│   │   ├── schema.yml                      # data tests
│   │   ├── transform/
│   │   │   └── stg_weather_forecast.sql    # ephemeral staging model
│   │   └── analytics/
│   │       ├── weather_moving_avg.sql
│   │       ├── weather_temp_anomaly.sql
│   │       ├── weather_rolling_rainfall.sql
│   │       └── weather_dry_spell.sql
│   └── snapshots/
│       └── snapshot_weather_forecast.sql
├── docs/
│   └── architecture.png        # system diagram
├── docker-compose.yaml         # local Airflow (dbt installed via pip requirements)
└── README.md
```

## Table structures

**`RAW.WEATHER_FORECAST`** (written by `weather_etl_dag.py`)

| Column | Type | Constraints | Notes |
|---|---|---|---|
| city | VARCHAR(100) | NOT NULL, part of PK | e.g. `San Jose`, `Santa Cruz` |
| latitude | FLOAT | NOT NULL | |
| longitude | FLOAT | NOT NULL | |
| forecast_date | DATE | NOT NULL, part of PK | |
| temp_max | FLOAT | | degrees C |
| temp_min | FLOAT | | degrees C |
| precipitation_sum | FLOAT | | mm |
| weather_code | INTEGER | | Open-Meteo WMO weather code |
| loaded_at | TIMESTAMP_NTZ | DEFAULT CURRENT_TIMESTAMP() | load watermark |

Primary key: `(city, forecast_date)`.

**`ANALYTICS.WEATHER_MOVING_AVG`**

| Column | Type | Notes |
|---|---|---|
| city | VARCHAR | |
| forecast_date | DATE | |
| temp_avg | FLOAT | `(temp_max + temp_min) / 2` |
| temp_max | FLOAT | |
| temp_min | FLOAT | |
| temp_avg_7day_moving_avg | FLOAT | trailing 7-row window per city |
| temp_max_7day_moving_avg | FLOAT | |
| temp_min_7day_moving_avg | FLOAT | |

**`ANALYTICS.WEATHER_TEMP_ANOMALY`**

| Column | Type | Notes |
|---|---|---|
| city | VARCHAR | |
| forecast_date | DATE | |
| temp_avg | FLOAT | |
| city_avg_temp | FLOAT | mean `temp_avg` for that city over the loaded window |
| temp_anomaly | FLOAT | `temp_avg - city_avg_temp` |

**`ANALYTICS.WEATHER_ROLLING_RAINFALL`**

| Column | Type | Notes |
|---|---|---|
| city | VARCHAR | |
| forecast_date | DATE | |
| precipitation_sum | FLOAT | |
| rolling_7day_rainfall | FLOAT | trailing 7-row sum per city |

**`ANALYTICS.WEATHER_DRY_SPELL`**

| Column | Type | Notes |
|---|---|---|
| city | VARCHAR | |
| spell_start | DATE | first dry day in the streak |
| spell_end | DATE | last dry day in the streak |
| dry_spell_length | INTEGER | consecutive days with `precipitation_sum = 0` |

**`SNAPSHOTS.SNAPSHOT_WEATHER_FORECAST`**: dbt `check` strategy snapshot of
`RAW.WEATHER_FORECAST`, keyed on `city || '-' || forecast_date`, tracking
changes to `temp_max`, `temp_min`, `precipitation_sum`, `weather_code` as
Open-Meteo revises its forecast on each re-run.

## Running it locally

```bash
docker compose up airflow-init
docker compose up -d
```

Airflow UI: http://localhost:8081 (default login `airflow` / `airflow`,
unless you changed `_AIRFLOW_WWW_USER_*`).

**Admin -> Variables**: create `weather_locations` with value:

```json
[
  {"city": "San Jose", "latitude": 37.3382, "longitude": -121.8863},
  {"city": "Santa Cruz", "latitude": 36.9741, "longitude": -122.0308}
]
```

**Admin -> Connections**: create/confirm `snowflake_conn` (Snowflake
connection type) with your account, user, keypair auth, role, warehouse,
and database. `weather_etl_dag.py` uses `SnowflakeHook` directly through
this connection; `weather_dbt_dag.py` reads the same connection's fields
and passes them to dbt as `DBT_*` environment variables (see
`dbt/profiles.yml`, which reads those `env_var()`s). Place your Snowflake
RSA private key at `dbt/rsa_key.p8` (gitignored -- never commit it).

Unpause `weather_etl_to_snowflake` and trigger it. It will load the raw
table, verify the load, and then trigger `weather_dbt_transform`
automatically -- you do not trigger the dbt DAG yourself.

## dbt project

```bash
cd dbt
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
dbt snapshot --profiles-dir . --project-dir .
```

(Inside the Airflow containers, both DAGs and the `dbt` CLI already point
at `/opt/airflow/dbt` -- see `docker-compose.yaml`'s volume mounts.)

## Tableau dashboard

Connect Tableau to Snowflake (`ANALYTICS` database/schema) and build
charts against `weather_moving_avg`, `weather_temp_anomaly`,
`weather_rolling_rainfall`, and `weather_dry_spell`, filterable by city
and date range. See the report for the dashboard description and
screenshots.
