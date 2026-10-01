"""Weather Prediction Analytics Lab: Open-Meteo forecast -> Snowflake (ELT extract/load leg).

Pulls a 16-day forecast for each city in the `weather_locations` Airflow
Variable from the Open-Meteo forecast API, and idempotently loads it into
Snowflake's RAW.WEATHER_FORECAST table.

Idempotency: each run DELETEs any existing rows for the (city, forecast_date)
pairs it is about to (re)insert, then INSERTs the fresh rows, all inside one
explicit Snowflake transaction (BEGIN / COMMIT / ROLLBACK). Re-running this
DAG for the same day, or backfilling, never produces duplicate rows.

On a successful load this DAG triggers `weather_dbt_transform` (the dbt
DAG), so the dbt run/test/snapshot always happens after a fresh ETL load.
"""

from datetime import date, timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = "snowflake_conn"
TABLE_NAME = "RAW.WEATHER_FORECAST"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_DAYS = 16

# Default used only the first time the Variable is created in the Airflow UI.
DEFAULT_LOCATIONS = [
    {"city": "San Jose", "latitude": 37.3382, "longitude": -121.8863},
    {"city": "Santa Cruz", "latitude": 36.9741, "longitude": -122.0308},
]


@dag(
    dag_id="weather_etl_to_snowflake",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["weather", "snowflake", "etl"],
    description="Extract Open-Meteo forecasts for 2+ cities and idempotently load Snowflake RAW.WEATHER_FORECAST.",
)
def weather_etl_to_snowflake():
    @task
    def get_locations() -> list[dict]:
        """Read the list of (city, latitude, longitude) dicts from an Airflow Variable.

        Demonstrates using an Airflow Variable to configure the pipeline
        without touching code: add/remove cities by editing the JSON value
        under Admin -> Variables -> weather_locations.
        """
        return Variable.get(
            "weather_locations",
            default_var=DEFAULT_LOCATIONS,
            deserialize_json=True,
        )

    @task
    def extract_forecast(locations: list[dict]) -> list[dict]:
        """Call the Open-Meteo forecast API once per city and flatten the results."""
        records: list[dict] = []

        for loc in locations:
            response = requests.get(
                OPEN_METEO_URL,
                params={
                    "latitude": loc["latitude"],
                    "longitude": loc["longitude"],
                    "daily": (
                        "temperature_2m_max,temperature_2m_min,"
                        "precipitation_sum,weather_code"
                    ),
                    "forecast_days": FORECAST_DAYS,
                    "timezone": "auto",
                },
                timeout=60,
            )
            response.raise_for_status()
            daily = response.json()["daily"]

            for forecast_date, temp_max, temp_min, precipitation, weather_code in zip(
                daily["time"],
                daily["temperature_2m_max"],
                daily["temperature_2m_min"],
                daily["precipitation_sum"],
                daily["weather_code"],
            ):
                records.append(
                    {
                        "city": loc["city"],
                        "latitude": loc["latitude"],
                        "longitude": loc["longitude"],
                        "forecast_date": forecast_date,
                        "temp_max": temp_max,
                        "temp_min": temp_min,
                        "precipitation_sum": precipitation,
                        "weather_code": weather_code,
                    }
                )

        if not records:
            raise ValueError("Open-Meteo returned no forecast records for any location.")

        return records

    @task
    def create_table() -> None:
        """Create the destination schema and table if they do not already exist."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run("CREATE SCHEMA IF NOT EXISTS RAW")
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                city               VARCHAR(100) NOT NULL,
                latitude           FLOAT NOT NULL,
                longitude          FLOAT NOT NULL,
                forecast_date      DATE NOT NULL,
                temp_max           FLOAT,
                temp_min           FLOAT,
                precipitation_sum  FLOAT,
                weather_code       INTEGER,
                loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (city, forecast_date)
            )
            """
        )

    @task
    def load_weather_forecast(records: list[dict]) -> int:
        """Idempotently load records into Snowflake inside one explicit transaction.

        DELETE the (city, forecast_date) rows this run is about to write, then
        INSERT the fresh batch, all inside BEGIN/COMMIT. Any failure rolls the
        whole batch back so partial/duplicate data is never left behind, and
        a retried or backfilled run always converges on the same end state.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cities = sorted({record["city"] for record in records})
        dates = sorted({record["forecast_date"] for record in records})

        delete_sql = f"""
            DELETE FROM {TABLE_NAME}
            WHERE city IN ({",".join(["%s"] * len(cities))})
              AND forecast_date IN ({",".join(["%s"] * len(dates))})
        """
        insert_sql = f"""
            INSERT INTO {TABLE_NAME}
                (city, latitude, longitude, forecast_date, temp_max, temp_min,
                 precipitation_sum, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                record["city"],
                record["latitude"],
                record["longitude"],
                record["forecast_date"],
                record["temp_max"],
                record["temp_min"],
                record["precipitation_sum"],
                record["weather_code"],
            )
            for record in records
        ]

        try:
            cursor.execute("BEGIN")
            cursor.execute(delete_sql, (*cities, *dates))
            cursor.executemany(insert_sql, rows)
            conn.commit()
            return len(rows)
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    @task
    def verify_load(expected_rows: int) -> int:
        """Sanity-check the committed load before letting the dbt DAG run on top of it."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        actual_rows = hook.get_first(
            f"""
            SELECT COUNT(*) FROM {TABLE_NAME}
            WHERE loaded_at >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
            """
        )[0]
        if actual_rows < expected_rows:
            raise ValueError(
                f"Row-count check failed: expected at least {expected_rows} "
                f"recently-loaded rows, found {actual_rows}."
            )
        return actual_rows

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_transform",
        trigger_dag_id="weather_dbt_transform",
        wait_for_completion=False,
    )

    locations = get_locations()
    extracted = extract_forecast(locations)
    table_ready = create_table()
    loaded = load_weather_forecast(extracted)
    table_ready >> loaded
    verified = verify_load(loaded)
    verified >> trigger_dbt_dag


weather_etl_to_snowflake()
