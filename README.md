# DATA 226 — Homework 3: Weather ETL on Airflow

Ports the HW2 Colab pipeline to Airflow. The DAG pulls the last 60 days of daily
weather for San Jose from the Open-Meteo API and full-refreshes a table in
Snowflake.

## Repository contents

| Path | What it is |
| --- | --- |
| `dags/weather_to_snowflake.py` | The DAG — all pipeline code lives here |
| `Screenshots_hw3.pdf` | All required screenshots |
| `docker-compose.yaml` | Local Airflow setup (Airflow 2.10.1 + Postgres) |
| `keys/` | Snowflake key-pair — git-ignored, not in this repo |

## DAG

DAG id `WeatherData_ETL`, scheduled `30 2 * * *`.

Tasks, built with the `@task` decorator:

| Task | What it does |
| --- | --- |
| `extract` | Calls Open-Meteo with lat/long read from Airflow Variables |
| `transform` | Flattens the JSON response into rows and validates for nulls |
| `load` | Creates the table if needed, then DELETE + INSERT in one transaction |
| `check_table_stats` | Logs the row count and date range |

Dependencies: `extract` → `transform` → `load` → `check_table_stats`. The first
three are chained by passing return values through XCom; the last is set
explicitly with `loaded >> stats`.

## Target table

`DEMO_DB.RAW.WEATHER`, with a composite primary key on
`(latitude, longitude, date)`:

| Column | Type |
| --- | --- |
| latitude | NUMBER(9,6) |
| longitude | NUMBER(9,6) |
| date | DATE |
| temp_max | FLOAT |
| temp_min | FLOAT |
| precipitation | FLOAT |
| weather_code | INTEGER |

## Full refresh and idempotency

`load` wraps `DELETE` and the `INSERT` loop in `BEGIN` / `COMMIT`, with
`ROLLBACK` in the `except` block, so a partial load can't leave the table in a
half-written state.

`CREATE TABLE IF NOT EXISTS` runs *before* `BEGIN` on purpose: DDL auto-commits
in Snowflake, so running it inside the transaction would end the transaction
early and leave the later `DELETE` outside its protection.

Because every run deletes and reloads the full window, running the DAG twice in
a row leaves the row count unchanged. Both runs logged:
DEMO_DB.RAW.WEATHER: 60 rows, 2026-07-19 ~ 2026-09-16


## Screenshots

All in `Screenshots_hw3.pdf`, in order:

1. The `@task` functions — `extract` and `transform`
2. `load` — table creation with the composite primary key
3. The transaction block and `check_table_stats`, plus **Admin → Variables**
   showing `latitude` and `longitude`
4. **Admin → Connections** — the `snowflake_conn` detail page
5. The Airflow Web UI with the DAG and its runs, and the **task logs** from two
   consecutive runs showing 60 rows both times (idempotency)

## Setup

Requires Docker Desktop.

1. `docker compose up -d`, then open http://localhost:8081
2. Admin → Variables:
   - `latitude` = `37.3382`
   - `longitude` = `-121.8863`
3. Admin → Connections, add `snowflake_conn` (type: Snowflake) with your
   account, user, role, warehouse `DEMO_WH`, database `DEMO_DB`, schema `RAW`,
   and the private key path `/opt/airflow/keys/rsa_key.p8`
4. Unpause `WeatherData_ETL` and trigger a run

### Key-pair authentication

The `keys/` folder is git-ignored and mounted read-only into the container at
`/opt/airflow/keys`. Generate the pair locally:
mkdir -p keys
docker run --rm -it -v "${PWD}/keys:/keys" apache/airflow:2.10.1 bash -c "openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out /keys/rsa_key.p8"
docker run --rm -it -v "${PWD}/keys:/keys" apache/airflow:2.10.1 bash -c "openssl rsa -in /keys/rsa_key.p8 -pubout -out /keys/rsa_key.pub"


Then register the public key on the Snowflake user with
`ALTER USER <user> SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, no BEGIN/END lines>'`.

Two things that cost me time, noted here in case they help someone else:

- Set only **one** of Private Key File or Private Key Content on the connection.
  The provider raises an error if both are present.
- The connection's password field holds the **key passphrase**, not the
  Snowflake account password. A mismatch shows up as
  `ValueError: Bad decrypt. Incorrect password?`

Verify the connection:
docker compose exec airflow python -c "from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook; print(SnowflakeHook(snowflake_conn_id='snowflake_conn').get_first('SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()'))"


## Notes

No credentials are stored in this repo. Snowflake access lives in the Airflow
Connection, the coordinates live in Airflow Variables, and `keys/` and `.env`
are git-ignored.
