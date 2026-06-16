# Automated Data Pipeline: S3 → Redshift with Apache Airflow

A production-style ELT pipeline that ingests raw JSON event data from Amazon S3 into a star-schema data warehouse on Amazon Redshift, orchestrated by Apache Airflow running in Docker. Built as the capstone project of the Udacity Data Engineering with AWS nanodegree.

---

## What This Project Demonstrates

| Skill | How It Appears Here |
| --- | --- |
| Pipeline orchestration | Airflow DAG with hourly schedule, retry logic, and dependency management |
| Custom operator development | Four reusable Airflow operators built from `BaseOperator` |
| Cloud data warehousing | Star schema on Amazon Redshift Serverless with staging tables |
| S3 data ingestion | Dynamic COPY statements generated at runtime from operator parameters |
| Data quality validation | Parametrized DQ operator running row-count and custom SQL checks |
| Containerized local dev | Full Airflow stack (webserver, scheduler, worker, triggerer) via Docker Compose |

---

## Architecture

```text
S3 (JSON logs)
    ├── log-data/        →  staging_events  ┐
    └── song-data/       →  staging_songs   ┘
                                             └──►  songplays  (fact)
                                                   users       (dim)
                                                   songs       (dim)
                                                   artists     (dim)
                                                   time        (dim)
                                                        └──► Data Quality Checks
```

Raw JSON files land in S3. Airflow's `StageToRedshiftOperator` issues a `COPY` command directly into Redshift staging tables, then transformation operators populate the star schema using SQL defined in a shared `SqlQueries` helper class.

---

## DAG

![Final project DAG](assets/final_project_dag_graph2.png)

The DAG runs hourly with no catchup. Task dependencies enforce the correct load order:

1. `Begin_execution` — dummy start gate
2. `Stage_events` and `Stage_songs` — parallel S3-to-Redshift ingestion
3. `Load_songplays_fact_table` — fact table populated from joined staging data
4. `Load_*_dim_table` (×4) — dimension tables loaded in parallel with truncate-insert
5. `Run_data_quality_checks` — validates all five tables before signaling success
6. `End_execution` — dummy terminal gate

**DAG defaults:** `depends_on_past=False`, 3 retries at 5-minute intervals, no email on retry, catchup off.

---

## Custom Operators

All four operators live in [plugins/operators/](plugins/operators/) and are registered as Airflow plugins.

### StageToRedshiftOperator

[`plugins/operators/stage_redshift.py`](plugins/operators/stage_redshift.py)

Generates and executes a Redshift `COPY` statement at runtime. Credentials are retrieved securely through Airflow's connection store (never hardcoded). The `format_as_json` parameter lets callers specify either `'auto'` or an S3 path to a JSON path file, making the same operator reusable for both events and songs datasets.

```python
StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="my-bucket",
    s3_key="log-data",
    format_as_json="s3://my-bucket/log_json_path.json"
)
```

### LoadFactOperator

[`plugins/operators/load_fact.py`](plugins/operators/load_fact.py)

Append-only fact table loader. Wraps any `SELECT` statement in an `INSERT INTO … SELECT` and executes it via Airflow's `PostgresHook`. Fact tables grow over time and never truncate.

### LoadDimensionOperator

[`plugins/operators/load_dimension.py`](plugins/operators/load_dimension.py)

Supports both **truncate-insert** (default) and **append** modes via the `append_data` flag. When `append_data=False`, the table is truncated before each load — the standard pattern for slowly changing dimensions where the full current state is rebuilt each run.

```python
LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    sql_query=SqlQueries.user_table_insert,
    append_data=False   # truncate-insert
)
```

### DataQualityOperator

[`plugins/operators/data_quality.py`](plugins/operators/data_quality.py)

Runs two classes of checks, both fully parametrized — no logic is hardcoded in the operator itself:

1. **Row-count check** — verifies each table in the `tables` list has at least one row.
2. **Custom SQL checks** — accepts a list of `{check_sql, expected_result, description}` dicts and compares actual query results against expected values.

All failures are collected and raised together as a single `ValueError`, triggering Airflow's retry and eventual task failure.

```python
DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=["songplays", "users", "songs", "artists", "time"],
    dq_checks=[
        {
            'description': 'songplays should have no NULL playid',
            'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL",
            'expected_result': 0
        },
        {
            'description': 'songs should have no duplicate songid',
            'check_sql': """
                SELECT COUNT(*) FROM (
                    SELECT songid, COUNT(*) FROM songs
                    GROUP BY songid HAVING COUNT(*) > 1
                )
            """,
            'expected_result': 0
        },
    ]
)
```

---

## Data Model

The warehouse follows a **star schema** optimized for analytical queries on user listening activity.

**Staging tables** (raw ingest, not queried by analysts):

- `staging_events` — raw app event log records
- `staging_songs` — raw song metadata records

**Fact table:**

- `songplays` — one row per song play event, with surrogate key derived from `MD5(sessionid || start_time)`

**Dimension tables:**

- `users` — user profiles (userid, name, gender, subscription level)
- `songs` — song catalog (songid, title, artist, year, duration)
- `artists` — artist records (artistid, name, location, coordinates)
- `time` — timestamp breakdown (hour, day, week, month, year, weekday)

Schema DDL: [`create_tables.sql`](create_tables.sql)

A notable implementation detail: the source song data contained duplicate `song_id` and `artist_id` values. The SQL queries for those dimension tables use `ROW_NUMBER() OVER (PARTITION BY …)` to deduplicate before inserting, preventing primary key violations.

---

## Tech Stack

- **Apache Airflow 2.8.1** — orchestration, scheduling, retry handling
- **Amazon S3** — raw data storage (JSON)
- **Amazon Redshift Serverless** — cloud data warehouse
- **Docker / Docker Compose** — local Airflow environment (CeleryExecutor with Redis and PostgreSQL)
- **Python 3** — operator and DAG authoring
- **PostgreSQL** — Airflow metadata database

---

## Running Locally

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```bash
docker-compose up -d
```

Visit [http://localhost:8080](http://localhost:8080) — credentials: `airflow` / `airflow`.

To use a different port, create a `.env` file:

```bash
AIRFLOW_PORT=8027
```

### Airflow Connections Required

In **Admin → Connections**, add:

| Conn Id | Type | Notes |
| --- | --- | --- |
| `aws_credentials` | Amazon Web Services | IAM access key + secret |
| `redshift` | Amazon Redshift | Redshift Serverless endpoint |

### AWS Setup

1. Create an IAM user with S3 read and Redshift access.
2. Provision a Redshift Serverless workgroup in `us-east-1`.
3. Copy the source data to your own S3 bucket:

   ```bash
   # Create bucket
   aws s3 mb s3://<your-bucket>/

   # Copy from Udacity's public bucket
   aws s3 cp s3://udacity-dend/log-data/  ~/log-data/  --recursive
   aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
   aws s3 cp s3://udacity-dend/log_json_path.json ~/

   # Upload to your bucket
   aws s3 cp ~/log-data/  s3://<your-bucket>/log-data/  --recursive
   aws s3 cp ~/song-data/ s3://<your-bucket>/song-data/ --recursive
   aws s3 cp ~/log_json_path.json s3://<your-bucket>/
   ```

4. Run [`create_tables.sql`](create_tables.sql) against your Redshift cluster to create the schema.
5. Enable and trigger the `final_project` DAG in the Airflow UI.

---

## Project Structure

```text
├── dags/
│   └── final_project.py          # DAG definition and task wiring
├── plugins/
│   ├── helpers/
│   │   └── sql_queries.py        # All SQL transformation statements
│   └── operators/
│       ├── stage_redshift.py     # S3 → Redshift staging operator
│       ├── load_fact.py          # Fact table append operator
│       ├── load_dimension.py     # Dimension truncate-insert operator
│       └── data_quality.py       # Parametrized data quality operator
├── create_tables.sql             # Redshift DDL
└── docker-compose.yaml           # Full Airflow stack for local dev
```

---

*Part of the [Udacity Data Engineering with AWS Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) — Project 4 of 4.*
