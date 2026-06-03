from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.datetime(2018, 11, 1, tz='UTC'),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule='@hourly',
    catchup=False,
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="christian-cadieux",
        s3_key="log-data",
        format_as_json="s3://christian-cadieux/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="christian-cadieux",
        s3_key="song-data"
    )

    load_songplays_table = LoadFactOperator(
        task_id = 'Load_songplays_fact_table',
        redshift_conn_id ="redshift",
        table = "songplays",
        sql_query = SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = "redshift",
        table = "users",
        sql_query = SqlQueries.user_table_insert,
        append_data = False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = "redshift",
        table = "songs",
        sql_query = SqlQueries.song_table_insert,
        append_data = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = "redshift",
        table = "artists",
        sql_query = SqlQueries.artist_table_insert,
        append_data = False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = "redshift",
        table = "time",
        sql_query = SqlQueries.time_table_insert,
        append_data = False
    )

    run_quality_checks = DataQualityOperator(
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
                'description': 'users should have no NULL userid',
                'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL",
                'expected_result': 0
            },
            {
                'description': 'users.level should only be free or paid',
                'check_sql': """
                    SELECT COUNT(*) FROM users 
                    WHERE level NOT IN ('free', 'paid')
                """,
                'expected_result': 0
            },
            {
                'description': 'songs should have no duplicate songid',
                'check_sql': """
                    SELECT COUNT(*) FROM (
                        SELECT songid, COUNT(*) 
                        FROM songs 
                        GROUP BY songid 
                        HAVING COUNT(*) > 1
                    )
                """,
                'expected_result': 0
            },
            {
                'description': 'artists should have no duplicate artistid',
                'check_sql': """
                    SELECT COUNT(*) FROM (
                        SELECT artistid, COUNT(*) 
                        FROM artists 
                        GROUP BY artistid 
                        HAVING COUNT(*) > 1
                    )
                """,
                'expected_result': 0
            },
        ]
    )


    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> end_operator
    

final_project_dag = final_project()
