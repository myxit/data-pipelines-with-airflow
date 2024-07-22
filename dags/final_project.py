from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.models import Variable

S3_BUCKET = Variable.get('s3_bucket')
S3_PREFIX_SONGS = Variable.get("s3_prefix_songs")
S3_LOG_SCHEMA = f"s3://{S3_BUCKET}/{Variable.get('s3_log_schema')}"
S3_PREFIX_LOG_DATA = "log-data/{year}/{month}"

AWS_CREDENTIALS = "aws_credentials"
REDSHIFT_CONN_ID = "redshift"

default_args = {
    "catchup": False,
    "owner": "admin",
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2018, 11, 1),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        aws_credentials_id=AWS_CREDENTIALS,
        do_truncate=False,
        json_schema=S3_LOG_SCHEMA,
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX_LOG_DATA,
        table="staging_events",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )
    
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks


final_project_dag = final_project()
