from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries



# Define default arguments for all tasks in the DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,       # Tasks don't rely on previous run success
    'retries': 3,                   # Total 3 retries for failed tasks
    'retry_delay': timedelta(minutes=5),  # 5-minute delay between retries
    'email_on_retry': False,
    'start_date': datetime(2018, 11, 1),
    'catchup':False,

}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="sunnyokey-v2",
        s3_key="log-data/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json",
        json_path='s3://sunnyokey-v2/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="sunnyokey-v2",
        s3_key="song-data/A/A/A/",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        load_fact_sql=SqlQueries.songplay_table_insert

    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        truncate_table=True,
        load_dim_sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        truncate_table=True,
        load_dim_sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        truncate_table=True,
        load_dim_sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        truncate_table=True,
        load_dim_sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    data_quality_checks=[
        {'check_query': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL', 'expected_result': 0},
        {'check_query': 'SELECT COUNT(*) FROM users WHERE userid IS NULL', 'expected_result': 0},
        {'check_query': 'SELECT COUNT(*) FROM songs WHERE artistid IS NULL', 'expected_result': 0},
        {'check_query': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL', 'expected_result': 0},
        {'check_query': 'SELECT COUNT(*) FROM time WHERE start_time IS NULL', 'expected_result': 0}
    ]
)
    stop_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_songs_to_redshift
    start_operator >> stage_events_to_redshift
    stage_songs_to_redshift >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> run_quality_checks >> stop_operator
    load_songplays_table >> load_song_dimension_table >> run_quality_checks >> stop_operator
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks >> stop_operator
    load_songplays_table >> load_time_dimension_table >> run_quality_checks >> stop_operator


final_project_dag = final_project()
