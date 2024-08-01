from textwrap import dedent
from datetime import datetime, timedelta
from pprint import pprint
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
        )

with DAG(
    'movie_summary',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_summary',
    schedule_interval="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'api', 'amt'],
) as dag:
    

    apply_type = EmptyOperator(
            task_id='apply_type',
            trigger_rule="all_done"
    )

    merge_df = EmptyOperator(
            task_id='merge_df',
            trigger_rule="all_done"
    )

    de_dup = EmptyOperator(                                                                                        task_id='de_dup',                                                                                      trigger_rule="all_done"
    )

    summary_df = EmptyOperator(                                                                                        task_id='summary_df',                                                                                      trigger_rule="all_done"
    )

