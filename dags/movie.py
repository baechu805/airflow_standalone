from textwrap import dedent
from datetime import datetime, timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op


with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='make_parquet',
    schedule_interval="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['movie'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    get_data = BashOperator(
        task_id="get.data",
        bash_command="""
            echo get.data
            """
    )
    save_data = BashOperator(
        task_id= "save.data",
        bash_command="""
            echo "save.data"
     """       
    )


    task_end = EmptyOperator(task_id='end')
    task_start = EmptyOperator(task_id='start')

    task_start >> get_data >>  save_data  >> task_end
                                       
