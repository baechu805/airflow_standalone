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
    'make_parquet',
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
    tags=['make_parquet'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_check = BashOperator(
        task_id="check.done",
        bash_command="""
            DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
            bash {{ var.value.CHECK_SH }} $DONE_FILE
            """
    )
    to_parquet = BashOperator(
        task_id= "to.parquet",
        bash_command="""
            echo "to.parquet"
            READ_PATH=~/data/csv/{{ds_nodash}}/csv.csv
            SAVE_PATH=~/data/parquet

           # mkdir -p $(dirname $SAVE_PATH)

            python  ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
        """
    )
    make_done = BashOperator(
        task_id= "make.done",
        bash_command="""
            echo "make_done"
        """
    )
    task_err = BashOperator(
        task_id= "err.report",
        bash_command="""
            echo "err.report"
        """
    )
    task_end = gen_emp('end', 'all_done') 
    task_start = gen_emp('start')

    task_start >> task_check >> to_parquet >> make_done >> task_end
    task_check >> task_err >> task_end
