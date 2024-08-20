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
import pprint
from pprint import pprint as pp


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
    REQUIREMENTS = ["git+https://github.com/baechu805/mov_agg.git@0.5/agg"]

    def gen_empty(*ids):
        tasks = []
        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)

    def gen_vpython(**kw):
        task = PythonVirtualenvOperator(
                task_id=kw['id'],
                python_callable=kw['fun_obj'],
                system_site_packages=False,
                requirements=REQUIREMENTS,
                op_kwargs=kw['op_kw']
            )
        return task

    def pro_data(**params):
        print("@" * 33)
        print(params['task_name'])
        print(params) # 여기는 task_name
        print("@" * 33)

    def pro_merge(task_name, **params):
        load_dt = params['ds_nodash']
        from mov_agg.u import merge
        df = merge(load_dt)
        print("*" * 33)
        print(df)

    start, end = gen_empty('start', 'end')

    apply_type = gen_vpython(
            id = 'apply.type',
            fun_obj = pro_data,
            op_kw = { "task_name": "apply_type!!!" }
            )

    merge_df = gen_vpython(
            id = 'merge_df',
            fun_obj = pro_data,
            op_kw = { "task_name": "merge_df!!!" }
            )
    de_dup = gen_vpython(
            id = 'de_dup',
            fun_obj = pro_data,
            op_kw = { "task_name": "de_dup!!!" }
            )
    summary_df = gen_vpython(
            id = 'summary_df',
            fun_obj = pro_data,
            op_kw = { "task_name": "summary_df!!!" })

    
    start >> merge_df
    merge_df >> de_dup >> apply_type
    apply_type >> summary_df >> end


