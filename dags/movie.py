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
    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule_interval="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie'],
) as dag:
    
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    def get_data(ds_nodash):
        from mov.api.call import save2df
        df=save2df(ds_nodash)
        print(df.head(5))
        
    def print_context(ds=None, **kwargs):
         pprint(kwargs)
         print(ds)
   
    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        
        df = apply_type2df(load_dt=ds_nodash)
        
        print(df.head(10))
        print("*" * 33)
        print(df.dtypes)

        # 개봉일 기준 그룹핑 누적 관객수 합
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt' : 'sum'}).reset_index()
        print(sum_df)

    def branch_fun(ds_nodash):
        # ld = kwargs['ds_nodash']
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
        if os.path.exists(path):
            return "rm.dir"

        else:
            return "get.data","echo.task"

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,

    )


    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=get_data,
        requirements=["git+https://github.com/baechu805/movie.git@0.3/api"],
        system_site_packages=False,
        trigger_rule="all_done",
        #venv_cache_path="/home/joo/tmp2/air_venv/get_data"
    )
    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="all_done",
        requirements=["git+https://github.com/baechu805/movie.git@0.3/api"],
       #  venv_cache_path="/home/joo/tmp2/air_venv/get_data"
    )


    rm_dir = BashOperator(
        task_id= "rm.dir",
        bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
    
    )

    echo_task = BashOperator(
            task_id='echo.task',
            bash_command="echo 'task'"
    )

    multi_y = EmptyOperator(task_id='multi.y') # 다양성 영화 유무
    multi_n = EmptyOperator(task_id='multi.n')
    nation_k  = EmptyOperator(task_id='nation.k')
    nation_f  = EmptyOperator(task_id='nation.f')
    end = EmptyOperator(task_id='end')
    start = EmptyOperator(task_id='start')
    
    join_task = BashOperator(
            task_id='join',
            bash_command="exit 1",
            trigger_rule="all_done"
     )
   
    start >> branch_op
    start >> join_task >> save_data

    branch_op >> rm_dir >> [get_data, multi_y, multi_n, nation_k, nation_f]  
    branch_op >> [get_data, multi_y, multi_n, nation_k, nation_f]
    branch_op >> echo_task >> save_data
    
    [get_data, multi_y, multi_n, nation_k, nation_f] >> save_data >> end

    
