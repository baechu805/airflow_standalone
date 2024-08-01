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
    tags=['movie', 'api', 'amt'],
) as dag:
    

    def common_get_data(ds_nodash, url_param):
        from mov.api.call import save2df
        df = save2df(load_dt=ds_nodash, url_param=url_param)

        print(df[['movieCd', 'movieNm']].head(5)) 
        
        for key, value in url_param.items():
            df[key] = value

        p_cols = ['load_dt'] + list(url_param.keys())
        df.to_parquet('~/tmp/test_parquet',
                partition_cols=p_cols
                # partition_cols=['load_dt', 'movieKey']
        )

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        
        df = apply_type2df(load_dt=ds_nodash)
        
        print("*" * 33)
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
            return "get.start","echo.task"

    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_fun,

    )

    save_data = PythonVirtualenvOperator(
            task_id="save.data",
            python_callable=save_data,
            system_site_packages=False,
            trigger_rule="all_done",
            requirements=["git+https://github.com/baechu805/movie.git@0.3/api"],
    )

    multi_y = PythonVirtualenvOperator(
            task_id='multi.y',
            python_callable=common_get_data,
            system_site_packages=False,
            trigger_rule="all_done",
            requirements=["git+https://github.com/baechu805/movie.git@0.3/api"],
            op_kwargs={
                "url_param": {"multiMovieYn": "Y"},
            }

    )


    multi_n = PythonVirtualenvOperator(
            task_id='multi.n',
            python_callable=common_get_data,
            system_site_packages=False,
            trigger_rule="all_done",
            requirements=["git+https://github.com/baechu805/movie.git@0.3/api"],
            op_kwargs={
                "url_param": {"multiMovieYn": "N"}
            }
    )
    nation_k  = PythonVirtualenvOperator(
            task_id='nation_k',
            python_callable=common_get_data,
            system_site_packages=False,
            trigger_rule="all_done",
            requirements=["git+https://github.com/baechu805/movie.git@0.3/api"],
            op_kwargs={
                "url_param": {"repNationCd": "K"}
        }
    )

    nation_f  = PythonVirtualenvOperator(
            task_id='nation_f',
            python_callable=common_get_data,
            system_site_packages=False,
            trigger_rule="all_done",
            requirements=["git+https://github.com/baechu805/movie.git@0.3/api"],
            op_kwargs={
                "url_param": {"repNationCd": "F"}
        }
    )



    rm_dir = BashOperator(
            task_id= "rm.dir",
            bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
    
    )

    echo_task = BashOperator(
            task_id='echo.task',
            bash_command="echo 'task'"
    )

    get_start = EmptyOperator(
            task_id='get.start',
            trigger_rule="all_done"
    )

    get_end = EmptyOperator(
            task_id='get.end',
            trigger_rule="all_done"
    )


    end = EmptyOperator(task_id='end')
    start = EmptyOperator(task_id='start')
    
    throw_err = BashOperator(
            task_id='throw.err',
            bash_command="exit 1",
            trigger_rule="all_done"
     )
   
    start >> branch_op
    start >> throw_err >> save_data

    branch_op >> rm_dir >> get_start 
    branch_op >> get_start
    branch_op >> echo_task
    
    get_start >> [multi_y, multi_n, nation_k, nation_f] >> get_end >> save_data >> end

    
