from textwrap import dedent
from datetime import datetime, timedelta
from pprint import pprint
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print(f"ds_nodash =>{kwargs['ds_nodash']}")
        print("=" * 20)
        from mov.api.call import get_key, save2df
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        YYYYMMDD=kwargs['ds_nodash']
        df=save2df(YYYYMMDD)
        print(df.head(5))

    def print_context(ds=None, **kwargs):
         """Print the Airflow context and ds variable from the context."""
         print("::group::All kwargs")
         pprint(kwargs)
         print("::endgroup::")
         print("::group::Context variable ds")
         print(ds)
         print("::endgroup::")
         return "Whatever you return gets printed in the logs"

    run_this = PythonVirtualenvOperator(
        task_id="print_the_context",
        python_callable=print_context
        requirements=["git+https://github.com/baechu805/movie.git@0.2/api"],
        system_site_packages=False,
    
    )
    get_data = PythonOperator(
        task_id="get.data",
        python_callable=get_data
    
    )

    save_data = BashOperator(
        task_id= "save.data",
        bash_command="""
        """   
    )


    task_end = EmptyOperator(task_id='end')
    task_start = EmptyOperator(task_id='start')

    task_start >> get_data >>  save_data  >> task_end
    task_start >> run_this >> task_end                                   
