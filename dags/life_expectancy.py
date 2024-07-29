
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'life_expectancy',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='life_expectancy DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['life_expectancy'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='life_expectancy',
        bash_command='life_expectancy',
    )

    task_sex = DummyOperator(
            task_id='sex'
            bash_command="""
            echo "sex"
        """
        )
    task_country = DummyOperator(
            task_id='country'
            bash_command="""
            echo "country"
        """
        )
    task_insurance = DummyOperator(
            task_id='insurance'
            bash_command="""
            echo "insurance"
        """
        )
    task_end = DummyOperator(
            task_id='end'
            bash_command="""
            echo "end"
        """
        )
    task_start = DummyOperator(
            task_id='start'
            bash_command="""
            echo "start"
        """
        )

    task_start >> t1
    t1 >> task_country >> task_end
    t1 >> task_sex >> task_end
    t1 >> task_insurance >> task_end

    # t1 >> [task_country, task_sex, task_insurance] >> task_end
