from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# DAG 정의
with DAG(
    'life_expectancy',
    default_args={
        'depends_on_past': False,
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

    # 작업 정의
    t1 = BashOperator(
        task_id='life_expectancy',
        bash_command='echo "life_expectancy"',
    )

    task_sex = BashOperator(
        task_id='sex',
        bash_command='echo "sex"',
    )

    task_country = BashOperator(
        task_id='country',
        bash_command='echo "country"',
    )

    task_insurance = BashOperator(
        task_id='insurance',
        bash_command='echo "insurance"',
    )

    task_end = EmptyOperator(
        task_id='end',
    )

    task_start = EmptyOperator(
        task_id='start',
    )

    # 작업 순서 정의
    task_start >> t1
    t1 >> task_country >> task_end
    t1 >> task_sex >> task_end
    t1 >> task_insurance >> task_end

