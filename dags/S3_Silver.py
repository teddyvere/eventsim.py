from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator


def get_target_key(**context) -> str:
    lists = context["lists"]
    exec_date = context["exec_date"]
    print(lists)
    print(exec_date)




with DAG (
    dag_id='S3_bronze_to_silver',
    start_date=datetime(2023,11,30),
    schedule='@once',
    catchup=False
) as dag:
    
    list_up_S3 = S3ListOperator(
        task_id='list_up_S3',
        bucket='eventsim',
        prefix='raw/',
        aws_conn_id='aws_connection',
        provide_context=True
    )

    get_target_S3_key = PythonOperator(
        task_id='get_target_S3_key',
        python_callable=get_target_key,
        op_kwargs = {
            "lists": "{{ti.xcom_pull(task_ids='list_up_S3')}}",
            "exec_date": " {{ ds }} "
        }
    )
