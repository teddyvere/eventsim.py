from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

with DAG (
    dag_id='bronze_to_silver',
    schedule='@once',
    start_date=datetime(2023, 12, 9),
    catchup=False
) as dag:
    spark_submit = SparkSubmitOperator(
        task_id='spark_submit',
        application='supports/spark_operator.py'
    )

spark_submit