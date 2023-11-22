from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    print('TEST FOR GOODC')

with DAG('sample_dag',
         description='A simple DAG',
         schedule_interval='0 * * * *',
         start_date=datetime(2023, 11, 22),
         catchup=False) as dag:
    
    task1 = PythonOperator(task_id='print_hello_task',
                           python_callable=print_hello,
                           dag=dag)
    
    task2 = DummyOperator(task_id='dummy_task',
                          dag=dag)

task1 >> task2