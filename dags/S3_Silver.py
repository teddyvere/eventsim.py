import json
from datetime import datetime

import boto3
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


def get_json_from_s3(ti):
    conn_info = Variable.get("AWS_S3_CONN", deserialize_json=True)
    print(conn_info['AWS_S3_BUCKET'])
    print(conn_info['AWS_ACCESS_KEY_ID'])
    print(conn_info['AWS_SECRET_ACCESS_KEY'])
    # Creating Session with Boto3
    s3_session = boto3.client(
        's3',
        aws_access_key_id=conn_info['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=conn_info['AWS_SECRET_ACCESS_KEY']
    )
    # Creating Object From the S3 Resource
    obj = s3_session.get_object(Bucket='eventsim', 
                                Key="eventsim/date_id={{ds}}.json")

    if obj == 200:
        print(f"Success S3 get_object response {obj}")
        file_content = obj.get()['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)

        df = pd.DataFrame(json_data)
        df['ts'] = df['ts'].map(lambda ts: datetime.fromtimestamp(ts/10000))
        print(df)
    else:
        print(f"Unsuccessful S3 get_object response {obj}")


with DAG (
    dag_id='S3_bronze_to_silver',
    start_date=datetime(2023,11,30),
    schedule='@once',
    catchup=False
) as dag:
    
    S3_key_sensor = S3KeySensor(
        task_id='sensor_S3_key',
        aws_conn_id='aws_connection',
        bucket_name='eventsim',
        bucket_key="eventsim/date_id={{ds}}.json",
        mode='poke',
        poke_interval=30,
        timeout=300
    )

    read_json_on_s3 = PythonOperator(
        task_id="read_json_on_s3",
        python_callable=get_json_from_s3,
        provide_context=True
    )

S3_key_sensor >> read_json_on_s3