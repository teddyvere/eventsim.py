import io
import os
from datetime import datetime

import boto3
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


def create_silver_from_s3(**context):
    conn_info = Variable.get("AWS_S3_CONN", deserialize_json=True)
    # Creating Session with Boto3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=conn_info['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=conn_info['AWS_SECRET_ACCESS_KEY']
    )
    # Creating Object From the S3 Resource
    s3_key = f"eventsim/date_id={context['execution_date']}-test.csv"
    response = s3_client.get_object(Bucket='eventsim', Key='eventsim/date_id=2023-12-01-test.csv')
    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200: 
    # Reading Json from S3
        file_content = response.get("Body")
        chunk_size=30000
        while True:
            chunk = file_content.read(chunk_size)
            print(chunk)
            if chunk:
                df = pd.read_csv(chunk)
                df['date_id'] = df['ts'].map(
                    lambda ts: datetime.strftime(
                        datetime.fromtimestamp(ts/1000), '%Y-%m-%d'
                        )
                    )
                for yyyymmdd in df['date_id'].unique():
                    df_daily = df[df.date_id==yyyymmdd]
                    # Check whether daily dataframe is already exist on S3
                    response = s3_client.get_object(
                        Bucket='eventsim', key=f'silver/date_id={yyyymmdd}.csv'
                        )
                    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                    # Found daily dataframe on S3 -> append
                    if status == 200:
                        df_exist = pd.read_csv(response.get("Body"))
                        df_merge = pd.merge([df_exist, df_daily], axis=0)
                    # Cannot find daily dataframe on S3 -> newly insert
                    elif status == 404:
                        df_merge = df_daily  
                    else:
                        raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")
                    # Update daily dataframe on S3
                    with io.StringIO() as csv_buffer:
                        df_merge.to_csv(csv_buffer, index=False)
                        response = s3_client.put_object(
                            Bucket='eventsim', Key=f'silver/date_id={yyyymmdd}.csv', Body=csv_buffer.getvalue()
                        )
                        if status == 200:
                            print(f"S3 put_object response. Status - {status} Date - {yyyymmdd} No.Records - {len(df_merge)}")
                        else:
                            print(f"S3 put_object response. Status - {status} Date - {yyyymmdd} No.Records - {len(df_merge)}")
            else:
                break
    else:
        raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")

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
        bucket_key="eventsim/date_id={{ds}}-test.csv",
        mode='poke',
        poke_interval=30,
        timeout=300
    )

    create_silver_s3 = PythonOperator(
        task_id="create_silver_s3",
        python_callable=create_silver_from_s3,
        provide_context=True
    )

S3_key_sensor >> create_silver_s3

