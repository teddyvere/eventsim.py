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
    s3_key = f"eventsim/date_id={{context['execution_date']}}-test.csv"
    response = s3_client.get_object(Bucket='eventsim', 
                                    Key='eventsim/date_id=2023-12-04-test.csv')
    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200: 
    # Reading Json from S3
        file_content = response.get("Body")
        chunk_size=30000
        cols = [
            'ts','userId','sessionId','page','auth','method',
            'status','level','itemInSession','location','userAgent','lastName',
            'firstName','registration','gender','artist','song','length','date_id'
        ]
        for chunk in pd.read_csv(file_content, chunksize=chunk_size):
            df_chunk = pd.DataFrame(chunk, columns=cols)
            # Convert timestamp to date_id
            df_chunk['date_id'] = df_chunk['ts'].map(
                lambda ts: datetime.strftime(
                    datetime.fromtimestamp(ts/1000), '%Y-%m-%d'
                    )
                )
            # Put object by date_id
            for date_id, df_date in df_chunk.groupby(['date_id']):
                # Check whether daily dataframe is already exist on S3
                try:
                    response = s3_client.get_object(
                        Bucket='eventsim', Key=f'silver/date_id={date_id[0]}/0.csv'
                        )
                    df_exist = pd.read_csv(response.get("Body"))
                    df_merge = pd.merge([df_exist, df_date], axis=0)
                # Cannot find daily dataframe on S3 -> newly insert
                except:
                    df_merge = df_date  
                with io.StringIO() as csv_buffer:
                    df_merge.to_csv(csv_buffer, index=False)
                    response = s3_client.put_object(
                        Bucket='eventsim', Key=f'silver/date_id={date_id[0]}/0.csv', Body=csv_buffer.getvalue()
                    )
                    if status == 200:
                        print(f"S3 put_object response. Status - {status} Date - {date_id[0]} No.Records - {len(df_merge)}")
                    else:
                        print(f"S3 put_object response. Status - {status} Date - {date_id[0]} No.Records - {len(df_merge)}")
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

    create_silver_S3 = PythonOperator(
        task_id="create_silver_s3",
        python_callable=create_silver_from_s3,
        provide_context=True
    )

S3_key_sensor >> create_silver_S3

