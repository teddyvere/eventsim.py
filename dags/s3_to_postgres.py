from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator


def json_parser(filepath):
    import json
    import pandas as pd

    with open(filepath) as f:
        json_data = json.load(f)
        df = pd.DataFrame(json_data['body'])
        return list(df.to_csv())
    
with DAG(
    dag_id='s3_to_postgres',
    start_date=datetime(2022, 10, 1),
    schedule='@once',
    catchup=False
) as dag:
    
    s3_key_sensor = S3KeySensor(
        task_id='sensor_s3_key',
        aws_conn_id='aws_connection',
        bucket_name='eventsim',
        bucket_key='eventsim/10000.json',
        mode='poke',
        poke_interval=30,
    )

    transfer_s3_to_sql = S3ToSqlOperator(
        task_id='transfer_s3_to_postgres',
        s3_bucket='eventsim',
        s3_key='eventsim/10000.json',
        parser=json_parser,
        table='events',
        column_list=(
            'id', 'ts', 'userid', 'sessionid', 'page', 'auth', 
            'method', 'status', 'level', 'iteminsession', 'location', 'useragent', 
            'lastname', 'firstname', 'registration', 'gender', 'artist', 'song', 'length'
        ),
        sql_conn_id='postgres_connection',
        aws_conn_id='aws_connection',
        dag=dag
    )

s3_key_sensor >> transfer_s3_to_sql
    