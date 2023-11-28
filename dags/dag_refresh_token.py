from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2 import IntegrityError

import requests
import json
import pytz

# DEFAULT ARGUMENTS FOR DAG
default_args = {
    'owner': 'quin',
    'retry_delay': timedelta(minutes=5)
}

"""
GLOBAL VARIABLES FOR SPOTIFY API
"""
CLIENT_ID = Variable.get("CLIENT_ID")
CLIENT_SECRET = Variable.get("CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = Variable.get("SPOTIFY_REFRESH_TOKEN")


def generate_access_token(ti):
    response = requests.post(
        'https://accounts.spotify.com/api/token',
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': SPOTIFY_REFRESH_TOKEN,
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        }
    )

    new_access_token = response.json().get('access_token')
    ti.xcom_push(key='new_access_token', value=new_access_token)
    Variable.set("SPOTIFY_ACCESS_TOKEN", new_access_token)


with DAG(
    dag_id='dag_refresh_token',
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='0 * * * *'
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_spotify',
        sql = """
            CREATE TABLE IF NOT EXISTS token_log (
                date_init timestamp,
                access_token character varying,
                primary key (date_init, access_token)
            )
        """
    )
    
    task2 = PythonOperator(
        task_id='generate_access_token',
        python_callable=generate_access_token
    )

    task3 = PostgresOperator(
        task_id='insert_token_into_table',
        postgres_conn_id='postgres_spotify',
        sql = """
            INSERT INTO token_log (date_init, access_token) VALUES (%(date_now)s, %(new_access_token)s)
        """,
        parameters={'date_now': datetime.now(), 'new_access_token': '{{ ti.xcom_pull(key="new_access_token") }}'},
    )
    task1 >> task2 >> task3