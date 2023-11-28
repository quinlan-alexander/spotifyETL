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

default_args = {
    'owner': 'quin',
    'retry_delay': timedelta(minutes=5)
}


"""TASK 1"""
def get_current_track_info(ti):
    access_token = Variable.get("SPOTIFY_ACCESS_TOKEN")

    response = requests.get(
        Variable.get("SPOTIFY_GET_CURRENT_TRACK_URL"),
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
    )

    if response.status_code != 200:
        return None
    else:
        resp_json = response.json()
        if 'item' in resp_json:
            track_id = resp_json['item']['id']
            track_name = resp_json['item']['name']
            track_album_name = resp_json['item']['album']['name']

            artists = [artist for artist in resp_json['item']['artists']]
            artists_names = ', '.join([artist['name'] for artist in artists])

            link = resp_json['item']['external_urls']['spotify']

            timestamp_milliseconds = resp_json['timestamp']
            timestamp_seconds = timestamp_milliseconds / 1000
            timestamp_datetime = datetime.fromtimestamp(timestamp_seconds, pytz.timezone('US/Pacific'))

            current_track_info = {
                "id": track_id,
                "name": track_name,
                "artist": artists_names,
                "album": track_album_name,
                "link": link,
                "listen_time": timestamp_datetime
            }

            ti.xcom_push(key='current_track_info', value=current_track_info) 
            return resp_json is not None


"""TASK 2"""
def get_current_track_duration(ti):
    access_token = Variable.get("SPOTIFY_ACCESS_TOKEN")
    current_track_info = ti.xcom_pull(task_ids='get_current_track_info', key='current_track_info')
    spotify_url = "https://api.spotify.com/v1/tracks/" + current_track_info['id']

    response = requests.get(
        spotify_url,
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
    )
    resp_json = response.json()
    current_track_duration = resp_json['duration_ms']
    ti.xcom_push(key='current_track_duration', value=current_track_duration)
    return resp_json['duration_ms']


"""TASK 3"""
def get_latest_listen_time(song_id):
    hook = PostgresHook(postgres_conn_id='postgres_spotify')

    query = """
          SELECT MAX(listen_time)
          FROM songs
          WHERE song_id = %s;
    """

    result = hook.get_first(query, parameters=(song_id,))
    return result[0] if result else None


def insert_song_into_table(ti):
    current_track_info = ti.xcom_pull(task_ids='get_current_track_info', key='current_track_info')
    current_listen_time = current_track_info['listen_time']
    latest_listen_time = get_latest_listen_time(current_track_info['id'])


    time_threshold = timedelta(seconds=30)
    if latest_listen_time is not None:
        diff = current_listen_time.replace(tzinfo=None) - latest_listen_time.replace(tzinfo=None)
        diff_ms = int((diff.seconds * 1000) + (diff.microseconds / 1000))

    track_duration_ms = ti.xcom_pull(task_ids='get_current_track_duration', key='current_track_duration')

    if latest_listen_time is None or (diff_ms >= track_duration_ms):
        insert_sql = """
                INSERT INTO songs (song_id, name, artist, album, link, listen_time)
                VALUES (%s, %s, %s, %s, %s, %s);
        """

        hook = PostgresHook(postgres_conn_id='postgres_spotify')

        try:
            hook.run(
                insert_sql,
                parameters=(
                    current_track_info['id'],
                    current_track_info['name'],
                    current_track_info['artist'],
                    current_track_info['album'],
                    current_track_info['link'],
                    current_track_info['listen_time']
                )
            )
            print("Row inserted successfully.")

        except IntegrityError as e:
            if "duplicate key" in str(e):
                print("Row already exists.")
            else:
                print(f"Error: {e}")

        except Exception as e:
            print(f"Error: {e}")

    else:
        print("Duplicate row. Not inserting.")


"""INSERT TRACK DAG"""
with DAG(
    dag_id='insert_track',
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='* * * * *'
) as dag:
    task1 = ShortCircuitOperator(
        task_id='get_current_track_info',
        python_callable=get_current_track_info
    )

    task2 = PythonOperator(
        task_id='get_current_track_duration',
        python_callable=get_current_track_duration
    )

    task3 = PythonOperator(
        task_id='insert_track_into_table',
        python_callable=insert_song_into_table,
        provide_context=True
    )
    task1 >> task2 >> task3