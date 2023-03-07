from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pandas as pd
import requests
import json
import sqlalchemy
from datetime import datetime, timedelta

conn = BaseHook.get_connection(conn_id='pg')
database, table_name = 'analytics', 'bitcoin'
url = 'https://api.coincap.io/v2/rates/bitcoin' #адрес запроса для получения данных
engine = sqlalchemy.create_engine('postgresql+psycopg2://'+conn.login+':'+conn.password+'@'+conn.host+':'+str(conn.port)+'/'+database) # postgresql

def logical_date_func(**context): # получаем из выполнения дату выполнения
  logical_date = context["dag_run"].conf["logical_date"] 

def bitcoin(logical_date): # основной скрипт
  data = json.loads(requests.get(url).content.decode('UTF-8')) #сделаем запрос к адресу и загрузим в json
  df = pd.json_normalize(data['data']) # превратим в df
  df["datetime"], df["logical_date"] = pd.to_datetime(data['timestamp'],unit='ms') + timedelta(hours=3), logical_date + timedelta(hours=3) # добавим столбцы времени выполнения
  values=[sqlalchemy.types.VARCHAR(length=50),sqlalchemy.types.VARCHAR(length=3),sqlalchemy.types.VARCHAR(length=10),sqlalchemy.types.VARCHAR(length=10),sqlalchemy.types.Numeric(30,16), sqlalchemy.types.DateTime(), sqlalchemy.types.DateTime()]  # определим типы столбцов 
  df.to_sql(name=table_name, con=engine, if_exists='append', index=False, dtype = dict(zip(list(df),values))) # отправим данные в sql

default_args = {
  'owner': 'airflow',
  'start_date': datetime(2023, 3, 7) - timedelta(hours=3), # чтобы выполнял с 00:00:00 именно нашего часового пояса
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

dag = DAG(  'bitcoin',
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        tags=['GlebT','API' ],
    )

task = PythonOperator(
    task_id='bitcoin',
    python_callable=bitcoin,
    dag=dag)