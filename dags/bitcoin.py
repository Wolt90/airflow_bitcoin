from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.variable
from datetime import datetime, timedelta, timezone
# from bitcoin import bitcoin


# def execute(self, context):
#     execution_date = context.get("execution_date")

def bitcoin():
  import pandas as pd
  import requests
  import json
  import sqlalchemy
  from sqlalchemy import create_engine
  import os

  url = 'https://api.coincap.io/v2/rates/bitcoin' #адрес запроса для получения данных
  r = requests.get(url) #сделаем запрос к адресу
  content = r.content.decode('UTF-8')
  data0=json.loads(content)
  df = pd.json_normalize(data0['data'])
  df["datetime"] = pd.to_datetime(data0['timestamp'],unit='ms') #+ timedelta(hours=3)
  df["logical_date"] = '123'  #"{{ ds }}" #datetime.now() #{{ ds }}
  print(df)
  #    запись в бд sql
  # подключение для передачи данных в mssql 
  # server = 'localhost:54321'
  server = 'c-c9q5ps4cduajbmmuvvnk.rw.mdb.yandexcloud.net:6432'
  database = 'analytics'
  username = 'admin123' 
  password = 'admin123'
  # database = os.environ.get('DB') 
  # username = os.environ.get('DB_USER') 
  # password = os.environ.get('DB_PASSWORD') 
  #открытие соединения для записи в бд sql
  engine = create_engine('postgresql+psycopg2://'+username+':'+password+'@'+server+'/'+database) # postgresql
  # engine = create_engine('clickhouse://'+username+':'+password+'@'+server+'/'+database) 
  #    получение заголовков столбцов и преобразование данных в правильный формат для записи в бд
  columns=list(df)
  values=[sqlalchemy.types.VARCHAR(length=50),sqlalchemy.types.VARCHAR(length=3),sqlalchemy.types.VARCHAR(length=10),sqlalchemy.types.VARCHAR(length=10),sqlalchemy.types.Numeric(30,16), sqlalchemy.types.DateTime(), sqlalchemy.types.VARCHAR(length=50)]   
  columns_values=dict(zip(columns,values))

  df.to_sql(name='bitcoin', con=engine, if_exists='append', index=False, dtype = columns_values) 

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2023, 2, 28) - timedelta(hours=3), # чтобы выполнял с 00:00:00 именно нашего часового пояса
  'email': ['gleb90@list.ru'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=1),
  'catchup':True,
# 'queue': 'bash_queue',
# 'pool': 'backfill',
# 'priority_weight': 10,
# 'end_date': datetime(2016, 1, 1),
}
# def python_method(ds, **kwargs):
#     Variable.set('execution_date', kwargs['execution_date'])
#     return

dag = DAG(  'bitcoin',
        schedule_interval=timedelta(minutes=30) ,
        default_args=default_args
    )

# t1 = PythonOperator(
#     task_id='execution_date',
#     provide_context=True,
#     python_callable=python_method,
#     dag=dag)
# date = "{{ ds }}"    #"{{ ds }}"
t2 = PythonOperator(
    task_id='bitcoin',
    python_callable=bitcoin,
    dag=dag)

# t1 >> t2