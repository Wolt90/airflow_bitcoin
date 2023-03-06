from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection(conn_id='pg')
DB_HOST = conn.host+':'+str(conn.port)
DB_USER = conn.login
DB_PASSWORD = conn.password

# прокинуть через переменные окружения

# def get_secrets(**kwargs):
#   conn = BaseHook.get_connection(kwargs['my_conn_id'])  # URI: {conn.get_uri()}
#   DB_HOST = conn.host
#   DB_USER = conn.login
#   DB_PASSWORD = conn.password

# def execute(self, context):
#     execution_date = context.get("execution_date")
def logical_date_func(**context): # получаем из выполнения дату выполнения
  logical_date = context["dag_run"].conf["logical_date"] 

def bitcoin(logical_date):#, DB_HOST, DB_USER, DB_PASSWORD): # основной скрипт
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
  df["datetime"] = pd.to_datetime(data0['timestamp'],unit='ms') + timedelta(hours=3)
  df["logical_date"] = logical_date + timedelta(hours=3) 
  print(df)
  #    запись в бд sql
  # подключение для передачи данных в mssql 
  server = 'host.docker.internal:54321'   #localhost из контейнера https://stackoverflow.com/questions/69596430/airflow-with-docker-connect-to-a-local-host-postgresql 
  # server = 'c-c9q5ps4cduajbmmuvvnk.rw.mdb.yandexcloud.net:6432'
  # database = 'analytics'
  # username = 'admin123' 
  # password = 'admin123'
  server = DB_HOST
  database = 'analytics'
  username = DB_USER 
  password = DB_PASSWORD
  # database = os.environ.get('DB')  #"{{ var.value.get('DB') }}"  #os.environ.get('POSTGRES_DB') 
  # username = os.environ.get('DB_USER') 
  # password = os.environ.get('DB_PASSWORD') 
  #открытие соединения для записи в бд sql
  engine = create_engine('postgresql+psycopg2://'+username+':'+password+'@'+server+'/'+database) # postgresql
  # engine = create_engine('clickhouse://'+username+':'+password+'@'+server+'/'+database) 
  #    получение заголовков столбцов и преобразование данных в правильный формат для записи в бд
  columns=list(df)
  values=[sqlalchemy.types.VARCHAR(length=50),sqlalchemy.types.VARCHAR(length=3),sqlalchemy.types.VARCHAR(length=10),sqlalchemy.types.VARCHAR(length=10),sqlalchemy.types.Numeric(30,16), sqlalchemy.types.DateTime(), sqlalchemy.types.DateTime()]   
  columns_values=dict(zip(columns,values))

  df.to_sql(name='bitcoin', con=engine, if_exists='append', index=False, dtype = columns_values) 

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2023, 3, 6) - timedelta(hours=3), # чтобы выполнял с 00:00:00 именно нашего часового пояса
  'email': ['gleb90@list.ru'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=1),
  'catchup':True
}

dag = DAG(  'bitcoin',
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        tags=['GlebT','API' ],
    )

# t1 = BashOperator(
#     task_id="bash_use_variable_good",
#     bash_command="echo variable DB=${DB}",
#     env={"DB": "{{ var.value.get('DB') }}"},
# )
t2 = PythonOperator(
    task_id='bitcoin',
    python_callable=bitcoin,
    # op_kwargs={conn.host:'DB_HOST', 'DB_USER':'DB_USER', 'DB_PASSWORD':'DB_PASSWORD'},
    dag=dag)

# t1 >> t2