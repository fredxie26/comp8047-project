import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 5),
    'email': ['fredxie26@yahoo.ca'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'layoff_load_mssql',
    default_args=default_args,
    description='Load layoff csv file to mssql with Airflow',
    schedule_interval='@once',
)

root_file_path = '/opt/airflow/dags/files/layoff/'

def insert_mssql_hook(ds=None, **kwargs):
    # print(ds)
    # print(exec_ds)
    # print(os.getcwd())
    mssql_hook = MsSqlHook(mssql_conn_id="my_mssql_conn", schema="comp8047")
    conn = mssql_hook.get_conn()
    cur = conn.cursor()
    query = f"INSERT INTO layoff_raw_temp VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    file_path = root_file_path + 'layoffs.csv'

    layoff_source_data = pd.read_csv(file_path, index_col=False)
    rows = pd.DataFrame(layoff_source_data)
    rows = rows.replace(',', '', regex=True).fillna(0)
    sql_data = tuple(map(tuple, rows.values))
    # print(sql_data)
    cur.executemany(query, sql_data)
    conn.commit()
    cur.close()
    conn.close()

t0 = DummyOperator (
    task_id='begin',
    dag=dag
)

t1 = FileSensor(
    task_id='step1',
    filepath=root_file_path + 'layoffs.csv',
    poke_interval=60*5,
    timeout=60*60,
    dag=dag
)

t2 = MsSqlOperator(
    task_id='step2',
    mssql_conn_id='my_mssql_conn',
    sql="query/create_layoff_temp.sql",
    dag=dag
)

t3 = PythonOperator(
    task_id='step3',
    provide_context=True,
    python_callable=insert_mssql_hook,
    dag=dag
)

t4 = MsSqlOperator(
    task_id='step4',
    mssql_conn_id='my_mssql_conn',
    sql="query/insert_layoff.sql",
    dag=dag
)

t5 = MsSqlOperator(
    task_id='step5',
    mssql_conn_id='my_mssql_conn',
    sql="query/drop_layoff_temp.sql",
    dag=dag
)

t0 >> t1 >> t2 >> t3 >> t4 >> t5