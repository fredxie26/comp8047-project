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
    'start_date': datetime(2024, 1, 1),
    'email': ['fredxie26@yahoo.ca'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'job_count_load_mssql',
    default_args=default_args,
    description='Load job count csv file to mssql with Airflow',
    schedule_interval='@daily',
)

country_list = ['us', 'ca']
root_file_path = '/opt/airflow/dags/files/job_count/'

def insert_mssql_hook(ds=None, **kwargs):
    # print(ds)
    exec_ds = kwargs.get("templates_dict").get("exec_ds")
    country = kwargs.get("templates_dict").get("country")
    # print(exec_ds)
    # print(os.getcwd())
    mssql_hook = MsSqlHook(mssql_conn_id="my_mssql_conn", schema="comp8047")
    conn = mssql_hook.get_conn()
    cur = conn.cursor()
    query = f"INSERT INTO {country}_job_count_raw_" + exec_ds + " VALUES (%s, %s, %s, %s, %s, %s)"
    file_path = root_file_path + f'{country}_job_results_' + ds + '.csv'

    us_job_count_data = pd.read_csv(file_path, index_col=False)
    rows = pd.DataFrame(us_job_count_data)
    rows = rows.replace(',', '', regex=True).dropna()
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

for country in country_list:
    t1 = FileSensor(
        task_id=f'{country}_step1',
        filepath=root_file_path + f"{country}_job_results_" + '{{ds}}.csv',
        poke_interval=60*5,
        timeout=60*60,
        dag=dag
    )

    t2 = MsSqlOperator(
        task_id=f'{country}_step2',
        mssql_conn_id='my_mssql_conn',
        sql="query/create_job_count_temp.sql",
        params={
            'country': country
        },
        dag=dag
    )

    t3 = PythonOperator(
        task_id=f'{country}_step3',
        provide_context=True,
        python_callable=insert_mssql_hook,
        templates_dict={
            'exec_ds': '{{ ds_nodash }}',
            'country': country
        },
        dag=dag
    )

    t4 = MsSqlOperator(
        task_id=f'{country}_step4',
        mssql_conn_id='my_mssql_conn',
        sql="query/insert_job_count.sql",
        params={
            'country': country
        },
        dag=dag
    )

    t5 = MsSqlOperator(
        task_id=f'{country}_step5',
        mssql_conn_id='my_mssql_conn',
        sql="query/drop_job_count_temp.sql",
        params={
            'country': country
        },
        dag=dag
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5