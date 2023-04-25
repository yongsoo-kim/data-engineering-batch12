from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
# from plugins import slack

import requests
import logging
import psycopg2


def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def extract(**context):
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    logging.info(execution_date)
    f = requests.get(link)
    return (f.json())


def transform(**context):
    json_response = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")

    weather_forecast_info = list()
    for d in json_response["daily"]:
        date = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        info = {
            "date": date,
            "temp": d["temp"]["day"],
            "min_temp": d["temp"]["min"],
            "max_temp": d["temp"]["max"]
        }
        weather_forecast_info.append(info)

    return weather_forecast_info


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    weather_forecast_info = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql = "DELETE FROM {schema}.{table};".format(schema=schema, table=table)

    for info in weather_forecast_info:
        date, temp, min_temp, max_temp = info["date"], info["temp"], info["min_temp"], info["max_temp"]
        sql += f"""INSERT INTO {schema}.{table} (date,temp,min_temp,max_temp) VALUES ('{date}','{temp}', '{min_temp}','{max_temp}');"""
    logging.info(sql)
    try:
        cur.execute(sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise


dag_fourth_assignment = DAG(
    dag_id='weather_forcast_info_full_refresh',
    start_date=datetime(2023, 4, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 2 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'url': Variable.get("openweathermap_api_key")
    },
    dag=dag_fourth_assignment)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    params={
    },
    dag=dag_fourth_assignment)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'yongsookim_com',  ## 자신의 스키마로 변경
        'table': 'weather_forecast'
    },
    dag=dag_fourth_assignment)

extract >> transform >> load
