from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta

from airflow import AirflowException

import requests
import logging
import psycopg2

from airflow.exceptions import AirflowException

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()


def execSQL(**context):

    schema = context['params']['schema'] 
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """
    sql += select_sql

    cur.execute(sql)

    cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"{schema}.{table} didn't have any record")

    try:
        sql = f"""INSERT INTO {schema}.{table}(run_date, nps) 
                    SELECT TO_CHAR('2023-01-02','YYYY-MM-DD') AS run_date, (((SELECT COUNT(DISTINCT id) from {schema}.temp_{table} WHERE score BETWEEN 9 and 10) - (SELECT COUNT(DISTINCT id) from {schema}.temp_{table} WHERE score BETWEEN 0 and 6))::Float / ((SELECT COUNT(DISTINCT id) from {schema}.temp_{table})) * 100) AS nps
                  ON CONFLICT (run_date) 
                  DO
                    UPDATE SET run_date=excluded.run_date, SET nps=excluded.nps, SET created_date=now() 
            """

        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


dag = DAG(
    dag_id = "Build_NPS_Summary",
    start_date = datetime(2023,1,1),
    schedule = '@once',
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'yongsookim_com',
        'table': 'nps_summary',
        'sql' : """SELECT * FROM yongsookim_com.nps WHERE TO_CHAR(created_at,'YYYY-MM-DD') = '2023-01-02'"""
    },
    dag = dag
)
