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
    #logging.info(select_sql)

    cur = get_Redshift_connection()

    # 기존의 서머리 테이블을 복사한 임시 테이블 생성
    try:
        sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
              CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table}"""
        cur.execute(sql)

    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")

    # NPS값을 계산후 임시테이블에 INSERT함. 이떄 중복이 생길수 있다.
    try:
        sql = f"""
                    WITH target_date_nps AS (SELECT * FROM yongsookim_com.nps WHERE TO_CHAR(created_at,'YYYY-MM-DD') = '2023-01-02')
                    INSERT INTO {schema}.{table}(run_date, nps) 
                    SELECT '2023-01-02' AS run_date, (((SELECT COUNT(DISTINCT id) from target_date_nps WHERE score BETWEEN 9 and 10) - (SELECT COUNT(DISTINCT id) from target_date_nps WHERE score BETWEEN 0 and 6))::Float / ((SELECT COUNT(DISTINCT id) from target_date_nps)) * 100) AS nps 
            """
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")

    # 기존 테이블을 삭제후, 임시테이블에서 중복을 뺸 결과를 써머리 테이블로 넣는다.
    try:
        sql = f"""DROP TABLE IF EXISTS {schema}.{table};
                  INSERT INTO {schema}.{table} 
                    SELECT run_time, nps, created_time
                    FROM (
                            SELECT * ,ROW_NUMBER OVER (PARTITION BY run_time ORDER BY created_time DESC) seq
                            FROM {schema}.temp_{table}
                      )
                    WHERE seq=1
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
