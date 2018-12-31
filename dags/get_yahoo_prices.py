"""
Created by yevgeniy on 2018-12-26
"""

from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.mysql_hook import MySqlHook

import numpy as np
import pandas as pd
from pandas_datareader import data, wb


def get_equity_prices(**kwargs):
    sp500 = data.DataReader('^GSPC', data_source="yahoo", start='1/1/2000', end='1/12/2018')
    sp500.info()


def transform_equity_prices(**kwargs):
    pass


def load_equity_prices(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='price_id')


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['yev.developer@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(days=4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    dag_id='yahoo_prices_loader',
    default_args=default_args,
    schedule_interval='@daily'
)

get_equity_prices_task = PythonOperator(
    task_id='get_equity_prices_task',
    python_callable=get_equity_prices,
    provide_context=True,
    dag=dag
)


transform_equity_prices_task = PythonOperator(
    task_id='transform_equity_prices_task',
    python_callable=transform_equity_prices,
    provide_context=True,
    dag=dag
)


load_equity_prices_task = PythonOperator(
    task_id='load_equity_prices_task',
    python_callable=load_equity_prices,
    provide_context=True,
    dag=dag
)

email = EmailOperator(
        task_id='send_email',
        to='yev.developer@gmail.com',
        subject='Loading Equity Prices finished',
        html_content=""" <h3>DONE</h3> """,
        dag=dag
)

get_equity_prices_task >> transform_equity_prices_task >> load_equity_prices_task >> email


if __name__ == "__main__" :
    dt = datetime.now()
    dt = dt.replace(tzinfo=timezone.utc)
    print("Start")
    get_equity_prices_task.run(start_date=dt, end_date=dt)
    print("Finish")

