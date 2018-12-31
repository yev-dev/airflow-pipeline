"""
Created by yevgeniy on 2018-12-25
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator


def print_hello():
    # from IPython import embed; embed()
    return 'Hello world!'


dag = DAG(
    'hello_world',
    description='Simple tutorial DAG',
    schedule_interval='0 1 * * *',
    start_date=datetime(2018, 12, 25), catchup=False

)


dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)


hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

email = EmailOperator(
        task_id='send_email',
        to='yev.developer@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
)


dummy_operator >> hello_operator >> email

