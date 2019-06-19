
from datetime import datetime, timedelta
from datetime import timezone
from os import path

import logging
import os
import pandas as pd


from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator as BaseMySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.mysql_hook import MySqlHook


def get_signals(local_path, file_name, prefix, ext, is_overwrite, **kwargs):

    """
    Extracting signals from database for max date

    :param local_path:
    :param file_name:
    :param prefix:
    :param ext:
    :param is_overwrite:
    :param kwargs:
    :return: file location

    """

    print(', '.join(['{}={!r}'.format(k, v) for k, v in kwargs.items()]))

    mysql_hook = MySqlHook(mysql_conn_id='research_db')

    date = str(kwargs['execution_date'].year) + '-' + str(kwargs['execution_date'].month) + '-' + str(kwargs['execution_date'].day)

    as_of_date = kwargs['ti'].xcom_pull(task_ids='mysql_get_max_signal_date_task')

    select_str = """ SELECT effective_date, security_id, sgnl, sprd FROM securities_history_us_ext_R WHERE effective_date = %(effective_date)s """

    df = mysql_hook.get_pandas_df(select_str, parameters={'effective_date': as_of_date})

    file = file_name + prefix + date + "." + ext

    filepath = path.join(os.sep,  local_path, file)

    logging.info('Saving to file: {}'.format(filepath))

    file_present = os.path.isfile(filepath)

    if not file_present:
        df.to_csv(filepath, index=False)
    else:
        if is_overwrite:
            os.remove(filepath)
            df.to_csv(filepath, index=False)
        else:
            raise  AirflowSkipException('File not parsed completely/correctly')
            logging.info("{} will be ignored. Skipped saving it".format(filepath))

    return filepath


def insert_signals(table_name, is_replace,  **kwargs):

    mysql_hook = MySqlHook(mysql_conn_id='signals_db')

    filepath = kwargs['ti'].xcom_pull(task_ids='load_signals_task')

    logging.info('Loading from file: {}'.format(filepath))

    df = pd.read_csv(filepath, skiprows=1, decimal='.', parse_dates=True, encoding='utf-8')

    mysql_hook.insert_rows(table=table_name, rows=df.values.tolist(), target_fields=['effective_date', 'security_id', 'sgnl', 'sprd'], replace=is_replace)


class ReturningMySqlOperator(BaseMySqlOperator):
    """
    Custom MySQLOperator to return SQL value so it can be extracted by other tasks via xcom pull mechanism
    """
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
        return hook.get_first(self.sql, parameters=self.parameters)


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
    dag_id='signals_etl',
    default_args=default_args,
    schedule_interval='@daily'
)

max_signal_date_sql = """SELECT MAX(as_of_date) FROM research_db.signals; """

mysql_get_max_signal_date_task = ReturningMySqlOperator(
    task_id='mysql_get_max_signal_date_task',
    sql=max_signal_date_sql, **default_args,
    mysql_conn_id='research_db'
)

load_signals_task = PythonOperator(
    task_id='load_signals_task',
    python_callable=get_signals,
    op_kwargs={'table_name': 'securities_history_us_ext_R',
                'local_path': '/Users/yevgeniy/Development/airflow-home/data',
                'file_name': 'signals',
                'prefix': '_',
                'ext': 'csv',
                'is_overwrite': True
               },
    provide_context=True,
    dag=dag
)


insert_signals_task = PythonOperator(
    task_id='insert_signals_task',
    python_callable=insert_signals,
    op_kwargs={'table_name': 'signals',
                'is_replace': True
               },
    provide_context=True,
    dag=dag
)

send_email_task = EmailOperator(
        task_id='send_email_task',
        to='yev.developer@gmail.com',
        subject='Signals ETL finished',
        html_content=""" <h3>DONE</h3> """,
        dag=dag
)


mysql_get_max_signal_date_task >> load_signals_task >> insert_signals_task >> send_email_task

if __name__ == "__main__":
    dt = datetime.now()
    dt = dt.replace(tzinfo=timezone.utc)
    logging.info("Start")
    mysql_get_max_signal_date_task.run(start_date=dt, end_date=dt)
    load_signals_task.run(start_date=dt, end_date=dt)
    insert_signals_task.run(start_date=dt, end_date=dt)
    logging.info("Finish")

