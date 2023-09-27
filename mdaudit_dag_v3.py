import sqlalchemy as sa
from urllib.parse import quote
import json
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.vertica_operator import VerticaOperator

from mdaudit_etl.scripts.collable import etl_proc


#-------------- DAG -----------------

default_args = {
    'owner': 'Швейников Андрей',
    'email': ['shveynikovab@st.tech'],
    'retries': 4,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'MD_Audit_v3',
        default_args=default_args,
        description='Получение данных из MD Audit.',
        start_date=dt.datetime(2023, 8, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        get_checks_and_answers = PythonOperator(
            task_id='get_checks_and_answers',
            python_callable=etl_proc.etl_start,
            op_kwargs={
                'source_type': 'rest_api',
                'data_type': 'mdaudit_questions',
                'start_date': '2023-09-01',
                'end_date': '2023-10-01',
                'end_date_EXCLUSIVE': True,
            }
        )


    with TaskGroup('Формирование_слоя_DDS') as data_to_dds:

        pass

    with TaskGroup('Формирование_слоя_dm') as data_to_dm:

        pass

    with TaskGroup('Проверка_данных') as data_check:

        pass

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_check >> end
