import datetime as dt
import os
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
LIB_DIR = os.path.join(os.path.dirname(DAG_DIR), 'lib')
sys.path.append(LIB_DIR)

from CustomOperators import MDAuditOperator


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
        start_date=dt.datetime(2023, 9, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        task1 = MDAuditOperator(
            dwh_connection_id='greenplum',
            table_name='stage.mdaudit_checklists',
            source_connection_id='mdaudit',
            endpoint='/v1/connector/rpc/stt_checklists?last_modified_at=gte.{start_date}&last_modified_at=lt.{end_date}',
        )

        task2 = MDAuditOperator(
            dwh_connection_id='greenplum',
            table_name='stage.mdaudit_shops',
            source_connection_id='mdaudit',
            endpoint='/v1/orgstruct/shops',
        )

        [task1, task2]
        
    with TaskGroup('Формирование_слоя_DDS') as data_to_dds:

        dds_regions = PostgresOperator(
            task_id='quality_of_service_regions',
            postgres_conn_id='greenplum',
            sql='scripts/dds_regions.sql',
        )

        tables = (
            'shops',
            'divisions',
            'templates',
            'resolvers',
        )

        parallel_tasks = []

        for table in tables:
            parallel_tasks.append(
                PostgresOperator(
                    task_id=f'quality_of_service_{table}',
                    postgres_conn_id='greenplum',
                    sql=f'scripts/dds_{table}.sql',
                )
            )

        dds_checks = PostgresOperator(
            task_id=f'quality_of_service_checks',
            postgres_conn_id='greenplum',
            sql='scripts/dds_checks.sql',
        )

        dds_answers = PostgresOperator(
            task_id='quality_of_service_answers',
            postgres_conn_id='greenplum',
            sql='scripts/dds_answers.sql',
        )

        dds_regions >> parallel_tasks >> dds_checks >> dds_answers

    with TaskGroup('Формирование_слоя_dm') as data_to_dm:

        pass

    with TaskGroup('Проверка_данных') as data_check:

        pass

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_check >> end
