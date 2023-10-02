import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from mdaudit_etl.scripts.collable import etl_questions, etl_shops


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
        start_date=dt.datetime(2023, 7, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        tasks = []

        for offset in range(3):

            tasks.append(
                PythonOperator(
                    task_id=f'get_checks_and_answers_month_offset_{offset}',
                    python_callable=etl_questions.etl_start,
                    op_kwargs={
                        'data_type': 'mdaudit_questions',
                        'month_offset': offset,
                    }
                )
            )

        tasks.append(
            PythonOperator(
                task_id=f'get_shops',
                python_callable=etl_shops.etl_start,
                op_kwargs={
                    'data_type': 'mdaudit_shops',
                    'periodic_data': False,
                }
            )
        )
        

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
            postgres_conn_id='greenplum'
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
