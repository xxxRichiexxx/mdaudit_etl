
from airflow.hooks.base import BaseHook
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CODE_DIR_PATH = os.path.join(BASE_DIR, 'lib')
sys.path.append(CODE_DIR_PATH)

from lib import ETL


api_con = BaseHook.get_connection('mdaudit')

dwh_con = BaseHook.get_connection('greenplum')

etl_proc = ETL(
    dwh_host=dwh_con.host,
    dwh_port=dwh_con.port,
    dwh_database=dwh_con.schema,
    dwh_user=dwh_con.login,
    dwh_password=dwh_con.password,
    dwh_scheme='stage',
    rest_api_endpoint=f'{api_con.host}/connector/rpc/stt_checklists',
    rest_api_method='get',
    rest_api_params_str='?last_modified_at=gte.{start_date}&last_modified_at=lt.{end_date}',
    rest_api_headers=json.loads(api_con.extra),
)
