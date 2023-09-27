
from airflow.hooks.base import BaseHook
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CODE_DIR_PATH = os.path.join(BASE_DIR, 'lib')
sys.path.append(CODE_DIR_PATH)

from lib import ETL


api_con = BaseHook.get_connection('mdaudit')
headers = json.loads(api_con.extra)

dwh_con = BaseHook.get_connection('greenplum')

etl_proc = ETL(
    dwh_host=dwh_con.host,
    dwh_port=dwh_con.port,
    dwh_database=dwh_con.schema,
    dwh_user=dwh_con.login,
    dwh_password=dwh_con.password,
    dwh_scheme='stage',
    rest_api_endpoint='https://api.qvalon.com/v1/connector/rpc/stt_checklists',
    rest_api_method='get',
    rest_api_auth=None,
    rest_api_params_str='?last_modified_at=gte.{start_date}&last_modified_at=lt.{end_date}',
    rest_api_headers={"Authorization": "Bearer SQ8aFRy6U06OLiE3eOhptdjU31sy3CaQ"},
    rest_api_data=None,
)

# i = ETL(
#     dwh_host='vs-dwh-gpm1.st.tech',
#     dwh_port='5432',
#     dwh_database='prod_dwh',
#     dwh_user='shveynikovab',
#     dwh_password='fk2QVnJH8i',
#     dwh_scheme='test',
#     rest_api_endpoint='https://api.qvalon.com/v1/connector/rpc/stt_checklists',
#     rest_api_method='get',
#     rest_api_auth=None,
#     rest_api_params_str='?last_modified_at=gte.{start_date}&last_modified_at=lt.{end_date}',
#     rest_api_headers={"Authorization": "Bearer SQ8aFRy6U06OLiE3eOhptdjU31sy3CaQ"},
#     rest_api_data=None,
#     rest_api_json_transform={
#         'record_path': 'answers',
#         'meta': ['id', 'shop_id'],
#         'meta_prefix': "check_",
#     }  
# )

# i.etl_start(
#     source_type='rest_api',
#     data_type='mdaudit_questions_v2',
#     start_date='2023-09-01',
#     end_date='2023-10-01',
#     end_date_EXCLUSIVE=True,
# )
