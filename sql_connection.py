from init_logger import log
import json
from env_config import get_root_directory_db

# Logger
logger = log('SQL CONNECTION')


# SQL information sit and uat
def sql_envs_descriptions() -> json:
    path_file_sql = get_root_directory_db()
    with open(path_file_sql) as f:
        sql_env = json.load(f)
    return sql_env


# SQL  DB credentials
def sql_credentials() -> json:
    sql = sql_envs_descriptions()
    return sql
