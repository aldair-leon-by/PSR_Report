from init_logger import log
import json
from env_config import get_root_directory_db

logger = log('MYSQL CONNECTION')


# MYSQL information sit and uat
def mysql_envs_descriptions() -> json:
    path_file_mysql = get_root_directory_db()
    with open(path_file_mysql) as f:
        mysql_env = json.load(f)
    return mysql_env


# MYSQL  DB credentials
def mysql_credentials() -> json:
    mysql = mysql_envs_descriptions()
    return mysql
