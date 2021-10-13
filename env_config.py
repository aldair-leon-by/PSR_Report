import os
from init_logger import log
import json

# Logger
logger = log('FILE LOAD')


# Absolut path resources file
def get_root_directory_file_name() -> str:
    configs_path = os.path.abspath("../PSR_querys/resources/resources_name.json")
    with open(configs_path) as f:
        file_name = json.load(f)
    return file_name


# Absolut path Report folder
def get_root_directory_folder_name() -> str:
    configs_path = os.path.abspath("../PSR_querys/Reports")
    return configs_path


# Absolut path excel files
def get_root_directory_folder_excel_data() -> str:
    configs_path = os.path.abspath("../PSR_querys/excel")
    return configs_path


# Absolut path excel report files
def get_root_directory_folder_resources() -> str:
    configs_path = os.path.abspath("../PSR_querys/resources/file_name.json")
    with open(configs_path) as f:
        file_name = json.load(f)
    return file_name


# Message list
def get_root_directory_folder_message_name() -> str:
    configs_path = os.path.abspath("../PSR_querys/resources/message_name.json")
    with open(configs_path) as f:
        message_name = json.load(f)
    return message_name


def get_details_name():
    details_name = []
    details_name_ = []
    details = get_root_directory_folder_resources()
    for key, values in details.items():
        if key == 'details':
            for i in values[0]:
                details_name_.append(i)
    for i in details_name_:
        details_name.append(details['details'][0][i])
    return details_name


def get_summary_name():
    summary_name = []
    summary_name_ = []
    summary = get_root_directory_folder_resources()
    for key, values in summary.items():
        if key == 'summary':
            for i in values[0]:
                summary_name_.append(i)
    for i in summary_name_:
        summary_name.append(summary['summary'][0][i])
    return summary_name


# Absolut path db file
def get_root_directory_db() -> str:
    sql_file_name = get_root_directory_file_name()
    x = os.path.exists(os.path.abspath("../PSR_querys/credentials/" + sql_file_name['db_file_name']))
    if x:
        logger.info('ENV FILE LOADED SUCCESSFULLY! ')
        return os.path.abspath("../PSR_querys/credentials/" + sql_file_name['db_file_name'])
    else:
        logger.error('ERROR PATH DOESNT EXIST')
    return 'Verify your file name'


# Absolut path resources excel directory
def get_root_directory_excel() -> str:
    return os.path.abspath("../PSR_querys/excel")
