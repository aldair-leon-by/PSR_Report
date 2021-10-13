import pandas as pd
from env_config import get_root_directory_folder_excel_data, get_summary_name, get_root_directory_folder_message_name
from init_logger import log

logger = log('MESSAGE TRANSFORM')


def message_type_name():
    path = get_root_directory_folder_excel_data()
    summary = get_summary_name()
    ingestToMB = pd.read_excel(path + "\/" + summary[0])
    message_name = get_root_directory_folder_message_name()
    message_type = []
    for i in ingestToMB['TYPE_OF_MESSAGE']:
        message_type.append(message_name[i])
    return message_type
