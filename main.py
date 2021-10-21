from sql_to_excel_lct_adapter import CsvCreation
from joinFile import JoinFiles

"""
Author: Aldair Leon
Date: Sep 8th, 2021

This code convert SQL data into CSV file from SW Connect DB
"""

# This class request of date_start and date_finish in this format YY/MM/DD HH:MM:SS.FFF and
# env "sql_db_uat or sql_db_sit"


date_start = '21/10/20 01:00:00.000'
date_finish = '21/10/20 02:00:00.000'
env = 'sql_db_prod'

x = CsvCreation(date_start, date_finish, env)
y = JoinFiles()

x.sql_connection_message()
x.sql_connection_adapter()
x.sql_query_message_Detail()
# x.sql_query_message_Summary()
x.sql_query_adapter_Detail()
# x.sql_query_adapter_Summary()
x.sql_to_mysql()
# y.excel_message_type_update()
y.exel_to_data_frama_Details()
y.exel_to_data_frama_Details_computation()
y.exel_to_data_frama_Details_e2e()
y.excel_Details_calculations()
y.creat_excel_file_details()

# y.exel_to_data_frame_Summary()
y.exel_to_data_frama_Summary_e2e()
y.creat_excel_file_summary()
