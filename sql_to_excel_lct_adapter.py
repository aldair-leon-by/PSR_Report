import pyodbc
import pandas as pd
from init_logger import log
from datetime import datetime
from sql_connection import sql_credentials
from mysql_to_excel_lct import ExcelCreationMysql
from env_config import get_root_directory_excel, get_root_directory_file_name

# Logger
logger = log('SQL QUERY')


# This class request of date start and date finish in format YY/MM/DD HH:MM:SS.FFF
class CsvCreation:
    # Constructor
    def __init__(self, date_time_start, date_time_finish, env):
        self.format_time = 'mm.ss.ff'
        self.date_time_start = date_time_start
        self.date_time_finish = date_time_finish
        self.env = env
        self.start_time = datetime.strptime(self.date_time_start, '%y/%m/%d %H:%M:%S.%f')
        self.finish_time = datetime.strptime(self.date_time_finish, '%y/%m/%d %H:%M:%S.%f')

    # SQL connection message_store db
    def sql_connection_message(self) -> object:
        env = sql_credentials()
        sql_server = env[self.env][0]['sql_server']
        sql_username = env[self.env][0]['sql_username']
        sql_password = env[self.env][0]['sql_password']
        sql_db = env[self.env][0]['sql_database_message_store']
        try:
            self.connection_message = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + sql_server + ';DATABASE=' + sql_db + ';UID=' + sql_username + ';PWD=' + sql_password)
            logger.info('Successfully connection SQL sql_database_message_store ' + self.env + '!')
            return self.connection_message
        except pyodbc.Error as ex:
            logger.error(ex)

    # SQL connection LCT_adapter db
    def sql_connection_adapter(self) -> object:
        env = sql_credentials()
        sql_server = env[self.env][0]['sql_server']
        sql_username = env[self.env][0]['sql_username']
        sql_password = env[self.env][0]['sql_password']
        sql_db = env[self.env][0]['sql_database_adapter']
        try:
            self.connection_adapter = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + sql_server + ';DATABASE=' + sql_db + ';UID=' + sql_username + ';PWD=' + sql_password)
            logger.info('Successfully connection SQL sql_database_adapter ' + self.env + '!')
            return self.connection_adapter
        except pyodbc.Error as ex:
            logger.error(ex)

    # SQL Query Ingest Service to Message Broker line by line
    def sql_query_message_Detail(self):
        self.excel_file_name = get_root_directory_file_name()
        # Query to execute
        query = 'WITH ingestService_to_MessageBroker AS(SELECT MSG_HDR.MSG_TYPE AS TYPE_OF_MESSAGE,' \
                'MSG_EVNT.CRTD_AT AS INGESTION_SERVICE_MESSAGE_STARTED,' \
                'MSG_HDR.LST_UPDT_AT AS INGESTION_SERVICE_MESSAGE_FINISHED,' \
                'BLK_HDR.BLK_HDR_ID,' \
                'MSG_HDR.MSG_ID,' \
                'BLK_HDR.BLK_ID,' \
                'BLK_HDR.CRNT_STATUS ' \
                'FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT ' \
                'JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ' \
                'ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID ' \
                'JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR ' \
                'ON BLK_HDR.BLK_ID LIKE CONCAT(?, MSG_HDR.MSG_ID,?) ' \
                'AND MSG_EVNT .STATUS = ? ' \
                'AND MSG_HDR.MDL_TYPE LIKE ? WHERE MSG_EVNT.CRTD_AT > ? and MSG_EVNT.CRTD_AT <  ? ' \
                ')' \
                'SELECT ingestService_to_MessageBroker.BLK_HDR_ID,' \
                'ingestService_to_MessageBroker.TYPE_OF_MESSAGE,' \
                'ingestService_to_MessageBroker.CRNT_STATUS, ' \
                'ingestService_to_MessageBroker.MSG_ID AS INGEST_SERVICE_MSG_ID, ' \
                'ingestService_to_MessageBroker.INGESTION_SERVICE_MESSAGE_STARTED,' \
                'ingestService_to_MessageBroker.INGESTION_SERVICE_MESSAGE_FINISHED,' \
                'ingestService_to_MessageBroker.BLK_ID AS MESSAGE_BROKER_BLK_ID,' \
                'MIN(BULK_EVENT.CRTD_AT) AS MESSAGE_BROKER_STARTED,' \
                'MAX(BULK_EVENT.CRTD_AT) AS MESSAGE_BROKER_FINISHED ' \
                'FROM ingestService_to_MessageBroker JOIN CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT ' \
                'ON ingestService_to_MessageBroker.BLK_HDR_ID = BULK_EVENT.BLK_HDR_ID ' \
                'WHERE  BULK_EVENT.STATUS != ? ' \
                'GROUP BY BULK_EVENT.BLK_HDR_ID, ' \
                'ingestService_to_MessageBroker.BLK_HDR_ID,' \
                'ingestService_to_MessageBroker.TYPE_OF_MESSAGE,' \
                'ingestService_to_MessageBroker.INGESTION_SERVICE_MESSAGE_STARTED, ' \
                'ingestService_to_MessageBroker.INGESTION_SERVICE_MESSAGE_FINISHED,' \
                'ingestService_to_MessageBroker.MSG_ID,' \
                'ingestService_to_MessageBroker.BLK_ID, ' \
                'ingestService_to_MessageBroker.CRNT_STATUS ' \
                'ORDER BY ingestService_to_MessageBroker.INGESTION_SERVICE_MESSAGE_STARTED'
        try:
            logger.info('Executing query... Message Store DB')
            sql_query = pd.read_sql_query(query, params=(
                '%', '%', 'Received', '%BYDM%', self.start_time, self.finish_time, 'Processed'),
                                          con=self.connection_message)
        except pyodbc.Error as ex:
            logger.error(ex)
        self.path = get_root_directory_excel()
        sql_query.to_excel(self.path + self.excel_file_name['excel_file_name_message_brokerDetails'], index=False)
        logger.info(
            'Excel created successfully location --> {0}'.format(
                self.path + self.excel_file_name['excel_file_name_message_brokerDetails']))

    # SQL Query Ingest Service to Message Broker summary of all ingestion
    def sql_query_message_Summary(self):

        query = 'WITH ingestService_to_MessageBroker_Total AS (' \
                'SELECT MSG_HDR.MSG_TYPE AS TYPE_OF_MESSAGE,' \
                'MSG_EVNT.CRTD_AT AS INGESTION_SERVICE_MESSAGE_STARTED,' \
                'MSG_HDR.LST_UPDT_AT AS INGESTION_SERVICE_MESSAGE_FINISHED,' \
                'BLK_HDR.BLK_HDR_ID,' \
                'BLK_HDR.CRNT_STATUS FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT ' \
                'JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID ' \
                'JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR  ON BLK_HDR.BLK_ID LIKE CONCAT(?, MSG_HDR.MSG_ID,?) ' \
                'AND MSG_EVNT.STATUS = ? ' \
                'AND MSG_HDR.MDL_TYPE LIKE ? WHERE MSG_EVNT.CRTD_AT > ? and MSG_EVNT.CRTD_AT < ? ) ' \
                'SELECT ' \
                'COUNT(ingestService_to_MessageBroker_Total.TYPE_OF_MESSAGE)/2 AS TOTAL_OF_MESSAGE, ' \
                'ingestService_to_MessageBroker_Total.TYPE_OF_MESSAGE, ' \
                'ingestService_to_MessageBroker_Total.CRNT_STATUS, ' \
                'MIN(ingestService_to_MessageBroker_Total.INGESTION_SERVICE_MESSAGE_STARTED) ' \
                'AS INGESTION_SERVICE_MESSAGE_STARTED, ' \
                'MAX(ingestService_to_MessageBroker_Total.INGESTION_SERVICE_MESSAGE_FINISHED) ' \
                'AS INGESTION_SERVICE_MESSAGE_FINISHED, ' \
                'MIN(BULK_EVENT.CRTD_AT) AS MESSAGE_BROKER_STARTED, ' \
                'MAX(BULK_EVENT.CRTD_AT) AS MESSAGE_BROKER_FINISHED ' \
                'FROM ingestService_to_MessageBroker_Total ' \
                'JOIN CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT ' \
                'ON ingestService_to_MessageBroker_Total.BLK_HDR_ID = BULK_EVENT.BLK_HDR_ID ' \
                'WHERE  BULK_EVENT.STATUS != ? ' \
                'GROUP BY ' \
                'ingestService_to_MessageBroker_Total.TYPE_OF_MESSAGE, ' \
                'ingestService_to_MessageBroker_Total.CRNT_STATUS ' \
                'ORDER BY min(ingestService_to_MessageBroker_Total.INGESTION_SERVICE_MESSAGE_STARTED);'
        try:
            sql_query = pd.read_sql_query(query, params=(
                '%', '%', 'Received', '%BYDM%', self.start_time, self.finish_time, 'Processed'),
                                          con=self.connection_message)
        except pyodbc.Error as ex:
            logger.error(ex)

        sql_query.to_excel(self.path + self.excel_file_name['excel_file_name_message_brokerSummary'], index=False)
        logger.info('Excel created successfully location --> {0}'.format(
            self.path + self.excel_file_name['excel_file_name_message_brokerSummary']))

    # SQL Query LCT Adapter line bby line
    def sql_query_adapter_Detail(self):
        query = 'SELECT AUDIT_TBL.MSG_TYPE,' \
                'AUDIT_TBL.MSG_INGEST_PARAM,' \
                'AUDIT_TBL.INGEST_STATUS_MSG,' \
                'AUDIT_TBL.INGESTION_ID,' \
                'AUDIT_TBL.MSG_STATUS,' \
                'AUDIT_TBL.CREATED_DATETIME AS LCT_ADAPTER_STARTED,' \
                'AUDIT_TBL.MODIFIED_DATETIME AS LCT_ADAPTER_FINISHED,' \
                'JSON_Value(DATA_TBL.GS1_header, ?) AS MESSAGE_BROKER_BLK_ID ' \
                'FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL ' \
                'JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID ' \
                'AND JSON_Value(GS1_header, ?) != ? WHERE DATA_TBL.CREATED_DATETIME  > ? ' \
                'AND DATA_TBL.CREATED_DATETIME <  ? ORDER BY AUDIT_TBL.MODIFIED_DATETIME ASC;'
        try:
            logger.info('Executing query... LCT Adapter DB')
            sql_query = pd.read_sql_query(query, params=(
                '$.messageId', '$.messageId', 'NULL', self.start_time, self.finish_time),
                                          con=self.connection_adapter)
        except pyodbc.Error as ex:
            logger.error(ex)
        self.excel_file_ingestion = self.path + self.excel_file_name['excel_file_name_adapter_Detail']
        sql_query.to_excel(self.excel_file_ingestion, index=False)
        logger.info('Excel created successfully location --> {0}'.format(
            self.path + self.excel_file_name['excel_file_name_adapter_Detail']))

    # SQL Query LCT Adapter summary of all ingestion
    def sql_query_adapter_Summary(self):
        query = 'SELECT count(AUDIT_TBL.MSG_STATUS) AS NUMBER_OF_MESSAGES, ' \
                'AUDIT_TBL.MSG_TYPE AS TYPE_OF_MESSAGE_INGEST, ' \
                'AUDIT_TBL.MSG_STATUS, ' \
                'min(AUDIT_TBL.CREATED_DATETIME) AS LCT_ADAPTER_STARTED, ' \
                'max(AUDIT_TBL.MODIFIED_DATETIME) AS LCT_ADAPTER_FINISHED  ' \
                'FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ' \
                'ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID AND JSON_Value(GS1_header, ?) != ?  ' \
                'WHERE DATA_TBL.CREATED_DATETIME  > ? and DATA_TBL.CREATED_DATETIME <  ? ' \
                'GROUP BY AUDIT_TBL.MSG_STATUS,AUDIT_TBL.msg_type ' \
                'ORDER BY min(AUDIT_TBL.CREATED_DATETIME); '
        try:
            sql_query = pd.read_sql_query(query, params=(
                '$.messageId', 'NULL', self.start_time, self.finish_time),
                                          con=self.connection_adapter)
        except pyodbc.Error as ex:
            logger.error(ex)
        sql_query.to_excel(self.path + self.excel_file_name['excel_file_name_adapter_Summary'], index=False)
        logger.info('Excel created successfully location --> {0}'.format(
            self.path + self.excel_file_name['excel_file_name_adapter_Summary']))

    # MySQL functions
    def sql_to_mysql(self):
        logger.info('MYSQL Process start')
        mysql = ExcelCreationMysql(self.env, self.excel_file_ingestion, self.path, self.excel_file_name)
        mysql.mysql_connection()
        mysql.mysql_query_computation_time()
        mysql.mysql_query_computation_performance()
