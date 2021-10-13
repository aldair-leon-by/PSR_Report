import mysql.connector
from mysqlx import errorcode
from mysql_connection import mysql_credentials, mysql_envs_descriptions
import pandas as pd
from init_logger import log

logger = log('MYSQL QUERY')


class CsvCreationMysql:
    def __init__(self, env, excel_file_ingestion, path, excel_file_name):
        self.env = env
        self.excel_file_ingestion = excel_file_ingestion
        self.path = path
        self.excel_file_name = excel_file_name

    def mysql_connection(self):
        env = mysql_credentials()
        mysql_server = env['my' + self.env][0]['mysql_server']
        mysql_username = env['my' + self.env][0]['mysql_username']
        mysql_password = env['my' + self.env][0]['mysql_password']
        mysql_db = env['my' + self.env][0]['mysql_database']
        try:
            self.cnx = mysql.connector.connect(user=mysql_username, password=mysql_password,
                                               host=mysql_server,
                                               database=mysql_db)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            logger.info('Successfully connection MYSQL my' + self.env + '!')

    def mysql_query_computation_time(self):
        self.df = pd.read_excel(self.excel_file_ingestion)
        self.ingestion_id = list(self.df['INGESTION_ID'])
        self.format_strings = ",".join(['%s'] * len(self.ingestion_id))
        query_start = "SELECT ingestion_id as INGESTION_ID, min(event_time) AS COMPUTATION_STARTED " \
                      "FROM stack_db.event WHERE ingestion_id IN (%s) " \
                      "AND status IN ('ACCEPTING') " \
                      "AND service IN ('computation') " \
                      "GROUP BY ingestion_id" % self.format_strings

        try:
            mysql_query = pd.read_sql(query_start, con=self.cnx, params=tuple(self.ingestion_id))
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        mysql_query.to_excel(self.path + self.excel_file_name['excel_file_name_computation_start'], index=False)
        logger.info('Excel created successfully location --> {0}'.format(
            self.path + self.excel_file_name['excel_file_name_computation_start']))

        query_finish = "SELECT ingestion_id as INGESTION_ID, max(event_time) AS COMPUTATION_FINISHED " \
                       "FROM stack_db.event WHERE ingestion_id IN (%s) " \
                       "AND status IN ('COMPLETED','COMPLETED_WITH_TIMEOUT') " \
                       "AND service IN ('computation') " \
                       "AND performance_metrics != 'NULL'" \
                       "GROUP BY ingestion_id" % self.format_strings

        try:
            mysql_query = pd.read_sql(query_finish, con=self.cnx, params=tuple(self.ingestion_id))
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        mysql_query.to_excel(self.path + self.excel_file_name['excel_file_name_computation_finish'],index=False)
        logger.info('Excel created successfully location --> {0}'.format(
            self.path + self.excel_file_name['excel_file_name_computation_finish']))

    def mysql_query_computation_performance(self):

        query = "SELECT max(event_time) AS COMPUTATION_FINISHED, ingestion_id as INGESTION_ID, status AS COMPUTATION_STATUS, " \
                "json_extract(performance_metrics,'$.summary.overall.totalCpuTimeMs') AS OVERALL_totalCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.overall.averageCpuTimeMs') AS OVERALL_averageCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.overall.performanceStatus') AS OVERALL_performanceStatus, " \
                "json_extract(performance_metrics,'$.summary.overall.totalInvocationCount') AS OVERALL_totalInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.overall.currentInvocationCount') AS OVERALL_currentInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.overall.invocationPerObjectRatio') AS OVERALL_invocationPerObjectRatio, " \
                "json_extract(performance_metrics,'$.summary.overall.totalSourcingObjectCount') AS OVERALL_totalSourcingObjectCount, " \
                "json_extract(performance_metrics,'$.summary.overall.totalProcessedObjectCount') AS OVERALL_totalProcessedObjectCount, " \
                "json_extract(performance_metrics,'$.summary.Order.totalCpuTimeMs') AS ORDER_totalCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.Order.averageCpuTimeMs') AS Order_averageCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.Order.performanceStatus') AS Order_performanceStatus, " \
                "json_extract(performance_metrics,'$.summary.Order.totalInvocationCount') AS ORDER_totalInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.Order.currentInvocationCount') AS ORDER_currentInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.Order.invocationPerObjectRatio') AS ORDER_invocationPerObjectRatio, " \
                "json_extract(performance_metrics,'$.summary.Order.totalSourcingObjectCount') AS ORDER_totalSourcingObjectCount, " \
                "json_extract(performance_metrics,'$.summary.Order.totalProcessedObjectCount') AS ORDER_totalProcessedObjectCount, " \
                "json_extract(performance_metrics,'$.summary.transportation.totalCpuTimeMs') AS TRANSPORTATION_totalCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.transportation.averageCpuTimeMs') AS TRANSPORTATION_averageCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.transportation.performanceStatus') AS TRANSPORTATION_performanceStatus, " \
                "json_extract(performance_metrics,'$.summary.transportation.totalInvocationCount') AS TRANSPORTATION_totalInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.transportation.currentInvocationCount') AS TRANSPORTATION_currentInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.transportation.invocationPerObjectRatio') AS TRANSPORTATION_invocationPerObjectRatio, " \
                "json_extract(performance_metrics,'$.summary.transportation.totalSourcingObjectCount') AS TRANSPORTATION_totalSourcingObjectCount, " \
                "json_extract(performance_metrics,'$.summary.transportation.totalProcessedObjectCount') AS TRANSPORTATION_totalProcessedObjectCount, " \
                "json_extract(performance_metrics,'$.summary.\"computation-backend\".totalCpuTimeMs') AS computation_backend_totalCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.\"computation-backend\".averageCpuTimeMs') AS computation_backend_averageCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.\"computation-backend\".performanceStatus') AS computation_backend_performanceStatus, " \
                "json_extract(performance_metrics,'$.summary.\"computation-backend\".totalInvocationCount') AS computation_backend_totalInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.\"computation-backend\".currentInvocationCount') AS computation_backend_currentInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.\"computation-backend\".invocationPerObjectRatio') AS computation_backend_invocationPerObjectRatio, " \
                "json_extract(performance_metrics,'$.summary.\"computation-backend\".totalSourcingObjectCount') AS computation_backend_totalSourcingObjectCount, " \
                "json_extract(performance_metrics,'$.summary.\"computation-backend\".totalProcessedObjectCount') AS computation_backend_totalProcessedObjectCount, " \
                "json_extract(performance_metrics,'$.summary.\"computation-frontend\".totalCpuTimeMs') AS computation_frontend_totalCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.\"computation-frontend\".averageCpuTimeMs') AS computation_frontend_averageCpuTimeMs, " \
                "json_extract(performance_metrics,'$.summary.\"computation-frontend\".performanceStatus') AS computation_frontend_performanceStatus, " \
                "json_extract(performance_metrics,'$.summary.\"computation-frontend\".totalInvocationCount') AS computation_frontend_totalInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.\"computation-frontend\".currentInvocationCount') AS computation_frontend_currentInvocationCount, " \
                "json_extract(performance_metrics,'$.summary.\"computation-frontend\".invocationPerObjectRatio') AS computation_frontend_invocationPerObjectRatio, " \
                "json_extract(performance_metrics,'$.summary.\"computation-frontend\".totalSourcingObjectCount') AS computation_frontend_totalSourcingObjectCount, " \
                "json_extract(performance_metrics,'$.summary.\"computation-frontend\".totalProcessedObjectCount') AS computation_frontend_totalProcessedObjectCount " \
                "FROM stack_db.event " \
                "WHERE ingestion_id IN (%s)  and status in ('COMPLETED','COMPLETED_WITH_TIMEOUT')  " \
                "AND service in ('COMPUTATION') " \
                "AND performance_metrics != 'NULL' " \
                "group by ingestion_id,performance_metrics,status;" % self.format_strings
        try:
            mysql_query = pd.read_sql(query, con=self.cnx, params=tuple(self.ingestion_id))
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        mysql_query.to_excel(self.path + self.excel_file_name['excel_file_name_computation_metrics'], index=False)
        logger.info('Excel created successfully location --> {0}'.format(
            self.path + self.excel_file_name['excel_file_name_computation_metrics']))
