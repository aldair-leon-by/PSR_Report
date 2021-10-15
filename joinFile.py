import os
from message import message_type_name
from env_config import get_root_directory_folder_excel_data, get_details_name, get_summary_name, \
    get_root_directory_folder_name, get_root_directory_file_name
import pandas as pd
from datetime import datetime
from init_logger import log

logger = log('JOIN FILE')


class JoinFiles:

    def __init__(self):
        self.file = None

    def excel_message_type_update(self):
        self.path = get_root_directory_folder_excel_data()
        IngestToMessageBrokerSummaryNew = message_type_name()
        summary = get_summary_name()
        ingestToMB = pd.read_excel(self.path + "\/" + summary[0])
        ingestToMB.insert(column='TYPE_OF_MESSAGE_INGEST', value=IngestToMessageBrokerSummaryNew, loc=2)
        ingestToMB.to_excel((self.path + "\/" + summary[0]))
        logger.info('Message Update Finished !!')

    def exel_to_data_frama_Details(self):
        self.path = get_root_directory_folder_excel_data()
        detail = get_details_name()
        ingestToMB = pd.read_excel(self.path + "\/" + detail[0])
        lctAdapter = pd.read_excel(self.path + "\/" + detail[1])
        self.e2eIngestionDetails = ingestToMB[
            ['TYPE_OF_MESSAGE', 'CRNT_STATUS', 'MESSAGE_BROKER_BLK_ID', 'INGESTION_SERVICE_MESSAGE_STARTED',
             'INGESTION_SERVICE_MESSAGE_FINISHED', 'MESSAGE_BROKER_STARTED', 'MESSAGE_BROKER_FINISHED']].merge(
            lctAdapter[['LCT_ADAPTER_STARTED', 'LCT_ADAPTER_FINISHED', 'MSG_STATUS', 'INGESTION_ID'
                , 'MESSAGE_BROKER_BLK_ID']],
            on="MESSAGE_BROKER_BLK_ID",
            how="left")
        logger.info('JOIN DETAILS REPORT !!')

    def exel_to_data_frama_Details_computation(self):
        computation = get_root_directory_file_name()
        computationStart = pd.read_excel(self.path + computation['excel_file_name_computation_start'])
        computationFinish = pd.read_excel(self.path + computation['excel_file_name_computation_finish'])
        computationMetrics = pd.read_excel(self.path + computation['excel_file_name_computation_metrics'])
        self.e2eComputationDetails = computationStart[['INGESTION_ID', 'COMPUTATION_STARTED']].merge(
            computationFinish[['INGESTION_ID', 'COMPUTATION_FINISHED']],
            on="INGESTION_ID",
            how="left")
        self.e2eComputationDetails = self.e2eComputationDetails[
            ['INGESTION_ID', 'COMPUTATION_STARTED', 'COMPUTATION_FINISHED']].merge(
            computationMetrics[['COMPUTATION_FINISHED', 'INGESTION_ID',
                                'COMPUTATION_STATUS', 'OVERALL_totalCpuTimeMs',
                                'OVERALL_averageCpuTimeMs',
                                'OVERALL_performanceStatus',
                                'OVERALL_totalInvocationCount',
                                'OVERALL_currentInvocationCount',
                                'OVERALL_invocationPerObjectRatio',
                                'OVERALL_totalSourcingObjectCount',
                                'OVERALL_totalProcessedObjectCount',
                                'ORDER_totalCpuTimeMs', 'Order_averageCpuTimeMs',
                                'Order_performanceStatus',
                                'ORDER_totalInvocationCount',
                                'ORDER_currentInvocationCount',
                                'ORDER_invocationPerObjectRatio',
                                'ORDER_totalSourcingObjectCount',
                                'ORDER_totalProcessedObjectCount',
                                'TRANSPORTATION_totalCpuTimeMs',
                                'TRANSPORTATION_averageCpuTimeMs',
                                'TRANSPORTATION_performanceStatus',
                                'TRANSPORTATION_totalInvocationCount',
                                'TRANSPORTATION_currentInvocationCount',
                                'TRANSPORTATION_invocationPerObjectRatio',
                                'TRANSPORTATION_totalSourcingObjectCount',
                                'TRANSPORTATION_totalProcessedObjectCount',
                                'computation_backend_totalCpuTimeMs',
                                'computation_backend_averageCpuTimeMs',
                                'computation_backend_performanceStatus',
                                'computation_backend_totalInvocationCount',
                                'computation_backend_currentInvocationCount',
                                'computation_backend_invocationPerObjectRatio',
                                'computation_backend_totalSourcingObjectCount',
                                'computation_backend_totalProcessedObjectCount',
                                'computation_frontend_totalCpuTimeMs',
                                'computation_frontend_averageCpuTimeMs',
                                'computation_frontend_performanceStatus',
                                'computation_frontend_totalInvocationCount',
                                'computation_frontend_currentInvocationCount',
                                'computation_frontend_invocationPerObjectRatio',
                                'computation_frontend_totalSourcingObjectCount',
                                'computation_frontend_totalProcessedObjectCount'
                                ]],
            on=["INGESTION_ID", "COMPUTATION_FINISHED"],
            how="left")
        logger.info('JOIN DETAILS COMPUTATION METRICS REPORT !!')

    def exel_to_data_frama_Details_e2e(self):
        self.e2eIngestionComputation = self.e2eIngestionDetails[
            ['TYPE_OF_MESSAGE', 'CRNT_STATUS', 'INGESTION_SERVICE_MESSAGE_STARTED',
             'INGESTION_SERVICE_MESSAGE_FINISHED', 'MESSAGE_BROKER_STARTED', 'MESSAGE_BROKER_FINISHED',
             'LCT_ADAPTER_STARTED', 'LCT_ADAPTER_FINISHED', 'MSG_STATUS', 'INGESTION_ID']].merge(
            self.e2eComputationDetails[['INGESTION_ID', 'COMPUTATION_STARTED', 'COMPUTATION_FINISHED',
                                        'COMPUTATION_STATUS', 'OVERALL_totalCpuTimeMs',
                                        'OVERALL_averageCpuTimeMs',
                                        'OVERALL_performanceStatus',
                                        'OVERALL_totalInvocationCount',
                                        'OVERALL_currentInvocationCount',
                                        'OVERALL_invocationPerObjectRatio',
                                        'OVERALL_totalSourcingObjectCount',
                                        'OVERALL_totalProcessedObjectCount',
                                        'ORDER_totalCpuTimeMs', 'Order_averageCpuTimeMs',
                                        'Order_performanceStatus',
                                        'ORDER_totalInvocationCount',
                                        'ORDER_currentInvocationCount',
                                        'ORDER_invocationPerObjectRatio',
                                        'ORDER_totalSourcingObjectCount',
                                        'ORDER_totalProcessedObjectCount',
                                        'TRANSPORTATION_totalCpuTimeMs',
                                        'TRANSPORTATION_averageCpuTimeMs',
                                        'TRANSPORTATION_performanceStatus',
                                        'TRANSPORTATION_totalInvocationCount',
                                        'TRANSPORTATION_currentInvocationCount',
                                        'TRANSPORTATION_invocationPerObjectRatio',
                                        'TRANSPORTATION_totalSourcingObjectCount',
                                        'TRANSPORTATION_totalProcessedObjectCount',
                                        'computation_backend_totalCpuTimeMs',
                                        'computation_backend_averageCpuTimeMs',
                                        'computation_backend_performanceStatus',
                                        'computation_backend_totalInvocationCount',
                                        'computation_backend_currentInvocationCount',
                                        'computation_backend_invocationPerObjectRatio',
                                        'computation_backend_totalSourcingObjectCount',
                                        'computation_backend_totalProcessedObjectCount',
                                        'computation_frontend_totalCpuTimeMs',
                                        'computation_frontend_averageCpuTimeMs',
                                        'computation_frontend_performanceStatus',
                                        'computation_frontend_totalInvocationCount',
                                        'computation_frontend_currentInvocationCount',
                                        'computation_frontend_invocationPerObjectRatio',
                                        'computation_frontend_totalSourcingObjectCount',
                                        'computation_frontend_totalProcessedObjectCount'

                                        ]],
            on=['INGESTION_ID'],
            how="left")
        logger.info('JOIN DETAILS INGESTION TO COMPUTATION METRICS REPORT !!')

    def creat_excel_file_details(self):
        format_time_start = self.e2eIngestionComputation['INGESTION_SERVICE_MESSAGE_STARTED'][0]
        format_time_finish = self.e2eIngestionComputation['INGESTION_SERVICE_MESSAGE_STARTED'].iloc[-1]
        directory_FileName_start = format_time_start.to_pydatetime()
        directory_FileName_finish = format_time_finish.to_pydatetime()
        self.folder_name_start = datetime.strptime(str(directory_FileName_start), '%Y-%m-%d %H:%M:%S.%f').strftime(
            '%B-%d-%Y')
        self.folder_name_finish = datetime.strptime(str(directory_FileName_finish), '%Y-%m-%d %H:%M:%S.%f').strftime(
            '%B-%d-%Y')
        self.report_folder = get_root_directory_folder_name()
        self.completeFoldernama = 'From- ' + self.folder_name_start + ' To- ' + self.folder_name_finish
        path = os.path.join(self.report_folder,self.completeFoldernama)
        path_detail = os.path.join(path, 'DetailReport')
        if not os.path.exists(path):
            os.mkdir(path, 0o666)
        if not os.path.exists(path_detail):
            os.mkdir(path_detail, 0o666)
        self.report_name_start = datetime.strptime(str(directory_FileName_start), '%Y-%m-%d %H:%M:%S.%f') \
            .strftime('h%Hm%Ms%Sms%f')
        self.reprt_name_finish = datetime.strptime(str(directory_FileName_finish), '%Y-%m-%d %H:%M:%S.%f') \
            .strftime('h%Hm%Ms%Sms%f')

        exel_file_name_details = 'DetailReport From- ' + self.report_name_start + ' To- ' + \
                                 self.reprt_name_finish + '.xlsx'
        with pd.ExcelWriter(path_detail + '\\' + exel_file_name_details) as write:
            self.e2eIngestionComputation.fillna('NaN', inplace=True)
            self.e2eIngestionComputation[
                ['INGESTION_ID', 'TYPE_OF_MESSAGE', 'CRNT_STATUS', 'INGESTION_SERVICE_MESSAGE_STARTED',
                 'INGESTION_SERVICE_MESSAGE_FINISHED',
                 'MESSAGE_BROKER_STARTED', 'MESSAGE_BROKER_FINISHED', 'LCT_ADAPTER_STARTED', 'LCT_ADAPTER_FINISHED',
                 'MSG_STATUS', 'COMPUTATION_STARTED', 'COMPUTATION_FINISHED', 'COMPUTATION_STATUS']].to_excel(
                write, index=False, sheet_name='DetailReport')
            self.e2eIngestionComputation[
                ['INGESTION_ID', 'OVERALL_totalCpuTimeMs',
                 'OVERALL_averageCpuTimeMs',
                 'OVERALL_performanceStatus',
                 'OVERALL_totalInvocationCount',
                 'OVERALL_currentInvocationCount',
                 'OVERALL_invocationPerObjectRatio',
                 'OVERALL_totalSourcingObjectCount',
                 'OVERALL_totalProcessedObjectCount',
                 'ORDER_totalCpuTimeMs', 'Order_averageCpuTimeMs',
                 'Order_performanceStatus',
                 'ORDER_totalInvocationCount',
                 'ORDER_currentInvocationCount',
                 'ORDER_invocationPerObjectRatio',
                 'ORDER_totalSourcingObjectCount',
                 'ORDER_totalProcessedObjectCount',
                 'TRANSPORTATION_totalCpuTimeMs',
                 'TRANSPORTATION_averageCpuTimeMs',
                 'TRANSPORTATION_performanceStatus',
                 'TRANSPORTATION_totalInvocationCount',
                 'TRANSPORTATION_currentInvocationCount',
                 'TRANSPORTATION_invocationPerObjectRatio',
                 'TRANSPORTATION_totalSourcingObjectCount',
                 'TRANSPORTATION_totalProcessedObjectCount',
                 'computation_backend_totalCpuTimeMs',
                 'computation_backend_averageCpuTimeMs',
                 'computation_backend_performanceStatus',
                 'computation_backend_totalInvocationCount',
                 'computation_backend_currentInvocationCount',
                 'computation_backend_invocationPerObjectRatio',
                 'computation_backend_totalSourcingObjectCount',
                 'computation_backend_totalProcessedObjectCount',
                 'computation_frontend_totalCpuTimeMs',
                 'computation_frontend_averageCpuTimeMs',
                 'computation_frontend_performanceStatus',
                 'computation_frontend_totalInvocationCount',
                 'computation_frontend_currentInvocationCount',
                 'computation_frontend_invocationPerObjectRatio',
                 'computation_frontend_totalSourcingObjectCount',
                 'computation_frontend_totalProcessedObjectCount'
                 ]].to_excel(
                write, index=False, sheet_name='DetailReportComputation')
            logger.info('EXCEL FILE DETAILS REPORT path -> ' + path_detail + '\\' + exel_file_name_details)

    def exel_to_data_frame_Summary(self):
        summary = get_summary_name()
        ingestToMB = pd.read_excel(self.path + "\/" + summary[0])
        lctAdapter = pd.read_excel(self.path + "\/" + summary[1])
        self.e2eIngestionSummary = ingestToMB[
            ['TOTAL_OF_MESSAGE', 'TYPE_OF_MESSAGE_INGEST', 'TYPE_OF_MESSAGE', 'CRNT_STATUS',
             'INGESTION_SERVICE_MESSAGE_STARTED',
             'INGESTION_SERVICE_MESSAGE_FINISHED', 'MESSAGE_BROKER_STARTED', 'MESSAGE_BROKER_FINISHED']].merge(
            lctAdapter[
                ['TYPE_OF_MESSAGE_INGEST', 'LCT_ADAPTER_STARTED', 'LCT_ADAPTER_FINISHED', 'MSG_STATUS']],
            on=["TYPE_OF_MESSAGE_INGEST"],
            how="left")
        logger.info('JOIN SUMMARY REPORT !!')

    def exel_to_data_frama_Summary_e2e(self):
        self.e2eIngestionComputationSummary = self.e2eIngestionComputation.groupby(
            ['TYPE_OF_MESSAGE', 'COMPUTATION_STATUS','MSG_STATUS']) \
            .agg(TOTAL_OF_MESSAGE=('COMPUTATION_STATUS', 'count'),
                 INGESTION_SERVICE_MESSAGE_STARTED=('INGESTION_SERVICE_MESSAGE_STARTED', 'min'),
                 INGESTION_SERVICE_MESSAGE_FINISHED=('INGESTION_SERVICE_MESSAGE_FINISHED', 'max'),
                 MESSAGE_BROKER_STARTED=('MESSAGE_BROKER_STARTED', 'min'),
                 MESSAGE_BROKER_FINISHED=('MESSAGE_BROKER_FINISHED', 'max'),
                 LCT_ADAPTER_STARTED=('LCT_ADAPTER_STARTED', 'min'),
                 LCT_ADAPTER_FINISHED=('LCT_ADAPTER_FINISHED', 'max'),
                 COMPUTATION_STARTED=('COMPUTATION_STARTED', 'min'),
                 COMPUTATION_FINISHED=('COMPUTATION_FINISHED', 'max')).reset_index()
        logger.info('JOIN SUMMARY INGESTION TO COMPUTATION !!')

    def creat_excel_file_summary(self):
        path = os.path.join(self.report_folder,self.completeFoldernama)
        path_summary = os.path.join(path, 'SummaryReport')
        if not os.path.exists(path):
            os.mkdir(path, 0o666)
        if not os.path.exists(path_summary):
            os.mkdir(path_summary, 0o666)
        exel_file_name_summary = 'SummaryReport - From- ' + self.report_name_start + ' To- ' + \
                                 self.reprt_name_finish + '.xlsx'
        self.e2eIngestionComputationSummary.to_excel(path_summary + '\\' + exel_file_name_summary, index=False)
        logger.info('EXCEL FILE SUMMARY REPORT path -> ' + path_summary + '\\' + exel_file_name_summary)
