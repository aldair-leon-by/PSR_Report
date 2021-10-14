import os
from message import message_type_name
from env_config import get_root_directory_folder_excel_data, get_details_name, get_summary_name, \
    get_root_directory_folder_name, get_root_directory_file_name
import pandas as pd
from datetime import datetime
from init_logger import log

logger = log('JOIN FILE')


class JoinFiles:

    def __init__(self, detail_file, summary_file):
        self.detail_file = detail_file
        self.summary_file = summary_file

    def excel_message_type_update(self):
        self.path = get_root_directory_folder_excel_data()
        IngestToMessageBrokerSummaryNew = message_type_name()
        summary = get_summary_name()
        ingestToMB = pd.read_excel(self.path + "\/" + summary[0])
        ingestToMB.insert(column='TYPE_OF_MESSAGE_INGEST', value=IngestToMessageBrokerSummaryNew, loc=2)
        ingestToMB.to_excel((self.path + "\/" + summary[0]))
        logger.info('Message Update Finished !!')

    def exel_to_data_frama_Details(self):
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

    def creat_excel_file_details(self):
        format_time = self.e2eIngestionComputation['INGESTION_SERVICE_MESSAGE_STARTED'][0]
        directory_FileName = format_time.to_pydatetime()
        self.folder_name = datetime.strptime(str(directory_FileName), '%Y-%m-%d %H:%M:%S.%f').strftime('%B-%d-%Y')
        self.report_folder = get_root_directory_folder_name()
        path = os.path.join(self.report_folder, self.folder_name)
        path_detail = os.path.join(path, 'DetailReport')
        if not os.path.exists(path):
            os.mkdir(path, 0o666)
        if not os.path.exists(path_detail):
            os.mkdir(path_detail, 0o666)
        self.report_name = datetime.strptime(str(directory_FileName), '%Y-%m-%d %H:%M:%S.%f').strftime('h%H_m%M_s'
                                                                                                       '%S_ms%f')
        exel_file_name_details = 'DetailReport - ' + self.report_name + '.xlsx'
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

    def exel_to_data_frama_Summary_computation(self):
        self.ComputationSummary = self.e2eIngestionComputation.groupby(['TYPE_OF_MESSAGE', 'COMPUTATION_STATUS']) \
            .agg(COMPUTATION_STARTED=('COMPUTATION_STARTED', 'min'),
                 COMPUTATION_FINISHED=('COMPUTATION_FINISHED', 'max'),
                 TOTAL_OF_MESSAGE=('COMPUTATION_STATUS', 'count')).reset_index()

    def exel_to_data_frama_Summary_e2e(self):
        self.e2eIngestionComputationSummary = self.e2eIngestionSummary[
            ['TOTAL_OF_MESSAGE', 'TYPE_OF_MESSAGE_INGEST', 'TYPE_OF_MESSAGE', 'CRNT_STATUS',
             'INGESTION_SERVICE_MESSAGE_STARTED',
             'INGESTION_SERVICE_MESSAGE_FINISHED', 'MESSAGE_BROKER_STARTED', 'MESSAGE_BROKER_FINISHED']].merge(
            self.ComputationSummary[
                ['TYPE_OF_MESSAGE', 'COMPUTATION_STATUS', 'COMPUTATION_STARTED', 'COMPUTATION_FINISHED',
                 'TOTAL_OF_MESSAGE']],
            on=['TOTAL_OF_MESSAGE', 'TYPE_OF_MESSAGE'])

    def creat_excel_file_summary(self):
        path = os.path.join(self.report_folder, self.folder_name)
        path_summary = os.path.join(path, 'SummaryReport')
        if not os.path.exists(path):
            os.mkdir(path, 0o666)
        if not os.path.exists(path_summary):
            os.mkdir(path_summary, 0o666)
        exel_file_name_summary = 'SummaryReport - ' + self.report_name + '.xlsx'
        self.e2eIngestionComputationSummary.to_excel(path_summary + '\\' + exel_file_name_summary, index=False)
        logger.info('EXCEL FILE SUMMARY REPORT path -> ' + path_summary + '\\' + exel_file_name_summary)
