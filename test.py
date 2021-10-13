import os
from datetime import datetime
import pandas as pd


df = pd.read_excel('IngestToMessageBrokerDetails.xlsx')
ts = df['INGESTION_SERVICE_MESSAGE_STARTED'][0]
format_time = ts.to_pydatetime()
folder_name = datetime.strptime(str(format_time), '%Y-%m-%d %H:%M:%S.%f').strftime('%B-%d-%Y')
report_name = datetime.strptime(str(format_time), '%Y-%m-%d %H:%M:%S.%f').strftime('h%H_m%M_s%S_ms%f')
os.mkdir(report_name, 0o666)

print(folder_name)
print(report_name)