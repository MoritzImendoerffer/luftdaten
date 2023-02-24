from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
from src.unzip import unzip
import zipfile

# You can generate a Token from the "Tokens Tab" in the UI
token = "Ezm4ZetiRzjDeMSlhvhjvSMaX-dYUJ8DQ1Sc1s5T6SGowgwLs7OAN3gPTSARobLBhtXflRobUWOYy0Cphz67Pw=="
org = "luftdaten"
bucket = "sensor_comm"

client = InfluxDBClient(url="http://localhost:8086", token=token)

data_path = "data/archive.sensor.community/csv_per_month"
dates = os.listdir(data_path)

sensors = ['sds11']

for date in dates:

    # Get the list of files
    folder_path = os.path.join(data_path, date)
    files = os.listdir(folder_path)
    zip_files = [f for f in files if f.endswith(".zip")]
    csv_files = [f for f in files if f.endswith(".csv")]
    csv_file_names = [f.strip(".csv") for f in csv_files]
    missing_files = [f for f in zip_files if f.strip(".zip") not in csv_files]

    if missing_files:
        for f in missing_files:
            file_path = os.path.join(folder_path, f)
            with zip_files.ZipFile(file_path, "r") as z:
                z.extractall(folder_path)
    