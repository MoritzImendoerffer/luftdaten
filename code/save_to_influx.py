from datetime import datetime
import logging
import json
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
from src.unzip import unzip
import zipfile
import polars as pl
import pandas as pd
import os
from src.api_keys import influx_token
import numpy as np
from tqdm import tqdm

"""
Started the cline with:
influxd --engine-path=/media/sda1/luftdb

admin token should be exported in the shell as TOKEN

'data/archive.sensor.community/csv_per_month/2018-07/2018-07_bme280.zip' missing. Corrupt zip file?
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('influxdb.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# You can generate a Token from the "Tokens Tab" in the UI
# TODO use environment variable or some other form to handle the token
org = "luftdaten"
bucket = "sensor_community"

def success_cb(details, data):
    url, token, org = details
    print(url, token, org)
    data = data.decode('utf-8').split('\n')
    logger.info(f'Total Rows Inserted: {len(data)}')  

def error_cb(details, data, exception):
    logger.exception(exception)

def retry_cb(details, data, exception):
    logger.error(f'Retrying because of an exception: {exception}')

client = InfluxDBClient(url="http://localhost:8086", token=influx_token)
write_api = client.write_api(write_options=SYNCHRONOUS,
                             success_callback=success_cb,
                             error_callback=error_cb,
                             retry_callback=retry_cb)

data_path = "data/archive.sensor.community/csv_per_month"
dates = os.listdir(data_path)
dates = sorted(dates)

config_path = "data/data_types/config.json"
config = json.load(open(config_path, "r"))


processed = [line.strip() for line in open("processed.txt", "r").readlines()]
processed_csv = [line.split(".")[0] for line in open("processed_csv.txt", "r").readlines()]

for date in tqdm(dates):

    if date in processed:
        print(f"skipping {date} as it has already been processed")
        continue

    folder_path = os.path.join(data_path, date)
    files = os.listdir(folder_path)
    zip_files = [f for f in files if f.endswith(".zip")]
    csv_files = [f for f in files if f.endswith(".csv")]
    csv_file_names = [f.split(".")[0] for f in csv_files]
    missing_files = [f for f in zip_files if f.split(".")[0]  not in csv_file_names]

    if missing_files:
        for mf in missing_files:
            file_path = os.path.join(folder_path, mf)
            try:
                with zipfile.ZipFile(file_path, "r") as z:
                    z.extractall(folder_path)
            except Exception as e:
                logging.exception(f"Error while extracting {file_path}")

    # update file list
    csv_files = [f for f in files if f.endswith(".csv")]

    for file in tqdm(csv_files):
        if file.split(".")[0] in processed_csv:
            print(f"skipping {file} as it has already been processed")
            continue

        sensor_type = file.split("_")[1].split(".")[0]

        if sensor_type == "unique":
            # not sure how this sensor name ended up in the database from sensor.community
            continue
    
        file_path = os.path.join(folder_path, file)
        conf = config[sensor_type]
        var_names = list(conf.keys())

        #TODO use to "convert_to" option

        dtypes = {v: conf[v]["dtype"] for i, v in enumerate(var_names)
                  if conf[v]["to_influx"]==True}
        
        rename = {v: conf[v]["rename"] for i, v in enumerate(var_names)
                  if ("rename" in conf[v].keys()) and (conf[v]["to_influx"]==True)}
        
        var_use = dtypes.keys()

        # read as string
        #df = pl.read_csv(file_path, dtypes=len(var_use)*[str], columns=var_use, sep=";")
        try:
            reader = pd.read_csv(file_path, 
                                dtype=str, 
                                usecols=var_use, 
                                sep=";",
                                chunksize = 100000)
        except Exception as e:
            logging.exception(f"Error while reading {file_path}")
            continue
        i = 1
        print(60*'=')
        while True:
            start = datetime.now()
            print(60*'-')
            print(f"starting import of file {file_path}: iteration {i}")
            try:
                df = reader.get_chunk()
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df.set_index("timestamp", inplace=True, drop=True)
                if "timestamp" in dtypes.keys():
                    # we don`t need it
                    dtypes.pop("timestamp")
                
                if "sensor_id" in dtypes.keys():
                    # sensor_id should remain a string
                    dtypes.pop('sensor_id')

                for key, dt in dtypes.items():
                    # values are usually not large. Downcasting saves space.
                    if (dt == 'float'):
                        df[key] = pd.to_numeric(df[key], errors="coerce", downcast='float')
                             
                    if (dt == 'int'): 
                        # sollte eigentlich nicht vorkommen
                        logging.info(f"Conversion and downcast to int for {sensor_type} and {key}")     
                        df[key] = pd.to_numeric(df[key], errors="coerce", downcast='int')
                if rename:
                    df.rename(columns=rename, inplace=True)

                end_0 = datetime.now()
                delta_0 = end_0 - start
                print(f"data mangling took {delta_0.total_seconds()} seconds")

                write_api.write(bucket, org, df,
                                data_frame_tag_columns=['sensor_id'], 
                                data_frame_measurement_name=sensor_type,
                                write_precision='s')

            except StopIteration:
                break
            except Exception as e:
                logger.exception(f"Exception while parsing {file_path}: {e}")
            
            end = datetime.now()
            delta = end - start
            print(f"loop took {delta.total_seconds()} seconds")
            i += 1
            print(60*'-')

        # save file path of csv
        idx = [i for i, v in enumerate(csv_files) if v == file][0]
        with open("processed_csv.txt", 'a') as f:
            for line in csv_files[0:idx]:
                f.write(line + "\n")

        print(f"stopped import of file {file_path}: iteration {i}")
        print(60*'=')

    # saved processed dates
    idx = [i for i, v in enumerate(dates) if v == date][0]
    with open("processed.txt", 'a') as f:
        for line in dates[0:idx+1]:
            f.write(line + "\n")
    