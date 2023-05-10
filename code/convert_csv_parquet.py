"""
Convert the csv data per folder to parquet files grouped per sensor_id
"""

import logging
import os
import subprocess
import numpy as np
import dask.dataframe as dd
import pandas as pd
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('logfile.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

DATAPATH = '../data/archive.sensor.community/csv_per_month'

DELETE_CSV = False
DELETE_ZIP = False
UNZIP = True
CONVERT_TO_PARQUET = False

# sensor type (coded in the filenames)
#sensor = "pms7003"
#cols2float = ['P0', 'P1', 'P2']
sensor = "sds011"
sensor = "all"
folders = os.listdir(DATAPATH)



# root path of the csv per month data

DATAPATH = '../data/archive.sensor.community/csv_per_month'


folders = os.listdir(DATAPATH)
folders = sorted(folders, key=lambda x: x.split('-'), reverse=True)

for date in tqdm(folders):
    """
    Loops over the folder containing csv per month data from archive.senorcommunity.org, opens 
    """
    print(30*'=')
    logging.info(f'Processing date: {date} and sensor: {sensor}')
    logging.info(f'Number of folders: {len(folders)}')
    logging.info(f"settings: delete_csv={DELETE_CSV}, \
                 delete_zip={DELETE_ZIP}, \
                 unzip={UNZIP}, \
                 convert_to_parquet={CONVERT_TO_PARQUET}")

    file_name = date + '_' + sensor + '.csv'
    zip_file = date + '_' + sensor + '.zip'
        
    file_path = os.path.join(DATAPATH, date, file_name)
    save_path = os.path.join(DATAPATH, date, sensor + '_parquet')

    if os.path.exists(save_path):
        logging.info(f'Files path already exists: {save_path}')
        continue
        
    if UNZIP:
        zip_path = os.path.join(DATAPATH, date, zip_file)
        if os.path.exists(zip_path):
            if os.path.exists(file_path):
                logging.info(f"csv already exists: {zip_path}")
            else:
                logging.info(f"unzipping: {zip_path}")
                cmd = "unzip" + ' ' + zip_path + ' -d' + ' ' + os.path.join(DATAPATH, date)
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True
                )
                std_out, std_err = process.communicate()
        else:
            logging.info(f"no zip file found: {zip_path}")

    if CONVERT_TO_PARQUET:

        if os.path.exists(file_path):
            logging.info(f'Converting to parquet: {file_path}')

            dtypes = dict(P1='object', P2='object')
            ddf = dd.read_csv(file_path, sep=';', dtype=dtypes, usecols=['sensor_id', 'lat', 'lon', 'timestamp', 'P1', 'P2'])
            dd.to_parquet(ddf, path=save_path, partition_on=['sensor_id'])
            df_unique = ddf.drop_duplicates('sensor_id')
            if 'location' in df_unique.columns:
                df_unique.loc[:, ['sensor_id', 'location', 'lat', 'lon']].to_parquet(path=os.path.join(save_path, 'index'))
            else:
                df_unique.loc[:, ['sensor_id', 'lat', 'lon']].to_parquet(path=os.path.join(save_path, 'index'))
            df_unique = 0
            ddf = 0

            logging.info(f'Converting done: {file_path}')

        else:
            logging.info(f'File not found: {file_path}')

    if DELETE_CSV:
        try:
            os.remove(file_path)
            print('deleting csv')
            logging.info(f'deleting csv: {file_path}')
        except Exception as e:
            pass

    if DELETE_ZIP:
        try:
            os.remove(file_path.replace('.csv', '.zip'))
            print('deleting zip')
            logging.info(f'deleting zip: {file_path}')
        except Exception as e:
            pass
    logging.info(f'Finished: {date} and sensor: {sensor}')
    print(30*'=')


logging.info('Finished all dates')    
print(30*'=')   
print('DONE')

print(30*'=')
