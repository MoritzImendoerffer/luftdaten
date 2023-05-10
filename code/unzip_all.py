"""
Convert the csv data per folder to parquet files grouped per sensor_id
"""

import logging
import os
import subprocess
import numpy as np
import pandas as pd
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('unzip.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

DATAPATH = '../data/archive.sensor.community/csv_per_month'


# root path of the csv per month data
DATAPATH = './data/archive.sensor.community/csv_per_month'


folders = os.listdir(DATAPATH)

for data in tqdm(folders):
    """
    Loops over the folder containing csv per month data from archive.senorcommunity.org, opens 
    """
    logging.info(f"Processing: {data}")
    # use glob.glob instead of os.listdir
    folder = os.path.join(DATAPATH, data)
    files = os.listdir(folder)
    for file in files:
        file_path = os.path.join(folder, file)
        csv_file = file_path.replace('.zip', '.csv')
        if file.endswith('.zip'):
            if os.path.exists(csv_file):
                logging.info(f"csv already exists: {csv_file}")
            else:
                logging.info(f"unzipping: {file_path}")
                cmd = "unzip" + ' ' + file_path + ' -d' + ' ' + folder
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True
                )
                std_out, std_err = process.communicate()
        