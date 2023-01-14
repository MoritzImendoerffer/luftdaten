"""
Convert the csv data per folder to parquet files grouped per sensor_id
#TODO use logging instead of print?
"""
import dask.dataframe as dd
import os


DATAPATH = '../data/archive.sensor.community/csv_per_month'

DELETE_CSV = True
DELETE_ZIP = False
UNZIP = True

folders = os.listdir(DATAPATH)


import pandas as pd
import os
import numpy as np
import subprocess

# root path of the csv per month data

DATAPATH = '../data/archive.sensor.community/csv_per_month'


DELETE_CSV = True
DELETE_ZIP = False
UNZIP = True

# sensor type (coded in the filenames)
sensor = "sds011"
#sensor = "pms7003"
#cols2float = ['P0', 'P1', 'P2']

folders = os.listdir(DATAPATH)

for date in folders:
    """
    Loops over the folder containing csv per month data from archive.senorcommunity.org, opens 
    """
    print(30*'=')
    print(date)

    file_name = date + '_' + sensor + '.csv'
    zip_file = date + '_' + sensor + '.zip'
              
    file_path = os.path.join(DATAPATH, date, file_name)
    save_path = os.path.join(DATAPATH, date, sensor + '_parquet')
    
    if UNZIP:
        zip_path = os.path.join(DATAPATH, date, zip_file)
        if os.path.exists(zip_path):
            if os.path.exists(file_path):
                print("csv already exists")
                print(zip_path)
                print(30 * '-')
            else:x
                print("unzipping")
                print(zip_path)
                print(30*'-')
                cmd = "unzip" + ' ' + zip_path + ' -d' + ' ' + os.path.join(DATAPATH, date)
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True
                )
                std_out, std_err = process.communicate()
        else:
            print("no zip file found")
            print(zip_path)
            print(30 * '-')

    if os.path.exists(save_path):
        print('Files path already exists')
        print(save_path)
        print(30*'-')
        continue
    
    if os.path.exists(file_path):
        dtypes = dict(P1='object', P2='object')
        ddf = dd.read_csv(file_path, sep=';', dtype=dtypes, usecols=['sensor_id', 'lat', 'lon', 'timestamp', 'P1', 'P2'])
        dd.to_parquet(ddf, path=save_path, partition_on=['sensor_id'])
        df_unique = ddf.drop_duplicates('sensor_id')
        df_unique.loc[:, ['sensor_id', 'location', 'lat', 'lon']].to_parquet(path=os.path.join(save_path, 'index'))
        df_unique = 0
        ddf = 0
    else:
        print('File not found')
        print(file_path)

    if DELETE_CSV:
        try:
            os.remove(file_path)
            print('deleting csv')
        except Exception as e:
            pass

    if DELETE_ZIP:
        try:
            os.remove(file_path.replace('.csv', '.zip'))
            print('deleting zip')
        except Exception as e:
            pass

    print(30*'=')



    
print(30*'=')   
print('DONE')
print(30*'=')
