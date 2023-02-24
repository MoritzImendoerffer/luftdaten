"""
Extract type of sensor as well as data types from the data
"""

import datetime
import subprocess
import time
import os
import numpy as np
import requests
import json
import gc
import polars as pd


import warnings

from src.api_keys import openai_api_key
import openai
openai.api_key = openai_api_key


data_path = "data/archive.sensor.community/csv_per_month"
date = "2022-12"

# Get the list of files
folder_path = os.path.join(data_path, date)
files = os.listdir(folder_path)
files = [f for f in files if f.endswith(".csv")]

# Get the list of sensors
# sensors = {}
# for f in files:
#     sensor = f.split("_")[1].split(".")[0]
#     if sensor not in sensors.keys():

#         # Get the list of data types
   
#         prompt = f'Generate a technical summary for the sensor {sensor}.'
#         response = openai.Completion.create(engine="text-davinci-003", 
#                                             prompt=prompt, 
#                                             max_tokens=2048, 
#                                             temperature=0.2)

#         sensors[sensor] = response.choices[0].text.strip("\n")

# save_path = os.path.join(folder_path, "sensors.json")
# with open(save_path, "w", encoding='utf-8') as f:
#     json.dump(sensors, f, ensure_ascii=False)


for file in files:
    file_path = os.path.join(folder_path, file)
    sensor = file.split("_")[1].split(".")[0]
    print(30*'=')
    print(sensor)
    df = {}
    try:
        df['df'] = pd.read_csv(file_path, sep=";")
        # save_path = os.path.join(folder_path, sensor + '_dsummary.xlsx')
        # df['df'].describe().to_excel(save_path)

        # save_path = os.path.join(folder_path, sensor +'_dtypes.xlsx')
        # df['df'].dtypes.to_excel(save_path)

        df.clear()
    except Exception as e:
        gc.collect()
        print(30*'-')
        print(f"sensors {sensor} has a problem: {e}")
        print(30*'=')

with open(os.path.join(folder_path, "sensors.json"), "r") as f:
    sensors = json.load(f)

with open(os.path.join(folder_path, "sensors.txt"), "w") as f:
    for key, item in sensors.items():
        f.write(60*'=' + '\n')
        f.write(key + '\n')
        f.write(60*'=' + '\n')
        f.write(item + '\n')