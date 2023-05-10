import pandas as pd
import os
import pathlib
import json
"""
Date: 2022-02-26
Aim: Make a config for each sensor to control which fields should be imported to influxdb
Output: dict with config
"""
path = "./data/data_types/"

files = [f for f in os.listdir(path) if f.endswith('dtypes.xlsx')]

sensors = set([f.split('_')[0] for f in files])

data = dict()
for f in files:
    sensor = f.split('_')[0]
    fpath = os.path.join(path, f)
    df = pd.read_excel(fpath, header=0, index_col=0)
    data[sensor] = dict()
    for item in df[0].items():
        # save config for each column variable
        data[sensor][item[0]] = dict(to_influx=False, dtype=item[1])

with open(os.path.join(path, 'config_new.json'), "w") as f:
    f.write(json.dumps(data))