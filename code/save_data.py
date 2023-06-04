import pathlib
import pandas as pd
import pathlib
import os
import numpy as np
import datetime

"""
Reads the raw data files in the "data" folder and creates aggregatet data
- data folder: '2_Projects/Saubere Luft am Schulweg/auswertung/schulwege_data/data'
- aggregated data is stored in '2_Projects/Saubere Luft am Schulweg/auswertung/schulwege_data'

"""
# Change this link if you are running your script locally
link_to_cloud = '/home/moritz/cloud.luftdaten.at'
internal_path = '2_Projects/Saubere Luft am Schulweg/auswertung/schulwege_data/'
save_path = pathlib.Path(link_to_cloud).joinpath(internal_path)
data_path = save_path.joinpath("data")

# frequencies to aggregate the data, should be compatible with pandas
# https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
frequencies = ["1min", "5min", "15min", "1d"]

files = [f for f in os.listdir(data_path) if f.endswith('.csv')]

table = pd.read_csv(data_path.parent.joinpath('table.csv'))
table.columns = ['id', 'name', 'lastseen', 'lastip', 'version']

df_list = []
columns = ["timestamp",	
           "latitude",	
           "longitude",	
           "temperature",
           "humidity",
           "pm1",
           "pm25",
           "pm10",
           "noxidx",
           "vocidx",
           "count"]

for file in files:
    _df = pd.read_csv(data_path.joinpath(file))
    _df.columns = columns
    _df['id'] = file.split('.csv')[0]
    df_list.append(_df)
df = pd.concat(df_list)

df['pm1'] /= 10
df['pm25'] /= 10
df['pm10'] /= 10

unique = df['id'].unique()
map_id = {k:v for k, v in zip(unique, np.arange(len(unique)))}
name_mapper = {item[0]: item[1] for item in [tuple(item) for item in table[['id', 'name']].values]}
ip_mapper = {item[0]: item[1] for item in [tuple(item) for item in table[['id', 'lastip']].values]}

df['id_int'] = df['id'].map(map_id)
# measured values are UTC, correct for summer time
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s') + datetime.timedelta(hours=2)
df['datetime_str'] = df["datetime"].astype(str)

mask = df['datetime'] > datetime.datetime(1970, 12, 31)
df = df.loc[mask,:]
df = df.merge(table.loc[:, ["id", "lastip", "name"]], on="id")

# aggregate each frequency in a loop and save the aggregated data to datapath
for freq in frequencies:
    print(freq)
    gr = df.set_index("id").groupby([pd.Grouper(key='datetime', freq=freq), 'id'])
    df_mean = gr.aggregate("mean", numeric_only=True)
    df_mean["name"] = df_mean.index.get_level_values(1).map(name_mapper)
    df_mean["ip"] = df_mean.index.get_level_values(1).map(ip_mapper)
    df_mean.to_excel(save_path.joinpath(f'data_{freq}.xlsx'))