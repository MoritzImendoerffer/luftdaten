import pathlib
import geoviews as gv
import hvplot.pandas
import holoviews as hv
from holoviews.operation import decimate
from holoviews.operation import datashader
from holoviews.operation import timeseries
import pandas as pd
import pathlib
import os
import numpy as np

from bokeh.plotting import show
import panel as pn
import datetime
hv.extension('bokeh')
pn.extension(comms='vscode')
#pn.extension('notebook')
#from holoviews.operation.datashader import datashade, dynspread
from bokeh.models import HoverTool
from panel.widgets import DateRangeSlider

import modin.pandas as pd

link = '/home/moritz/Sync/schulwege_data/data'
data_path = pathlib.Path(link)
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

gr = df.set_index("id").groupby([pd.Grouper(key='datetime', freq='5Min'), 'id'])
df_mean = gr.aggregate("mean", numeric_only=True)
df_mean["name"] = df_mean.index.get_level_values(1).map(name_mapper)
df_mean["ip"] = df_mean.index.get_level_values(1).map(ip_mapper)
#df_mean.reset_index(drop=True).to_excel(data_path.parent.joinpath('data_5min.xlsx'))
df_mean.to_excel(data_path.parent.joinpath('data_5min.xlsx'))

gr = df.set_index("id").groupby([pd.Grouper(key='datetime', freq='1Min'), 'id'])
df_mean = gr.aggregate("mean", numeric_only=True)
df_mean["name"] = df_mean.index.get_level_values(1).map(name_mapper)
df_mean["ip"] = df_mean.index.get_level_values(1).map(ip_mapper)
#df_mean.reset_index(drop=True).to_excel(data_path.parent.joinpath('data_1min.xlsx'))
df_mean.to_excel(data_path.parent.joinpath('data_1min.xlsx'))

gr = df.set_index("id").groupby([pd.Grouper(key='datetime', freq='15Min'), 'id'])
df_mean = gr.aggregate("mean", numeric_only=True)
df_mean["name"] = df_mean.index.get_level_values(1).map(name_mapper)
df_mean["ip"] = df_mean.index.get_level_values(1).map(ip_mapper)
#df_mean.reset_index(drop=True).to_excel(data_path.parent.joinpath('data_15min.xlsx'))
df_mean.to_excel(data_path.parent.joinpath('data_15min.xlsx'))

gr = df.set_index("id").groupby([pd.Grouper(key='datetime', freq='1d'), 'id'])
df_mean = gr.aggregate("mean", numeric_only=True)
df_mean["name"] = df_mean.index.get_level_values(1).map(name_mapper)
df_mean["ip"] = df_mean.index.get_level_values(1).map(ip_mapper)
#df_mean.reset_index(drop=True).to_excel(data_path.parent.joinpath('data_1day.xlsx'))
df_mean.to_excel(data_path.parent.joinpath('data_1day.xlsx'))