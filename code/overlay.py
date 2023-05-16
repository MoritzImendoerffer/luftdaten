import pathlib
import geoviews as gv
import hvplot.pandas
import holoviews as hv
from holoviews.operation import decimate
#from holoviews.operation import datashader
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
df['id_int'] = df['id'].map(map_id)
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s') + datetime.timedelta(hours=2)
df['datetime_str'] = df["datetime"].astype(str)

unique_locations = {value: index for index, value in enumerate(set([tuple(i) for i in df.loc[:, ["latitude", "longitude"]].values]))}
df["location_tuple"] = [tuple(i) for i in df.loc[:, ["latitude", "longitude"]].values]
df["location_id"] = df["location_tuple"].map(unique_locations)

mask = df['datetime'] > datetime.datetime(1970, 12, 31)

df = df.loc[mask,:]
df = df.merge(table.loc[:, ["id", "lastip", "name"]], on="id")

#gr = df[mask].set_index("id").groupby([pd.Grouper(key='datetime', freq='1d'), 'id'])


mask = df["lastip"] == "213.162.80.83"
df = df.loc[mask,:]
# Create a list of unique ids
unique_ids = df['id'].unique().tolist()
unique_names = df['name'].unique().tolist()

mask = df['datetime'] > datetime.datetime(year=2023, 
                                          month=5, 
                                          day=6, 
                                          hour=7
                                          )

mask1 = df['datetime'] < datetime.datetime(year=2023, 
                                          month=5, 
                                          day=6, 
                                          hour=17
                                          )

p = df.hvplot.line(x="datetime",
                   y="pm10",
                   by="name").opts(width=1000, height=800)

dashboard = pn.Column(p)

#dashboard.servable()
bserve = pn.serve(dashboard, port=12345)
bserve.stop()

print("done")