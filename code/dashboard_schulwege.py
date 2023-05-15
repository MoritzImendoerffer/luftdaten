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
# measured values are UTC, correct for summer time
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s') + datetime.timedelta(hours=2)
df['datetime_str'] = df["datetime"].astype(str)

unique_locations = {value: index for index, value in enumerate(set([tuple(i) for i in df.loc[:, ["latitude", "longitude"]].values]))}
df["location_tuple"] = [tuple(i) for i in df.loc[:, ["latitude", "longitude"]].values]
df["location_id"] = df["location_tuple"].map(unique_locations)
mask = df['datetime'] > datetime.datetime(1970, 12, 31)
df = df.loc[mask,:]
df = df.merge(table.loc[:, ["id", "lastip", "name"]], on="id")

#gr = df[mask].set_index("id").groupby([pd.Grouper(key='datetime', freq='1d'), 'id'])


# Create a list of unique ids
#unique_ids = df['id'].unique().tolist()
unique_names = df['name'].unique().tolist()
unique_ip = df['lastip'].unique().tolist()
# Dropdown widget to select an id
#id_selector = pn.widgets.Select(name='Select id', options=unique_ids)
ip_selector = pn.widgets.Select(name='Select ip', 
                                options=unique_ip, 
                                value=unique_ip[0])

name_selector = pn.widgets.MultiSelect(name="Select name", 
                                       options=unique_names,
                                       value=[df.loc[df["lastip"]==unique_ip[0], "name"][0]])

# range slider for times
date_range_slider = DateRangeSlider(name='Date Range', 
                                    start=df['datetime'].min(), 
                                    end=df['datetime'].max(), 
                                    value=(df['datetime'].min(), df['datetime'].max()), width=600)


# Timeseries plot
def timeseries_plot(select_ip, selected_name, date_range):
    filtered_df = df[(df['name'].isin(list(selected_name))) & 
                     (df['lastip'].isin([select_ip])) &
                     (df['datetime'] >= date_range[0]) & 
                     (df['datetime'] <= date_range[1])].sort_values("datetime")
    
    hover = HoverTool(tooltips=[("pm10", "@pm10"), ("lon", "@longitude"), ('lat', '@latitude')])
    plot = hv.Scatter(filtered_df, kdims=['datetime'], vdims=['pm10', 'longitude', 'latitude', 'location_id']).opts(
        width=600, height=400, tools=[hover], ylabel='PM10', color='location_id')
    c = hv.Curve(filtered_df, kdims=['datetime'], vdims=['pm10']).opts(
        width=600, height=400, ylabel='PM10', color='black', alpha=0.5)
    curve = timeseries.rolling(c)
    return decimate(plot) * decimate(curve)

def map_plot(select_ip, selected_name, date_range):
    filtered_df = df[(df['name'].isin(list(selected_name))) & 
                     (df['lastip'].isin([select_ip])) &
                     (df['datetime'] >= date_range[0]) & 
                     (df['datetime'] <= date_range[1])].sort_values("datetime")
    hover = HoverTool(tooltips=[("pm10", "@pm10"), ("date", "@datetime_str")])
    points = gv.Points(filtered_df, kdims=['longitude', 'latitude'], vdims=['datetime_str', 'pm10']).opts(
        color='red', cmap='viridis', colorbar=True, width=600, height=400, size=10, tools=[hover])
    
    tiled_map = gv.tile_sources.OSM() #.opts(width=600, height=400)
    return tiled_map * decimate(points) #points.opts(tools=[hover])

@pn.depends(ip_selector.param.value, name_selector.param.value, date_range_slider.param.value)
def update_plots(selected_ip, selected_name, date_range):
    ts_plot_obj = timeseries_plot(selected_ip, selected_name, date_range)
    map_plot_obj = map_plot(selected_ip, selected_name, date_range)
    return pn.Row(pn.panel(ts_plot_obj), pn.panel(map_plot_obj))

dashboard = pn.Column(
    pn.Column(ip_selector,
              name_selector,
              date_range_slider,
              update_plots)
)

#dashboard.servable()
bserve = pn.serve(dashboard, port=12342)
bserve.stop()