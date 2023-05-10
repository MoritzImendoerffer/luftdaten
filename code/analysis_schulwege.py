import pathlib
import geopandas as gp
import geoviews as gv
import hvplot.pandas
import holoviews as hv
from bokeh import server
import pandas as pd
import pathlib
import os
import numpy as np
from matplotlib import pyplot as plt 
hv.extension('bokeh')
from bokeh.plotting import show
import panel as pn

link = '/home/moritz/Sync/schulwege_data/'
data_path = pathlib.Path(link)
files = [f for f in os.listdir(data_path) if f.endswith('.csv')]

df_list = []
for file in files:
    _df = pd.read_csv(data_path.joinpath(file))
    _df['id'] = file.split('.csv')[0]
    df_list.append(_df)
df = pd.concat(df_list)

# no gps means not standard deviation of the location
gr = df.groupby('id')['longitude']
mask = gr.aggregate(np.std) == 0.0
print(f"N without GPS: {mask.sum()}")
print(f"N with GPS: {mask.shape[0] - mask.sum()}")

# the the rows for which a gps signal is there
ids_gps = mask[~mask].index.to_list()
mask = df['id'].apply(lambda x: x in ids_gps)
df_gps = df.loc[mask, :]
p = df_gps.hvplot.scatter(x='id', y='pm10')

# display graph in browser
# a bokeh server is automatically started
bokeh_server = pn.Row(p).show(port=12346)

# stop the bokeh server (when needed)
bokeh_server.stop()